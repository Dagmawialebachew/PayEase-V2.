import asyncpg
import logging
from typing import Optional, Any, Dict, List
from asyncpg import Pool
from decimal import Decimal
from typing import Optional


SCHEMA_SQL = """
-- 1. Workers Table
CREATE TABLE IF NOT EXISTS workers (
    id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    phone TEXT,
    club TEXT NOT NULL,
    daily_rate DECIMAL(10, 2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Loans Table
CREATE TABLE IF NOT EXISTS loans (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES workers(id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'deducted'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Payouts Table
CREATE TABLE IF NOT EXISTS payouts (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES workers(id) ON DELETE CASCADE,
    days_worked INTEGER NOT NULL,
    gross_amount DECIMAL(10, 2) NOT NULL,
    loan_deduction DECIMAL(10, 2) DEFAULT 0,
    net_amount DECIMAL(10, 2) NOT NULL,
    club TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Attendance Table (The "Automatic Counter")
CREATE TABLE IF NOT EXISTS attendance (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES workers(id) ON DELETE CASCADE,
    work_date DATE DEFAULT CURRENT_DATE,
    rate_at_time DECIMAL(10, 2), -- Captures rate in case it changes later
    UNIQUE(worker_id, work_date) -- Prevents double-counting the same day
);

-- Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_workers_club ON workers (club);
CREATE INDEX IF NOT EXISTS idx_loans_status ON loans (status);
CREATE INDEX IF NOT EXISTS idx_payouts_worker ON payouts (worker_id);
CREATE INDEX IF NOT EXISTS idx_payouts_worker_created_at ON payouts(worker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts(created_at);
CREATE INDEX IF NOT EXISTS idx_loans_worker_status ON loans(worker_id, status);
CREATE INDEX IF NOT EXISTS idx_workers_is_active ON workers(is_active);
CREATE INDEX IF NOT EXISTS idx_payouts_club ON payouts(club);


-- Add settlement linkage to attendance
ALTER TABLE attendance
ADD COLUMN IF NOT EXISTS settlement_id INTEGER REFERENCES payouts(id);

-- Add fields to payouts for partial vs final and idempotency
ALTER TABLE payouts
ADD COLUMN IF NOT EXISTS parent_settlement_id INTEGER REFERENCES payouts(id),
ADD COLUMN IF NOT EXISTS is_final BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS reversed BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS idempotency_key TEXT UNIQUE;


ALTER TABLE payouts ADD COLUMN IF NOT EXISTS idempotency_key TEXT UNIQUE;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS processed_loans JSONB DEFAULT '[]'::jsonb;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS reversed BOOLEAN DEFAULT FALSE;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS reversed_at TIMESTAMP WITH TIME ZONE;
-- Track which attendance days belong to which final payout
ALTER TABLE attendance ADD COLUMN IF NOT EXISTS settlement_id INTEGER;

-- Track which partial payouts belong to a final settlement
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS parent_settlement_id INTEGER;

-- Index for speed
CREATE INDEX IF NOT EXISTS idx_attendance_settlement ON attendance(settlement_id);


-- Unified Transaction View (For the Ledger Tab)
-- This combines loans and payouts into one chronological feed
CREATE OR REPLACE VIEW transaction_ledger AS
SELECT 
    'loan' as type, id, worker_id, amount as net_amount, 0 as deduction, created_at, 'N/A' as club
FROM loans
UNION ALL
SELECT 
    'payout' as type, id, worker_id, net_amount, loan_deduction, created_at, club
FROM payouts
ORDER BY created_at DESC;
"""

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Optional[Pool] = None

    async def connect(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=10,
                statement_cache_size=0
            )
            logging.info("Connected to Payease PostgreSQL")

    async def setup(self):
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

    # --- WORKER METHODS ---
    async def add_worker(self, name: str, phone: str, club: str, rate: float):
        query = """
            INSERT INTO workers (full_name, phone, club, daily_rate)
            VALUES ($1, $2, $3, $4) RETURNING id
        """
        return await self._pool.fetchval(query, name, phone, club, rate)

    async def get_active_workers(self, club: str = None):
        if club:
            return await self._pool.fetch("SELECT * FROM workers WHERE is_active = TRUE AND club = $1", club)
        return await self._pool.fetch("SELECT * FROM workers WHERE is_active = TRUE")

    async def toggle_worker_status(self, worker_id: int):
        return await self._pool.execute("UPDATE workers SET is_active = NOT is_active WHERE id = $1", worker_id)

    # --- LOAN METHODS ---
    async def add_loan(self, worker_id: int, amount: float):
        return await self._pool.fetchval(
            "INSERT INTO loans (worker_id, amount) VALUES ($1, $2) RETURNING id", 
            worker_id, amount
        )

    async def get_pending_loans_total(self, worker_id: int) -> float:
        query = "SELECT COALESCE(SUM(amount), 0) FROM loans WHERE worker_id = $1 AND status = 'pending'"
        return await self._pool.fetchval(query, worker_id)

    # --- PAYOUT LOGIC (The Core Engine) ---
    async def process_payout(self, worker_id: int, days_worked: int):
        """
        Calculates wage, finds pending loans, deducts them, and records the payout 
        in a single database transaction.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Get worker info
                worker = await conn.fetchrow("SELECT daily_rate, club FROM workers WHERE id = $1", worker_id)
                
                # 2. Calculate Gross
                gross = worker['daily_rate'] * days_worked
                
                # 3. Calculate Loans to deduct
                loan_total = await conn.fetchval(
                    "SELECT COALESCE(SUM(amount), 0) FROM loans WHERE worker_id = $1 AND status = 'pending'", 
                    worker_id
                )
                
                # 4. Final Math
                net = gross - loan_total
                
                # 5. Update loans to 'deducted'
                await conn.execute("UPDATE loans SET status = 'deducted' WHERE worker_id = $1 AND status = 'pending'", worker_id)
                
                # 6. Insert Payout Record
                payout_id = await conn.fetchval("""
                    INSERT INTO payouts (worker_id, days_worked, gross_amount, loan_deduction, net_amount, club)
                    VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
                """, worker_id, days_worked, gross, loan_total, net, worker['club'])
                
                return {"payout_id": payout_id, "net": net, "deducted_loans": loan_total}

    # --- DASHBOARD & ANALYTICS ---
    async def get_dashboard_stats(self):
        query = """
        WITH dashboard AS (
            SELECT 
                (SELECT COUNT(*) FROM workers) as total_workers,
                (SELECT COUNT(*) FROM workers WHERE is_active = TRUE) as active_workers,
                (SELECT COALESCE(SUM(amount), 0) FROM loans WHERE status = 'pending') as total_outstanding_loans,
                (
                    COALESCE((SELECT SUM(rate_at_time) FROM attendance WHERE settlement_id IS NULL), 0) - 
                    COALESCE((SELECT SUM(gross_amount) FROM payouts WHERE is_final = FALSE AND reversed = FALSE AND parent_settlement_id IS NULL), 0)
                ) as total_unpaid,
                (SELECT COALESCE(SUM(net_amount), 0) FROM payouts WHERE reversed = FALSE) as total_money_out,
                (SELECT COUNT(DISTINCT club) FROM workers) as total_clubs
        ),
        weekly AS (
            -- Get last 7 days of payouts grouped by day and club
            SELECT 
                to_char(created_at, 'Mon') as day,
                COALESCE(club, 'General') as club_name,
                SUM(net_amount) as daily_total
            FROM payouts
            WHERE created_at > CURRENT_DATE - INTERVAL '7 days' AND reversed = FALSE
            GROUP BY 1, 2
            ORDER BY MIN(created_at)
        )
        SELECT 
            d.*,
            (SELECT json_agg(weekly.*) FROM weekly) as weekly_stats
        FROM dashboard d;
        """
        row = await self._pool.fetchrow(query)
        # Convert to dict and handle Decimal/Date types for JSON
        res = dict(row)
        res['weekly_stats'] = res.get('weekly_stats') or []
        return res


    async def get_club_distribution(self):
        """Data for pie charts: how many workers per club"""
        return await self._pool.fetch("SELECT club, COUNT(*) as count FROM workers GROUP BY club")

    async def get_unified_ledger(self, limit: int = 20):
        """Fetches the merged view of loans and payouts"""
        return await self._pool.fetch("SELECT * FROM transaction_ledger LIMIT $1", limit)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()# Database connection and SCHEMA_SQL
            
    async def record_daily_attendance(self):
    # This query inserts work for today ONLY if it doesn't already exist
        query = """
            INSERT INTO attendance (worker_id, rate_at_time, work_date)
            SELECT id, daily_rate, CURRENT_DATE
            FROM workers
            WHERE is_active = TRUE
            ON CONFLICT (worker_id, work_date) DO NOTHING;
        """
        try:
            async with self._pool.acquire() as conn:
                status = await conn.execute(query)
                # 'status' looks like "INSERT 0 5" (5 inserted) or "INSERT 0 0" (all were duplicates)
                count = status.split(' ')[-1]
                
                if count == "0":
                    logging.info("Attendance Pulse: All active workers already recorded for today.")
                else:
                    logging.info(f"Attendance Pulse: Successfully recorded {count} workers for today.")
                    
                return status
        except Exception as e:
            logging.error(f"Scheduler failed to record attendance: {e}")
        
    async def get_worker_settlement_summary(self, worker_id: int):
        """
        Calculates total owed since last payout based on attendance history.
        """
        query = """
        WITH last_payment AS (
            SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamp) as last_date
            FROM payouts
            WHERE worker_id = $1 AND reversed = FALSE
        )
        SELECT 
            w.full_name,
            w.daily_rate,
            COUNT(a.id) as days_to_pay,
            (COUNT(a.id) * w.daily_rate) as gross_owed,
            (SELECT COALESCE(SUM(amount), 0) FROM loans WHERE worker_id = $1 AND status = 'pending') as total_debt
        FROM workers w
        LEFT JOIN attendance a ON w.id = a.worker_id
        WHERE w.id = $1 
        AND a.work_date > (SELECT last_date FROM last_payment)
        GROUP BY w.id;
        """
        return await self._pool.fetchrow(query, worker_id)
  

    async def get_flexible_settlement(self, worker_id: int) -> Optional[dict]:
        """
        Returns settlement summary for a worker.

        Fields returned:
        - full_name
        - daily_rate
        - days_on
        - gross_owed
        - already_paid
        - active_loans
        - remaining_before_loans  (gross_owed - already_paid)
        - remaining_after_loans   (remaining_before_loans - active_loans)
        """
        query = """
SELECT
    w.full_name,
    w.daily_rate::numeric AS daily_rate,
    -- days not yet linked to a final settlement
    (SELECT COUNT(*) FROM attendance a WHERE a.worker_id = $1 AND a.settlement_id IS NULL) AS days_on,
    -- FIX 1: Calculate gross based on attendance rate_at_time, not just current rate
    (SELECT COALESCE(SUM(a.rate_at_time), 0) 
     FROM attendance a 
     WHERE a.worker_id = $1 AND a.settlement_id IS NULL) AS gross_owed,
    -- FIX 2: SUM THE GROSS_AMOUNT (Value), NOT NET_AMOUNT (Cash)
    (SELECT COALESCE(SUM(p.gross_amount), 0) FROM payouts p
        WHERE p.worker_id = $1 
        AND p.parent_settlement_id IS NULL 
        AND p.is_final = FALSE 
        AND p.reversed = FALSE) AS already_paid,
    -- total pending loans
    (SELECT COALESCE(SUM(l.amount), 0) FROM loans l WHERE l.worker_id = $1 AND l.status = 'pending') AS active_loans
FROM workers w
WHERE w.id = $1
GROUP BY w.id, w.full_name, w.daily_rate
LIMIT 1;
"""

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, worker_id)
            if not row:
                return None

            # Convert to Python numeric types (floats) safely
            def to_decimal(val):
                if val is None:
                    return Decimal("0")
                return Decimal(str(val))

            daily_rate = to_decimal(row["daily_rate"])
            days_on = int(row["days_on"] or 0)
            gross_owed = to_decimal(row["gross_owed"])
            already_paid = to_decimal(row["already_paid"])
            active_loans = to_decimal(row["active_loans"])

            remaining_before_loans = (gross_owed - already_paid).quantize(Decimal("0.01"))
            remaining_after_loans = (remaining_before_loans - active_loans).quantize(Decimal("0.01"))

            return {
                "full_name": row["full_name"],
                "daily_rate": float(daily_rate),
                "days_on": days_on,
                "gross_owed": float(gross_owed),
                "already_paid": float(already_paid),
                "active_loans": float(active_loans),
                "remaining_before_loans": float(remaining_before_loans),
                "remaining_after_loans": float(remaining_after_loans)
            }


    async def get_weekly_stats(self):
        async with self._pool.acquire() as conn:
            # Fetches sum of payouts grouped by day for the last 7 days
            query = """
             SELECT 
    to_char(created_at, 'Dy') as day, 
    club, 
    SUM(net_amount) as total
FROM payouts
WHERE created_at > now() - interval '7 days'
  AND reversed = FALSE
GROUP BY day, club, date_trunc('day', created_at)
ORDER BY date_trunc('day', created_at) ASC;
            """
            rows = await conn.fetch(query)
            return [{"day": r["day"], "total": float(r["total"])} for r in rows]
