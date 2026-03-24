# api/api.py
from aiohttp import web
import aiohttp_cors
import logging
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
import json
import base64
import datetime
import uuid

from app_context import db  # shared Database instance from your app_context

# --- Helpers ---

def _record_to_dict(rec):
    from datetime import datetime, date, timezone  # Add these at the top
    if rec is None:
        return {}
    
    d = dict(rec)
    for k, v in d.items():
        # Handle Currency/Decimals
        if isinstance(v, Decimal):
            d[k] = float(v)
        
        # Handle Dates and Timestamps
        elif isinstance(v, datetime):  # This checks for datetime objects
            if v.tzinfo is None:
                d[k] = v.replace(tzinfo=timezone.utc).isoformat()
            else:
                d[k] = v.isoformat()
        elif isinstance(v, date):  # This checks for simple date objects
            d[k] = v.isoformat()
            
    return d

# Cursor helpers for transactions paging
def _encode_cursor(created_at_iso: str, id: int) -> str:
    payload = json.dumps({"t": created_at_iso, "id": id})
    return base64.urlsafe_b64encode(payload.encode()).decode()

def _decode_cursor(cursor: str) -> Optional[Tuple[str, int]]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        obj = json.loads(raw)
        return obj.get("t"), int(obj.get("id"))
    except Exception:
        return None

# --- Dashboard ---
# Inside your Database class

# Update your API Route
async def get_dashboard(request: web.Request):
    try:
        # 1. Fetch the unified stats and weekly chart data
        stats = await db.get_dashboard_stats()
        weekly = await db.get_weekly_stats() 
        
        # 2. Map the new database keys to the JSON response
        return web.json_response({
            "active_workers": stats["active_workers"],
            "total_workers": stats["total_workers"],
            "total_outstanding_loans": float(stats["total_outstanding_loans"]),
            
            # The new metrics you requested
            "total_unpaid": float(stats["total_unpaid"]),
            "total_money_out": float(stats["total_money_out"]),
            
            "total_clubs": stats["total_clubs"],
            "weekly_stats": weekly 
        })
    except Exception as e:
        logging.exception("Dashboard Failed: %s", e)
        # Return empty/zeroed structure on error to prevent frontend crashes
        return web.json_response({
            "error": "Syncing...",
            "active_workers": 0,
            "total_unpaid": 0,
            "total_money_out": 0,
            "weekly_stats": []
        }, status=200)

# --- Workers ---
async def list_workers(request: web.Request):
    try:
        params = request.rel_url.query
        club = params.get("club")
        q = params.get("q")
        active = params.get("active")

        # Base Query using a CTE (Common Table Expression) for high-performance math
        sql = """
        WITH worker_stats AS (
            SELECT 
                w.id,
                -- Total Labor Value from attendance not yet closed
                COALESCE((SELECT SUM(a.rate_at_time) FROM attendance a WHERE a.worker_id = w.id AND a.settlement_id IS NULL), 0) as gross_owed,
                -- Total Value already cleared via partial payouts
                COALESCE((SELECT SUM(p.gross_amount) FROM payouts p WHERE p.worker_id = w.id AND p.is_final = FALSE AND p.reversed = FALSE AND p.parent_settlement_id IS NULL), 0) as already_paid,
                -- Active Loans
                COALESCE((SELECT SUM(l.amount) FROM loans l WHERE l.worker_id = w.id AND l.status = 'pending'), 0) as active_loan,
                -- Last Payout Date
                (SELECT created_at FROM payouts WHERE worker_id = w.id ORDER BY created_at DESC LIMIT 1) as last_payout_at
            FROM workers w
        )
        SELECT 
            w.id, w.full_name, w.phone, w.club, w.daily_rate, w.is_active, w.created_at, w.registered_at, 
            s.active_loan,
            s.last_payout_at,
            (s.gross_owed - s.already_paid) as unpaid_value
        FROM workers w
        JOIN worker_stats s ON w.id = s.id
        """

        where_clauses = []
        args = []

        if club:
            args.append(club)
            where_clauses.append(f"w.club = ${len(args)}")
        if active is not None:
            val = active.lower() in ("1", "true", "yes")
            args.append(val)
            where_clauses.append(f"w.is_active = ${len(args)}")
        if q:
            args.append(f"%{q}%")
            where_clauses.append(f"w.full_name ILIKE ${len(args)}")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        sql += " ORDER BY w.is_active DESC, w.full_name ASC LIMIT 500"

        async with request.app['db']._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            
            workers = []
            for r in rows:
                # Convert the database row to a mutable dictionary
                rec = dict(r) 
                
                # 1. Handle Decimals/Numerics (JSON can't handle them directly)
                rec["daily_rate"] = float(rec["daily_rate"])
                rec["active_loan"] = float(rec["active_loan"] or 0)
                rec["unpaid_value"] = float(rec["unpaid_value"] or 0)
                
                # 2. Handle Dates
                if rec.get("last_payout_at"):
                    rec["last_payout_at"] = rec["last_payout_at"].isoformat()
                else:
                    rec["last_payout_at"] = None

                # 2. Handle Registration Date (THE FIX)
                if rec.get("registered_at"):
                    # If it exists (manually set), use it
                    rec["registered_at"] = rec["registered_at"].isoformat()
                elif rec.get("created_at"):
                    # Fallback to the DB record timestamp
                    rec["registered_at"] = rec["created_at"].isoformat()
                else:
                    rec["registered_at"] = None

                # 3. Keep created_at for other uses
                if rec.get("created_at"):
                    rec["created_at"] = rec["created_at"].isoformat()

                workers.append(rec)
                print('here are workers', workers)

        return web.json_response(workers)

    except Exception as e:
        logging.exception("Failed to list workers: %s", e)
        return web.json_response([], status=500)
    
    
async def add_loan(request: web.Request):
    try:
        data = await request.json()
        worker_id = data.get("worker_id")
        amount = float(data.get("amount", 0))

        if not worker_id or amount <= 0:
            return web.json_response({"error": "Invalid data"}, status=400)

        async with db._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO loans (worker_id, amount, status) VALUES ($1, $2, 'pending')",
                worker_id, amount
            )
            
        return web.json_response({"status": "success"})
    except Exception as e:
        logging.exception("Loan Error: %s", e)
        return web.json_response({"error": str(e)}, status=500)
    
    
async def add_worker(request: web.Request):
    try:
        data = await request.json()
        name = data.get("full_name")
        phone = data.get("phone")
        club = data.get("club")
        rate = float(data.get("daily_rate", 0))

        if not name or not club:
            raise web.HTTPBadRequest(text="full_name and club are required")

        if hasattr(db, "add_worker"):
            worker_id = await db.add_worker(name=name, phone=phone, club=club, rate=rate)
        else:
            async with db._pool.acquire() as conn:
                worker_id = await conn.fetchval(
                    "INSERT INTO workers (full_name, phone, club, daily_rate, is_active) VALUES ($1,$2,$3,$4,TRUE) RETURNING id",
                    name, phone, club, rate
                )

        return web.json_response({"status": "success", "id": int(worker_id)})
    except web.HTTPError:
        raise
    except Exception as e:
        logging.exception("Failed to add worker: %s", e)
        raise web.HTTPInternalServerError(text="Failed to add worker")

async def update_worker(request: web.Request):
    try:
        worker_id = int(request.match_info["id"])
        data = await request.json()
        
        # Extract all fields sent by the frontend
        name = data.get("full_name")
        club = data.get("club")
        phone = data.get("phone")
        rate = float(data.get("daily_rate", 0))

        if rate < 0:
            raise web.HTTPBadRequest(text="daily_rate must be non-negative")

        async with db._pool.acquire() as conn:
            # Update the SQL to include all fields
            await conn.execute("""
                UPDATE workers 
                SET full_name = $1, club = $2, daily_rate = $3, phone = $4 
                WHERE id = $5
            """, name, club, rate, phone, worker_id)

        return web.json_response({
            "status": "updated", 
            "full_name": name,
            "club": club,
            "daily_rate": rate
        })
    except Exception as e:
        logging.exception("Failed to update worker: %s", e)
        raise web.HTTPInternalServerError(text="Failed to update worker")

async def toggle_worker(request: web.Request):
    try:
        worker_id = int(request.match_info["id"])
        async with db._pool.acquire() as conn:
            # Explicitly cast the result to ensure it's a boolean
            new_status = await conn.fetchval(
                "UPDATE workers SET is_active = NOT is_active WHERE id = $1 RETURNING is_active",
                worker_id
            )
            
            # If worker_id doesn't exist, new_status will be None
            if new_status is None:
                return web.json_response({"error": "Worker not found"}, status=404)

        # Force convert to bool to ensure JSON serialization is strictly true/false
        return web.json_response({
            "status": "updated", 
            "is_active": bool(new_status) 
        })
    except Exception as e:
        logging.exception("Failed to toggle worker: %s", e)
        return web.json_response({"error": "Internal server error"}, status=500)
# --- Loans ---
async def create_loan(request: web.Request):
    try:
        worker_id = int(request.match_info["id"])
        data = await request.json()
        amount = float(data.get("amount", 0))
        if amount <= 0:
            raise web.HTTPBadRequest(text="amount must be positive")

        async with db._pool.acquire() as conn:
            loan_id = await conn.fetchval(
                "INSERT INTO loans (worker_id, amount, status) VALUES ($1, $2, 'pending') RETURNING id",
                worker_id, Decimal(str(amount))
            )

        return web.json_response({"status": "success", "loan_id": int(loan_id)})
    except web.HTTPError:
        raise
    except Exception as e:
        logging.exception("Failed to create loan: %s", e)
        raise web.HTTPInternalServerError(text="Failed to create loan")

# --- Payouts (atomic, idempotent) ---
# Replace or add these handlers in api/api.py
import json
import uuid
import datetime
from decimal import Decimal

UNDO_WINDOW_SECONDS = 10  # seconds allowed for undo/reversal

# --- Confirm payout (improved: records detailed loan adjustments for safe reversal) ---
import json
import uuid
import logging
from decimal import Decimal, ROUND_HALF_UP
from aiohttp import web

DECIMAL_QUANT = Decimal("0.01")

async def confirm_payout(request: web.Request):
    """
    POST /api/payouts/confirm
    REWRITTEN: Gross-Value Accounting Engine.
    Fixes negative 'already paid' bugs and loan-clearing logic.
    """
    try:
        payload = await request.json()
        worker_id = int(payload.get("worker_id"))
        
        # 1. Inputs: Use Decimal to prevent floating point ghosts
        custom_gross = Decimal(str(payload.get("gross_amount", 0))) if payload.get("gross_amount") is not None else None
        custom_loan_deduction = Decimal(str(payload.get("loan_deduction", 0))) if payload.get("loan_deduction") is not None else Decimal("0")
        days_count_hint = int(payload.get("days", 0)) if payload.get("days") is not None else 0
        idempotency_key = payload.get("idempotency_key") or str(uuid.uuid4())

        async with request.app['db']._pool.acquire() as conn:
            async with conn.transaction():
                
                # 2. Idempotency Check (Safety First)
                existing = await conn.fetchrow(
                    "SELECT id, net_amount, is_final FROM payouts WHERE idempotency_key = $1",
                    idempotency_key
                )
                if existing:
                    return web.json_response({
                        "status": "duplicate",
                        "payout_id": existing["id"],
                        "net": float(existing["net_amount"]),
                        "is_final": bool(existing["is_final"])
                    })

                # 3. Calculate Total Owed (Total Labor Value)
                # Sum of specific rates at the time attendance was marked
                gross_owed = await conn.fetchval("""
                    SELECT COALESCE(SUM(rate_at_time), 0)
                    FROM attendance
                    WHERE worker_id = $1 AND settlement_id IS NULL
                """, worker_id)
                gross_owed = Decimal(str(gross_owed)).quantize(DECIMAL_QUANT)

                # 4. Calculate Already Applied Value (Previous Labor Settled)
                # This counts BOTH previous cash and previous loan deductions
                value_already_applied = await conn.fetchval("""
    SELECT COALESCE(SUM(gross_amount), 0) FROM payouts
    WHERE worker_id = $1 
    AND is_final = FALSE 
    AND parent_settlement_id IS NULL 
    AND reversed = FALSE
""", worker_id)
                value_already_applied = Decimal(str(value_already_applied)).quantize(DECIMAL_QUANT)

                # 5. Determine current Record Value
                # record_gross = the amount of debt we are clearing in THIS transaction
                if custom_gross is not None:
                    record_gross = custom_gross.quantize(DECIMAL_QUANT)
                else:
                    record_gross = (gross_owed - value_already_applied)

                # 6. Determine Cash Handover (Net)
                loan_deduction_input = custom_loan_deduction.quantize(DECIMAL_QUANT)
                net_now = (record_gross - loan_deduction_input).quantize(DECIMAL_QUANT)
                
                # Safety: If loan deduction > labor value, cash is 0, not negative
                if net_now < 0:
                    net_now = Decimal("0.00")

                # 7. Process Loan Ledger updates
                processed_loans = []
                remaining_to_deduct = loan_deduction_input
                if remaining_to_deduct > 0:
                    pending_loans = await conn.fetch("""
                        SELECT id, amount FROM loans 
                        WHERE worker_id = $1 AND status = 'pending' 
                        ORDER BY created_at ASC FOR UPDATE
                    """, worker_id)
                    
                    for loan in pending_loans:
                        if remaining_to_deduct <= 0: break
                        loan_id = loan["id"]
                        loan_amt = Decimal(str(loan["amount"]))
                        deduct = min(loan_amt, remaining_to_deduct)
                        remaining_to_deduct -= deduct
                        processed_loans.append({"loan_id": loan_id, "deducted": float(deduct)})

                        if deduct >= loan_amt:
                            await conn.execute("UPDATE loans SET status = 'deducted', amount = 0 WHERE id = $1", loan_id)
                        else:
                            await conn.execute("UPDATE loans SET amount = amount - $1 WHERE id = $2", float(deduct), loan_id)

                # 8. Decision Gate: Is this the end of the line for these records?
                # Attendance is closed if (Past Value + Current Value) matches (Total Owed)
                total_value_cleared = value_already_applied + record_gross
                will_close = total_value_cleared >= (gross_owed - Decimal("0.01")) 

                # 9. Execute Payout Insertion
                payout_id = await conn.fetchval("""
                    INSERT INTO payouts (
                        worker_id, days_worked, gross_amount, loan_deduction,
                        net_amount, club, idempotency_key, processed_loans, is_final
                    )
                    VALUES ($1, $2, $3, $4, $5, (SELECT club FROM workers WHERE id = $1), $6, $7::jsonb, $8)
                    RETURNING id
                """, worker_id, days_count_hint, float(record_gross), float(loan_deduction_input), 
                   float(net_now), idempotency_key, json.dumps(processed_loans), will_close)

                if will_close:
                    # Finalize: Seal attendance and parent the partials
                    await conn.execute("""
                        UPDATE attendance SET settlement_id = $1
                        WHERE worker_id = $2 AND settlement_id IS NULL
                    """, payout_id, worker_id)

                    await conn.execute("""
                        UPDATE payouts SET parent_settlement_id = $1
                        WHERE worker_id = $2 AND parent_settlement_id IS NULL AND id != $1
                    """, payout_id, worker_id)
                    status = "final"
                else:
                    status = "partial"

        # 10. Final Response for UI Refresh
        return web.json_response({
            "status": "success",
            "payout_id": payout_id,
            "type": status,
            "net_now": float(net_now),
            "record_gross": float(record_gross),
            "remaining_balance": float(max(0, gross_owed - total_value_cleared))
        })

    except Exception as e:
        logging.exception("Payout System Failure: %s", e)
        return web.json_response({"error": str(e)}, status=500)
# --- Reverse payout (undo) ---
async def reverse_payout(request: web.Request):
    """
    POST /api/payouts/reverse/{payout_id}
    Behavior:
      - Allows a short undo window (UNDO_WINDOW_SECONDS) to reverse a payout.
      - Uses the stored payouts.processed_loans JSON to restore loan rows to their previous state.
      - Marks the payout as reversed (sets reversed = TRUE, reversed_at timestamp) to prevent double reversal.
      - Runs inside a DB transaction and locks affected rows.
    """
    try:
        payout_id = int(request.match_info["payout_id"])

        async with db._pool.acquire() as conn:
            async with conn.transaction():
                # Fetch payout and ensure it exists
                payout = await conn.fetchrow(
                    "SELECT id, worker_id, net_amount, created_at, processed_loans, reversed FROM payouts WHERE id = $1 FOR UPDATE",
                    payout_id
                )
                if not payout:
                    raise web.HTTPNotFound(text="payout not found")

                if payout.get("reversed"):
                    return web.json_response({"status": "already_reversed", "payout_id": payout_id}, status=200)

                created_at = payout["created_at"]
                now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                # Allow reversal only within the undo window
                elapsed = (now_utc - created_at).total_seconds()
                if elapsed > UNDO_WINDOW_SECONDS:
                    raise web.HTTPForbidden(text="Undo window expired")

                processed_loans = payout.get("processed_loans") or []
                # If processed_loans is stored as text, parse it
                if isinstance(processed_loans, str):
                    try:
                        processed_loans = json.loads(processed_loans)
                    except Exception:
                        processed_loans = []

                # Restore loans using the recorded prev_amount and deducted values
                for entry in processed_loans:
                    loan_id = int(entry.get("loan_id"))
                    prev_amount = Decimal(str(entry.get("prev_amount", 0)))
                    deducted = Decimal(str(entry.get("deducted", 0)))

                    # Lock the loan row
                    loan_row = await conn.fetchrow("SELECT id, amount, status FROM loans WHERE id = $1 FOR UPDATE", loan_id)
                    if not loan_row:
                        # If loan row missing, skip but log
                        logging.warning("Loan id %s referenced by payout %s not found during reversal", loan_id, payout_id)
                        continue

                    # Compute what to set back: if prev_amount was fully deducted, restore prev_amount and set status pending
                    # If partial deduction, add back deducted amount to current amount
                    # We rely on prev_amount to be the amount before deduction
                    await conn.execute(
                        "UPDATE loans SET amount = $1, status = 'pending' WHERE id = $2",
                        prev_amount, loan_id
                    )

                # Optionally, insert a reversal record into a reversals table for audit (if exists)
                # Mark payout as reversed
                await conn.execute(
                    "UPDATE payouts SET reversed = TRUE, reversed_at = now() WHERE id = $1",
                    payout_id
                )

                # Insert a ledger reversal entry (transaction_ledger) if you maintain one
                # This is optional and depends on your schema; safe to skip if not present.

        return web.json_response({"status": "reversed", "payout_id": payout_id})
    except web.HTTPError:
        raise
    except Exception as e:
        logging.exception("Failed to reverse payout: %s", e)
        raise web.HTTPInternalServerError(text="Failed to reverse payout")

async def bulk_payout(request: web.Request):
    """
    POST /api/payouts/bulk
    Body: { idempotency_key (recommended) }
    Server will compute payouts for all active workers and return a preview and/or processed result.
    """
    try:
        payload = await request.json()
        idempotency_key = payload.get("idempotency_key") or str(uuid.uuid4())

        async with db._pool.acquire() as conn:
            async with conn.transaction():
                # Idempotency: check if this bulk key already processed
                existing = await conn.fetchrow("SELECT id FROM payouts WHERE idempotency_key = $1 LIMIT 1", idempotency_key)
                if existing:
                    return web.json_response({"status": "duplicate", "message": "Bulk idempotency key already used"})

                # Fetch active workers
                workers = await conn.fetch("SELECT id, daily_rate FROM workers WHERE is_active = TRUE")
                results = []
                for w in workers:
                    wid = w["id"]
                    daily_rate = Decimal(w["daily_rate"])
                    days = 1  # default; you can extend to accept days per worker
                    gross = (daily_rate * Decimal(days)).quantize(Decimal("0.01"))

                    # Sum pending loans and lock
                    pending_loans = await conn.fetch(
                        "SELECT id, amount FROM loans WHERE worker_id = $1 AND status = 'pending' ORDER BY created_at ASC FOR UPDATE",
                        wid
                    )
                    loan_deduction = Decimal("0.00")
                    processed_loan_ids = []
                    remaining_gross = gross

                    for loan in pending_loans:
                        loan_amount = Decimal(loan["amount"])
                        if remaining_gross <= 0:
                            break
                        deduct = min(loan_amount, remaining_gross)
                        loan_deduction += deduct
                        remaining_gross -= deduct
                        processed_loan_ids.append(int(loan["id"]))
                        if deduct >= loan_amount:
                            await conn.execute("UPDATE loans SET status = 'deducted' WHERE id = $1", loan["id"])
                        else:
                            new_amount = loan_amount - deduct
                            await conn.execute("UPDATE loans SET amount = $1 WHERE id = $2", new_amount, loan["id"])

                    net = (gross - loan_deduction).quantize(Decimal("0.01"))
                    payout_id = await conn.fetchval(
                        "INSERT INTO payouts (worker_id, days_worked, gross_amount, loan_deduction, net_amount, club, created_at, idempotency_key) "
                        "VALUES ($1,$2,$3,$4,$5,(SELECT club FROM workers WHERE id = $1), now(), $6) RETURNING id",
                        wid, days, gross, loan_deduction, net, idempotency_key
                    )
                    results.append({
                        "worker_id": wid,
                        "payout_id": int(payout_id),
                        "gross": float(gross),
                        "loan_deduction": float(loan_deduction),
                        "net": float(net),
                        "processed_loans": processed_loan_ids
                    })

        return web.json_response({"status": "success", "results": results})
    except Exception as e:
        logging.exception("Failed to run bulk payout: %s", e)
        raise web.HTTPInternalServerError(text="Failed to run bulk payout")

# --- Transactions feed (unified, filterable, cursor paging) ---
async def get_transactions(request: web.Request):
    try:
        params = request.rel_url.query
        limit = min(int(params.get("limit", 50)), 500)
        cursor = params.get("cursor")
        sort = params.get("sort", "created_at:desc")
        q = params.get("q")
        worker_id = params.get("worker_id")
        club = params.get("club")
        tx_type = params.get("type") or params.get("kind")
        min_amount = params.get("min_amount")
        max_amount = params.get("max_amount")
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        sql = """
            SELECT tl.type, tl.id, tl.worker_id, w.full_name as worker_name,
                   COALESCE(tl.net_amount, tl.amount, 0) as net_amount,
                   COALESCE(tl.deduction, 0) as loan_deduction, tl.created_at, tl.club
            FROM transaction_ledger tl
            LEFT JOIN workers w ON w.id = tl.worker_id
        """
        where_clauses = []
        args = []

        if tx_type:
            args.append(tx_type)
            where_clauses.append(f"tl.type = ${len(args)}")
        if worker_id:
            args.append(int(worker_id))
            where_clauses.append(f"tl.worker_id = ${len(args)}")
        if club:
            args.append(club)
            where_clauses.append(f"tl.club = ${len(args)}")
        if min_amount:
            args.append(float(min_amount))
            where_clauses.append(f"COALESCE(tl.net_amount, tl.amount, 0) >= ${len(args)}")
        if max_amount:
            args.append(float(max_amount))
            where_clauses.append(f"COALESCE(tl.net_amount, tl.amount, 0) <= ${len(args)}")
        if start_date:
            args.append(start_date)
            where_clauses.append(f"tl.created_at >= ${len(args)}::timestamptz")
        if end_date:
            args.append(end_date)
            where_clauses.append(f"tl.created_at <= ${len(args)}::timestamptz")

        if q:
            if q.isdigit():
                args.append(int(q))
                args.append(f"%{q}%")
                where_clauses.append(f"(tl.id = ${len(args)-1} OR w.full_name ILIKE ${len(args)})")
            else:
                args.append(f"%{q}%")
                where_clauses.append(f"w.full_name ILIKE ${len(args)}")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        order_field, order_dir = sort.split(":") if ":" in sort else ("created_at", "desc")
        order_dir = order_dir.lower()

        if cursor:
            decoded = _decode_cursor(cursor)
            if decoded:
                cur_time, cur_id = decoded
                args.append(cur_time)
                args.append(cur_id)
                if order_dir == "desc":
                    sql += f" AND (tl.created_at < ${len(args)-1} OR (tl.created_at = ${len(args)-1} AND tl.id < ${len(args)}))"
                else:
                    sql += f" AND (tl.created_at > ${len(args)-1} OR (tl.created_at = ${len(args)-1} AND tl.id > ${len(args)}))"

        sql += f" ORDER BY tl.{order_field} {order_dir.upper()}, tl.id {order_dir.upper()} LIMIT ${len(args)+1}"
        args.append(limit + 1)

        async with db._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)

        items = []
        for r in rows[:limit]:
            items.append(_record_to_dict(r))

        next_cursor = None
        if len(rows) > limit:
            last = rows[limit - 1]
            created_at_iso = last["created_at"].isoformat()
            next_cursor = _encode_cursor(created_at_iso, last["id"])

        return web.json_response({
            "items": items,
            "next_cursor": next_cursor,
            "count": len(items)
        })
    except Exception as e:
        logging.exception("Failed to fetch transactions: %s", e)
        return web.json_response({"items": [], "next_cursor": None, "count": 0})


    

async def list_workers(request: web.Request):
    try:
        params = request.rel_url.query
        club = params.get("club")
        q = params.get("q")
        active = params.get("active")

        # Use request.app['db'] to get your database instance
        db_instance = request.app['db']

        sql = """
        WITH worker_stats AS (
            SELECT 
                w.id,
                COALESCE((SELECT SUM(a.rate_at_time) FROM attendance a WHERE a.worker_id = w.id AND a.settlement_id IS NULL), 0) as gross_owed,
                COALESCE((SELECT SUM(p.gross_amount) FROM payouts p WHERE p.worker_id = w.id AND p.is_final = FALSE AND p.reversed = FALSE AND p.parent_settlement_id IS NULL), 0) as already_paid,
                COALESCE((SELECT SUM(l.amount) FROM loans l WHERE l.worker_id = w.id AND l.status = 'pending'), 0) as active_loan,
                (SELECT created_at FROM payouts WHERE worker_id = w.id ORDER BY created_at DESC LIMIT 1) as last_payout_at
            FROM workers w
        )
        SELECT 
            w.id, w.full_name, w.phone, w.club, w.daily_rate, w.is_active,w.registered_at, w.created_at,
            COALESCE(s.active_loan, 0) as active_loan,
            s.last_payout_at,
            COALESCE((s.gross_owed - s.already_paid), 0) as unpaid_value
        FROM workers w
        LEFT JOIN worker_stats s ON w.id = s.id
        """

        where_clauses = []
        args = []

        if club:
            args.append(club)
            where_clauses.append(f"w.club = ${len(args)}")
        if active is not None:
            val = active.lower() in ("1", "true", "yes")
            args.append(val)
            where_clauses.append(f"w.is_active = ${len(args)}")
        if q:
            args.append(f"%{q}%")
            where_clauses.append(f"w.full_name ILIKE ${len(args)}")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        sql += " ORDER BY w.is_active DESC, w.full_name ASC LIMIT 500"

        async with db_instance._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            
            workers = []
            for r in rows:
                rec = dict(r) 
                
                # Cast numeric types for JSON
                rec["daily_rate"] = float(rec["daily_rate"] or 0)
                rec["active_loan"] = float(rec["active_loan"] or 0)
                rec["unpaid_value"] = float(rec["unpaid_value"] or 0)
                
                # ISO Format for dates
                if rec.get("registered_at"):
                    # This converts date(2026, 3, 24) to "2026-03-24"
                    rec["registered_at"] = str(rec["registered_at"]) 
                elif rec.get("created_at"):
                    # Fallback to created_at if registered_at is empty
                    rec["registered_at"] = str(rec["created_at"])
                else:
                    rec["registered_at"] = None

                # 3. Handle other timestamps
                if rec.get("last_payout_at"):
                    rec["last_payout_at"] = str(rec["last_payout_at"])
                
                if rec.get("created_at"):
                    rec["created_at"] = str(rec["created_at"])

                workers.append(rec)

        return web.json_response(workers)

    except Exception as e:
        # If you see this in your logs, the query failed!
        logging.exception("LIST_WORKERS_ERROR: %s", e)
        # Return empty list so the frontend doesn't crash, but you'll know it's broken
        return web.json_response([])
async def add_worker(request: web.Request):
    """
    Adds a new worker with retroactive registration support.
    Expects JSON:
      { "full_name": "...", "phone": "...", "club": "...", "daily_rate": 500, "registered_at": "YYYY-MM-DD" }
    """
    try:
        data = await request.json()
        name = data.get("full_name")
        phone = data.get("phone")
        club = data.get("club")
        rate = float(data.get("daily_rate", 0))
        
        # 1. Capture the new date field, default to today if null/missing
        registered_at = data.get("registered_at") or datetime.date.today().isoformat()

        if not name or not club:
            raise web.HTTPBadRequest(text="full_name and club are required")

        # 2. Update the helper call to include 'registered_at'
        if hasattr(db, "add_worker"):
            # This fixes the "missing 1 required positional argument" error
            worker_id = await db.add_worker(
                name=name, 
                phone=phone, 
                club=club, 
                rate=rate, 
                registered_at=registered_at
            )
        else:
            # Fallback manual insert also updated for the 2030 Ultra schema
            async with db._pool.acquire() as conn:
                worker_id = await conn.fetchval(
                    """
                    INSERT INTO workers (full_name, phone, club, daily_rate, registered_at, is_active) 
                    VALUES ($1, $2, $3, $4, $5::DATE, TRUE) 
                    RETURNING id
                    """,
                    name, phone, club, rate, registered_at
                )

        return web.json_response({"status": "success", "id": int(worker_id)})
        
    except web.HTTPError:
        raise
    except Exception as e:
        logging.exception("Failed to add worker: %s", e)
        # Return a clearer error to the frontend
        return web.json_response({"error": str(e)}, status=500)


# In api.py, add this route
async def delete_worker(request: web.Request):
    try:
        # Get ID from URL path
        worker_id = int(request.match_info['id'])
        
        success = await db.delete_worker(worker_id)
        
        if success:
            return web.json_response({"status": "success"})
        return web.json_response({"error": "Worker not found"}, status=404)
    except Exception as e:
        logging.error(f"API Delete Error: {e}")
        return web.json_response({"error": str(e)}, status=500)
    

    
async def toggle_worker(request: web.Request):
    """
    Toggle worker active status. POST /api/workers/{id}/toggle
    """
    try:
        worker_id = int(request.match_info["id"])
        # If DB helper exists, use it
        if hasattr(db, "toggle_worker_status"):
            await db.toggle_worker_status(worker_id)
        else:
            async with db._pool.acquire() as conn:
                # Flip boolean atomically
                await conn.execute(
                    "UPDATE workers SET is_active = NOT is_active WHERE id = $1",
                    worker_id
                )
        return web.json_response({"status": "updated"})
    except Exception as e:
        logging.exception("Failed to toggle worker: %s", e)
        raise web.HTTPInternalServerError(text="Failed to toggle worker")


async def get_ledger(request: web.Request):
    """
    Returns unified transaction ledger (loans + payouts).
    Query params:
      - limit (default 20)
      - worker_id (optional)
    """
    try:
        limit = int(request.query.get("limit", 20))
        worker_id = request.query.get("worker_id")

        sql = "SELECT * FROM transaction_ledger"
        args: List[Any] = []
        if worker_id:
            args.append(int(worker_id))
            sql += f" WHERE worker_id = ${len(args)}"
        sql += " ORDER BY created_at DESC LIMIT $%d" % (len(args) + 1)
        args.append(limit)

        async with db._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)

        return web.json_response([_record_to_dict(r) for r in rows])
    except Exception as e:
        logging.exception("Failed to fetch ledger: %s", e)
        # Return empty ledger on failure to avoid UI breakage
        return web.json_response([])


# Add imports at top of file
import base64
import json
from typing import Optional, Tuple

# Helper: encode/decode cursor (created_at, id)
def _encode_cursor(created_at: str, id: int) -> str:
    payload = json.dumps({"t": created_at, "id": id})
    return base64.urlsafe_b64encode(payload.encode()).decode()

def _decode_cursor(cursor: str) -> Optional[Tuple[str, int]]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        obj = json.loads(raw)
        return obj.get("t"), int(obj.get("id"))
    except Exception:
        return None
    
# --- Worker Detail ---
async def get_worker_detail(request: web.Request):
    """
    GET /api/workers/{id}/detail
    Returns full profile for a worker including loans and payouts.
    """
    try:
        worker_id = int(request.match_info["id"])
        async with db._pool.acquire() as conn:
            # Basic worker info
            worker = await conn.fetchrow("""
                SELECT id, full_name, club, daily_rate, is_active, created_at
                FROM workers
                WHERE id = $1
            """, worker_id)
            if not worker:
                raise web.HTTPNotFound(text="Worker not found")

            # Active loan balance
            active_loan = await conn.fetchval("""
                SELECT COALESCE(SUM(amount),0)
                FROM loans
                WHERE worker_id = $1 AND status = 'pending'
            """, worker_id)

            # Loan history
            loans = await conn.fetch("""
                SELECT id, amount, status, created_at
                FROM loans
                WHERE worker_id = $1
                ORDER BY created_at DESC
                LIMIT 20
            """, worker_id)

            # Payout history
            payouts = await conn.fetch("""
                SELECT id, gross_amount, loan_deduction, net_amount, days_worked, created_at
                FROM payouts
                WHERE worker_id = $1
                ORDER BY created_at DESC
                LIMIT 20
            """, worker_id)

            # Last payout date
            last_payout = await conn.fetchval("""
                SELECT created_at
                FROM payouts
                WHERE worker_id = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, worker_id)

        return web.json_response({
            "id": worker["id"],
            "full_name": worker["full_name"],
            "club": worker["club"],
            "daily_rate": float(worker["daily_rate"]),
            "is_active": worker["is_active"],
            "created_at": worker["created_at"].isoformat(),
            "active_loan": float(active_loan or 0),
            "last_payout": last_payout.isoformat() if last_payout else None,
            "loans": [
                {
                    "id": l["id"],
                    "amount": float(l["amount"]),
                    "status": l["status"],
                    "created_at": l["created_at"].isoformat()
                } for l in loans
            ],
            "payouts": [
                {
                    "id": p["id"],
                    "gross": float(p["gross_amount"]),
                    "deduction": float(p["loan_deduction"]),
                    "net": float(p["net_amount"]),
                    "days": p["days_worked"],
                    "created_at": p["created_at"].isoformat()
                } for p in payouts
            ]
        })
    except Exception as e:
        logging.exception("Failed to fetch worker detail: %s", e)
        raise web.HTTPInternalServerError(text="Failed to fetch worker detail")



async def get_settlement_summary(request):
    worker_id = int(request.match_info['id'])
    db = request.app['db']
    
    # Use the more detailed flexible query logic
    summary = await db.get_flexible_settlement(worker_id)
    
    if not summary:
        return web.json_response({"error": "Worker not found"}, status=404)
        
    # Map the database column names to exactly what the JS expects
    gross_owed = float(summary['days_on'] * summary['daily_rate'])
    already_paid = float(summary['already_paid'])
    total_debt = float(summary['active_loans'])
    
    return web.json_response({
        "full_name": summary['full_name'],
        "daily_rate": float(summary['daily_rate']),
        "effective_days": summary['days_on'],
        "gross_owed": gross_owed,
        "already_paid": already_paid,
        "total_debt": total_debt,
        "net_settlement": (gross_owed - already_paid) - total_debt
    })
    
async def get_transactions(request: web.Request):
    """
    GET /api/transactions
    Supports: limit, cursor, sort, q, worker_id, club, type, min_amount, max_amount, start_date, end_date
    """
    try:
        params = request.rel_url.query
        limit = min(int(params.get("limit", 50)), 500)
        cursor = params.get("cursor")
        sort = params.get("sort", "created_at:desc")
        q = params.get("q")
        worker_id = params.get("worker_id")
        club = params.get("club")
        tx_type = params.get("type") or params.get("kind")
        min_amount = params.get("min_amount")
        max_amount = params.get("max_amount")
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        # Base SQL: join workers to get full_name
        # transaction_ledger view has (type, id, worker_id, net_amount, deduction, created_at, club)
        sql = """
            SELECT tl.type, tl.id, tl.worker_id, w.full_name as worker_name,
                   tl.net_amount, tl.deduction as loan_deduction, tl.created_at, tl.club
            FROM transaction_ledger tl
            LEFT JOIN workers w ON w.id = tl.worker_id
        """
        where_clauses = []
        args = []

        # Filters
        if tx_type:
            args.append(tx_type)
            where_clauses.append(f"tl.type = ${len(args)}")
        if worker_id:
            args.append(int(worker_id))
            where_clauses.append(f"tl.worker_id = ${len(args)}")
        if club:
            args.append(club)
            where_clauses.append(f"tl.club = ${len(args)}")
        if min_amount:
            args.append(float(min_amount))
            where_clauses.append(f"COALESCE(tl.net_amount, tl.amount, 0) >= ${len(args)}")
        if max_amount:
            args.append(float(max_amount))
            where_clauses.append(f"COALESCE(tl.net_amount, tl.amount, 0) <= ${len(args)}")
        if start_date:
            args.append(start_date)
            where_clauses.append(f"tl.created_at >= ${len(args)}::timestamptz")
        if end_date:
            args.append(end_date)
            where_clauses.append(f"tl.created_at <= ${len(args)}::timestamptz")

        # Full text / simple search on worker name or id
        if q:
            # If q looks numeric, search id too
            if q.isdigit():
                args.append(int(q))
                where_clauses.append(f"(tl.id = ${len(args)} OR w.full_name ILIKE ${len(args)+1})")
                args.append(f"%{q}%")
            else:
                args.append(f"%{q}%")
                where_clauses.append(f"w.full_name ILIKE ${len(args)}")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        # Cursor-based pagination: decode cursor into (created_at, id)
        # Default ordering
        order_field, order_dir = sort.split(":") if ":" in sort else ("created_at", "desc")
        order_dir = order_dir.lower()
        if cursor:
            decoded = _decode_cursor(cursor)
            if decoded:
                cur_time, cur_id = decoded
                # For descending order, fetch rows strictly before the cursor
                if order_dir == "desc":
                    args.append(cur_time)
                    args.append(cur_id)
                    sql += f" AND (tl.created_at < ${len(args)-1} OR (tl.created_at = ${len(args)-1} AND tl.id < ${len(args)}))"
                else:
                    args.append(cur_time)
                    args.append(cur_id)
                    sql += f" AND (tl.created_at > ${len(args)-1} OR (tl.created_at = ${len(args)-1} AND tl.id > ${len(args)}))"

        sql += f" ORDER BY tl.{order_field} {order_dir.upper()}, tl.id {order_dir.upper()} LIMIT ${len(args)+1}"
        args.append(limit + 1)  # fetch one extra to determine next_cursor

        async with db._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)

        items = []
        for r in rows[:limit]:
            items.append(_record_to_dict(r))

        next_cursor = None
        if len(rows) > limit:
            last = rows[limit - 1]
            # created_at may be datetime; convert to ISO for cursor
            created_at_iso = last["created_at"].isoformat()
            next_cursor = _encode_cursor(created_at_iso, last["id"])

        return web.json_response({
            "items": items,
            "next_cursor": next_cursor,
            "count": len(items)
        })
    except Exception as e:
        logging.exception("Failed to fetch transactions: %s", e)
        return web.json_response({"items": [], "next_cursor": None, "count": 0})


import os
import qrcode
import asyncio
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

class ReportEngine:
    def __init__(self, db_pool, bot):
        self.db = db_pool
        self.bot = bot
        self.styles = getSampleStyleSheet()
        self.accent_color = colors.HexColor("#00d2ff") 
        
    async def _fetch_report_data(self, filters):
        mode = filters.get('mode', 'audit')
        days = int(filters.get('range', 30))
        
        # Base query using the same view as your get_transactions
        query = """
            SELECT tl.created_at as date, w.full_name as worker_name, tl.club, tl.net_amount as amount
            FROM transaction_ledger tl
            LEFT JOIN workers w ON tl.worker_id = w.id
            WHERE tl.created_at >= CURRENT_DATE - (INTERVAL '1 day' * $1)
        """
        params = [days]

        if mode == 'worker' and filters.get('workerId'):
            query += " AND tl.worker_id = $2"
            params.append(int(filters['workerId']))
        elif mode == 'debt':
            query = """
                SELECT created_at as date, 'Active Loan' as worker_name, 'SYSTEM' as club, amount 
                FROM loans 
                WHERE status = 'pending'
            """
            params = [] # Debt mode doesn't need the 'days' interval usually

        async with self.db._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def generate_and_send(self, user_id, filters):
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        filename = f"report_{filters['mode']}_{timestamp}.pdf"
        qr_path = None  # Initialize as None so the 'finally' block knows it might not exist yet
        
        try:
            data = await self._fetch_report_data(filters)
            doc = SimpleDocTemplate(filename, pagesize=A4)
            elements = []

            # 2030 Branded Header
            elements.append(Paragraph("PAYEASE PRO", self._get_title_style()))
            elements.append(Paragraph(f"AUDIT MODE: {filters['mode'].upper()}", self._get_subtitle_style()))
            elements.append(Spacer(1, 20))

            # Data Table
            table_data = [["DATE", "WORKER", "CLUB", "AMOUNT (ETB)"]]
            for row in data:
                table_data.append([
                    row['date'].strftime('%d %b %y'),
                    row['worker_name'] or "N/A",
                    row['club'] or "GENERAL",
                    f"{float(row['amount']):,.2f}"
                ])

            t = Table(table_data, colWidths=[80, 180, 100, 120])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.accent_color),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
                ('GRID', (0, 0), (-1, -1), 0.1, colors.grey)
            ]))
            elements.append(t)

            # High-End QR Verification
            qr_path = f"qr_{timestamp}.png" # Now we assign it
            qr = qrcode.make(f"AUTH-{timestamp}")
            qr.save(qr_path)
            
            elements.append(Spacer(1, 40))
            elements.append(Image(qr_path, width=60, height=60))
            elements.append(Paragraph("VERIFIED SYSTEM RECORD", self._get_subtitle_style()))

            doc.build(elements)

            from aiogram.types import FSInputFile
            await self.bot.send_document(user_id, FSInputFile(filename), caption="⚡️ *Report Generated.*")
        except Exception as e:
            logging.error(f"Engine Failure: {e}")
        finally:
            # Clean up files so your server doesn't get cluttered
            for f in [filename, qr_path]:
                if f and os.path.exists(f): # Check if f is not None AND exists
                    os.remove(f)

    def _get_title_style(self):
        return ParagraphStyle('Title', fontSize=22, textColor=self.accent_color, fontName='Helvetica-Bold')

    def _get_subtitle_style(self):
        return ParagraphStyle('Subtitle', fontSize=9, textColor=colors.grey, fontName='Helvetica-Bold')
    
report_engine = None

async def handle_report_request(request: web.Request):
    global report_engine # Use the placeholder
    data = await request.json()
    user_id = data.get("telegram_id")
    
    if report_engine:
        asyncio.create_task(report_engine.generate_and_send(user_id, data))
        return web.json_response({"status": "processing"})
    else:
        return web.json_response({"error": "Engine Not Initialized"}, status=500)

# 2. Add a function to "Inject" the bot later
def initialize_report_engine(db_pool, bot_instance):
    global report_engine
    report_engine = ReportEngine(db_pool=db_pool, bot=bot_instance)
    
    
# --- Route registration and CORS setup ---
def setup_admin_routes(app: web.Application):
    app.router.add_get("/api/dashboard", get_dashboard)
    app.router.add_get("/api/workers", list_workers)
    app.router.add_post("/api/workers", add_worker)
    app.router.add_post("/api/loans", add_loan)
    app.router.add_post("/api/workers/{id}/toggle", toggle_worker)
    app.router.add_post("/api/workers/{id}/loan", create_loan)
    app.router.add_post("/api/workers/{id}/update", update_worker)
    app.router.add_post("/api/payouts/confirm", confirm_payout)
    app.router.add_post("/api/reports/generate", handle_report_request)
    app.router.add_post("/api/payouts/reverse/{payout_id}", reverse_payout)
    app.router.add_post("/api/payouts/bulk", bulk_payout)
    app.router.add_get("/api/transactions", get_transactions)
    app.router.add_get("/api/workers/{id}/detail", get_worker_detail)
    app.router.add_get("/api/workers/{id}/settlement-summary", get_settlement_summary)
    app.router.add_post("/api/workers/{id}/delete", delete_worker)


    app.router.add_get("/api/ledger", get_transactions)  # backward compatibility
