import asyncio
import asyncpg
import json
import uuid
from decimal import Decimal
from datetime import datetime, date, timedelta

# Database Connection Info - Update these
DSN = "postgresql://payease:payeaseforever@localhost:5432/payease_db"


async def populate_test_data():
    conn = await asyncpg.connect(DSN)
    print("Connected to Database.")

    try:
        async with conn.transaction():
            print("Truncating tables...")
            # We truncate in order to respect Foreign Key constraints
            await conn.execute("TRUNCATE workers, loans, payouts, attendance RESTART IDENTITY CASCADE;")

            # 1. Create Workers
            print("Creating workers...")
            workers = [
                ("Abebe Kebede", "0911223344", "Club Alpha", 500.00),
                ("Sara Tekle", "0922334455", "Club Alpha", 600.00),
                ("Mulugeta Tesfaye", "0933445566", "Club Beta", 450.00)
            ]
            
            worker_ids = []
            for name, phone, club, rate in workers:
                wid = await conn.fetchval("""
                    INSERT INTO workers (full_name, phone, club, daily_rate) 
                    VALUES ($1, $2, $3, $4) RETURNING id
                """, name, phone, club, rate)
                worker_ids.append(wid)

            abebe_id, sara_id, mulu_id = worker_ids

            # 2. Add Attendance (The "Automatic Counter")
            # Let's give them 3 days of work history that are NOT yet settled
            print("Adding attendance...")
            today = date.today()
            for i in range(3):
                work_day = today - timedelta(days=i)
                for wid in worker_ids:
                    await conn.execute("""
                        INSERT INTO attendance (worker_id, work_date, rate_at_time)
                        VALUES ($1, $2, (SELECT daily_rate FROM workers WHERE id = $1))
                    """, wid, work_day)

            # 3. Add Loans
            # Abebe has a pending loan of 200 ETB
            print("Adding loans...")
            await conn.execute("""
                INSERT INTO loans (worker_id, amount, status) VALUES ($1, $2, 'pending')
            """, abebe_id, Decimal("200.00"))

            # 4. Add a "Partial Payout"
            # Sara already took a 300 ETB partial payout yesterday
            print("Adding partial payout...")
            await conn.execute("""
                INSERT INTO payouts (worker_id, days_worked, gross_amount, loan_deduction, net_amount, club, idempotency_key)
                VALUES ($1, 0, 300, 0, 300, 'Club Alpha', $2)
            """, sara_id, f"test_partial_{uuid.uuid4()}")

            print("\nSuccessfully populated real test data!")
            print(f"Summary:")
            print(f"- Abebe: 3 days work (1500 ETB) + 200 ETB Loan pending.")
            print(f"- Sara: 3 days work (1800 ETB) + 300 ETB Partial already paid.")
            print(f"- Mulugeta: 3 days work (1350 ETB) - Clean.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(populate_test_data())