"""Mission 002: educational PostgreSQL-backed orchestrated Saga.

Single-process demo worker. Independent service boundaries and external payment
provider are simulated; this is NOT a production payment processor or general SDK.
"""
import os
import uuid
import time
import logging
from contextlib import asynccontextmanager
from threading import Thread, Event
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("saga")
DSN = os.getenv("DATABASE_URL", "postgresql://saga:localdev@localhost:55432/shopflow")
stop = Event()

DDL = """
CREATE TABLE IF NOT EXISTS inventory (
  sku text PRIMARY KEY, available integer NOT NULL CHECK(available >= 0),
  reserved integer NOT NULL DEFAULT 0 CHECK(reserved >= 0)
);
INSERT INTO inventory(sku,available,reserved) VALUES ('headphones',10,0)
ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS sagas (
  id uuid PRIMARY KEY, request_key text UNIQUE NOT NULL, sku text NOT NULL,
  amount integer NOT NULL, state text NOT NULL, step text NOT NULL,
  fail_mode text NOT NULL DEFAULT 'none', attempts integer NOT NULL DEFAULT 0,
  next_attempt timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz, worker_id text, error text,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS operations (
  op_key text PRIMARY KEY, saga_id uuid NOT NULL REFERENCES sagas(id),
  kind text NOT NULL, result text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sagas_due_idx ON sagas(next_attempt)
 WHERE state IN ('RUNNING','COMPENSATING','UNCERTAIN');
"""

def connect():
    return psycopg.connect(DSN, row_factory=dict_row)

def setup():
    with connect() as conn:
        conn.execute(DDL)

class Checkout(BaseModel):
    request_key: str = Field(min_length=1, max_length=100)
    sku: str = "headphones"
    amount: int = Field(default=2500, gt=0)
    fail_mode: str = Field(default="none", pattern="^(none|decline|timeout|compensation_once)$")

def snapshot(saga_id):
    with connect() as conn:
        s = conn.execute("SELECT * FROM sagas WHERE id=%s", (saga_id,)).fetchone()
        if not s: raise HTTPException(404, "Saga not found")
        ops = conn.execute("SELECT op_key,kind,result FROM operations WHERE saga_id=%s ORDER BY created_at,op_key", (saga_id,)).fetchall()
        return {"saga": s, "operations": ops}

def start_checkout(body):
    with connect() as conn:
        existing = conn.execute("SELECT id,sku,amount,fail_mode FROM sagas WHERE request_key=%s", (body.request_key,)).fetchone()
        if existing:
            if (existing["sku"],existing["amount"],existing["fail_mode"]) != (body.sku,body.amount,body.fail_mode):
                raise HTTPException(409, "Idempotency key reused with different payload")
            return snapshot(existing["id"])
        sid = uuid.uuid4()
        try:
            conn.execute("""INSERT INTO sagas(id,request_key,sku,amount,state,step,fail_mode)
              VALUES (%s,%s,%s,%s,'RUNNING','RESERVE',%s)""",
              (sid,body.request_key,body.sku,body.amount,body.fail_mode))
        except psycopg.errors.UniqueViolation:
            conn.rollback()
            row = conn.execute("SELECT id FROM sagas WHERE request_key=%s",(body.request_key,)).fetchone()
            return snapshot(row["id"])
    return snapshot(sid)

def claim(worker):
    # Claim ownership; the lease is a liveness aid, not a fencing guarantee.
    with connect() as conn:
        row = conn.execute("""SELECT id FROM sagas
          WHERE state IN ('RUNNING','COMPENSATING','UNCERTAIN')
          AND next_attempt <= now() AND (lease_until IS NULL OR lease_until < now())
          ORDER BY next_attempt,created_at FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
        if not row: return None
        conn.execute("""UPDATE sagas SET worker_id=%s, lease_until=now()+interval '20 seconds'
          WHERE id=%s""",(worker,row["id"]))
        return row["id"]

def advance(sid, worker):
    # Every simulated business effect and state transition is committed atomically
    # here. Real remote services require durable intent, stable keys, and inquiry.
    with connect() as conn:
        s = conn.execute("SELECT * FROM sagas WHERE id=%s FOR UPDATE",(sid,)).fetchone()
        if not s or s["worker_id"] != worker or s["lease_until"] is None:
            return
        if s["lease_until"].timestamp() < time.time(): return
        state,step = s["state"],s["step"]
        next_state,next_step,error = state,step,None
        if state == "RUNNING" and step == "RESERVE":
            key=f"{sid}:reserve"
            old=conn.execute("SELECT result FROM operations WHERE op_key=%s",(key,)).fetchone()
            if old or conn.execute("""UPDATE inventory SET available=available-1,reserved=reserved+1
                  WHERE sku=%s AND available>0 RETURNING sku""",(s["sku"],)).fetchone():
                conn.execute("""INSERT INTO operations(op_key,saga_id,kind,result)
                  VALUES (%s,%s,'RESERVE','SUCCEEDED') ON CONFLICT DO NOTHING""",(key,sid))
                next_step="CHARGE"
            else:
                next_state,next_step,error="FAILED","DONE","OUT_OF_STOCK"
        elif state == "RUNNING" and step == "CHARGE":
            key=f"{sid}:charge"
            old=conn.execute("SELECT result FROM operations WHERE op_key=%s",(key,)).fetchone()
            if old:
                result=old["result"]
            elif s["fail_mode"] == "decline":
                result="DECLINED"
                conn.execute("""INSERT INTO operations VALUES (%s,%s,'CHARGE',%s,now())""",(key,sid,result))
            else:
                # Timeout models an unknown acknowledgement after a successful charge.
                result="SUCCEEDED"
                conn.execute("""INSERT INTO operations VALUES (%s,%s,'CHARGE',%s,now())""",(key,sid,result))
            if s["fail_mode"] == "timeout" and s["attempts"] == 0:
                next_state,next_step,error="UNCERTAIN","INQUIRE_PAYMENT","PAYMENT_ACK_LOST"
            elif result=="DECLINED":
                next_state,next_step,error="COMPENSATING","RELEASE","PAYMENT_DECLINED"
            else:
                next_step="CONFIRM"
        elif state=="UNCERTAIN" and step=="INQUIRE_PAYMENT":
            # Reconciliation consults the simulated provider operation ledger.
            op=conn.execute("SELECT result FROM operations WHERE op_key=%s",(f"{sid}:charge",)).fetchone()
            if op and op["result"]=="SUCCEEDED": next_state,next_step="RUNNING","CONFIRM"
            elif op and op["result"]=="DECLINED": next_state,next_step="COMPENSATING","RELEASE"
            else:
                error="PAYMENT_OUTCOME_STILL_UNKNOWN"
        elif state=="RUNNING" and step=="CONFIRM":
            conn.execute("""INSERT INTO operations(op_key,saga_id,kind,result)
              VALUES (%s,%s,'CONFIRM','SUCCEEDED') ON CONFLICT DO NOTHING""",(f"{sid}:confirm",sid))
            next_state,next_step="COMPLETED","DONE"
        elif state=="COMPENSATING" and step=="RELEASE":
            if s["fail_mode"]=="compensation_once" and s["attempts"]==0:
                error="INJECTED_RELEASE_FAILURE"
            else:
                key=f"{sid}:release"
                if not conn.execute("SELECT 1 FROM operations WHERE op_key=%s",(key,)).fetchone():
                    conn.execute("""UPDATE inventory SET available=available+1,reserved=reserved-1
                      WHERE sku=%s AND reserved>0""",(s["sku"],))
                    conn.execute("""INSERT INTO operations(op_key,saga_id,kind,result)
                      VALUES (%s,%s,'RELEASE','SUCCEEDED')""",(key,sid))
                next_state,next_step="COMPENSATED","DONE"
        if error and next_state in ("UNCERTAIN","COMPENSATING"):
            log.warning("saga=%s step=%s error=%s",sid,step,error)
        attempts=s["attempts"]+1 if error else 0
        conn.execute("""UPDATE sagas SET state=%s,step=%s,error=%s,attempts=%s,
          next_attempt=now()+(%s * interval '1 second'),lease_until=NULL,
          worker_id=NULL,updated_at=now() WHERE id=%s""",
          (next_state,next_step,error,attempts,min(30,2**min(attempts,5)) if error else 0,sid))

def work_once():
    worker=str(uuid.uuid4())
    sid=claim(worker)
    if sid:
        try: advance(sid,worker)
        except Exception:
            log.exception("Worker failed saga=%s; lease expiry permits retry",sid)
        return str(sid)
    return None

def loop():
    while not stop.wait(0.5):
        try: work_once()
        except Exception: log.exception("Worker polling failed")

@asynccontextmanager
async def lifespan(app):
    setup()
    stop.clear()
    thread=Thread(target=loop,daemon=True)
    thread.start()
    yield
    stop.set()
    thread.join(timeout=3)

app=FastAPI(title="Mission 002 — ShopFlow Saga Lab", lifespan=lifespan)

@app.post("/checkouts",status_code=202)
def create_checkout(body:Checkout):
    return start_checkout(body)

@app.get("/sagas/{saga_id}")
def get_saga(saga_id:uuid.UUID):
    return snapshot(saga_id)

@app.get("/inventory")
def inventory():
    with connect() as conn:
        return conn.execute("SELECT * FROM inventory ORDER BY sku").fetchall()

@app.get("/health")
def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status":"ok"}

@app.post("/admin/worker/run-once")
def run_once():
    # Local teaching endpoint: never expose unauthenticated in production.
    return {"claimed":work_once()}
