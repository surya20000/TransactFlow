import os
import time
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER','transactflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD','transactflow')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5432')}/"
    f"{os.getenv('POSTGRES_DB','transactflow')}"
)

engine: Optional[sqlalchemy.engine.Engine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5)
    yield
    if engine:
        engine.dispose()


app = FastAPI(
    title       = "TransactFlow Risk API",
    description = "Account risk features and device linkage",
    version     = "1.0.0",
    lifespan    = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET"],
    allow_headers  = ["*"],
)


class AccountFeatures(BaseModel):
    account_id:         str
    txn_count:          int
    total_amount:       float
    avg_amount:         float
    max_txn_count_24h:  int
    spike_count:        int
    distinct_merchants: int
    distinct_cities:    int
    latest_date:        str


class RiskResponse(BaseModel):
    account_id:    str
    risk_score:    float = Field(..., ge=0.0, le=1.0)
    risk_label:    str   = Field(..., pattern=r"^(LOW|MEDIUM|HIGH)$")
    features:      AccountFeatures
    score_factors: list[str]
    latency_ms:    float


class LinkedAccount(BaseModel):
    account_id:   str
    device_id:    str
    txn_count:    int
    total_amount: float


class DeviceResponse(BaseModel):
    device_id:          str
    distinct_accounts:  int
    linked_accounts:    list[LinkedAccount]
    multi_account_flag: bool
    latency_ms:         float


class HealthResponse(BaseModel):
    status:  str
    db:      str
    version: str


def compute_risk_score(feat: AccountFeatures, device_shared: int) -> tuple[float, str, list[str]]:
    """
    Simple rule-based score in [0.0, 1.0].
    Four independent factors are capped and summed; label is LOW/MEDIUM/HIGH.
    """
    factors: list[str] = []
    score = 0.0

    spike_rate    = feat.spike_count / max(feat.txn_count, 1)
    spike_contrib = min(spike_rate * 1.5, 0.35)
    score += spike_contrib
    if spike_contrib > 0.10:
        factors.append(f"High amount-spike rate: {spike_rate:.1%}")

    velocity_contrib = min(feat.max_txn_count_24h / 100.0, 0.30)
    score += velocity_contrib
    if feat.max_txn_count_24h > 20:
        factors.append(f"High velocity: {feat.max_txn_count_24h} txns in 24h")

    device_contrib = min((device_shared - 1) * 0.05, 0.25) if device_shared > 1 else 0
    score += device_contrib
    if device_shared > 1:
        factors.append(f"Device shared with {device_shared - 1} other account(s)")

    geo_contrib = min((feat.distinct_cities - 1) * 0.02, 0.10)
    score += geo_contrib
    if feat.distinct_cities > 4:
        factors.append(f"Unusual geo spread: {feat.distinct_cities} cities")

    score = round(min(score, 1.0), 4)
    label = "HIGH" if score >= 0.6 else "MEDIUM" if score >= 0.3 else "LOW"

    return score, label, factors


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health():
    db_status = "ok"
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        db_status = f"error: {e}"
    return HealthResponse(status="ok", db=db_status, version=app.version)


@app.get("/risk/{account_id}", response_model=RiskResponse, tags=["risk"])
def get_risk(
    account_id: str,
    date: Optional[str] = Query(None, description="ISO date YYYY-MM-DD; defaults to latest"),
):
    t0 = time.perf_counter()

    if not account_id.startswith("ACC") or len(account_id) != 9:
        raise HTTPException(status_code=422, detail=f"Invalid account_id: '{account_id}'")

    try:
        with engine.connect() as conn:
            if date:
                row = conn.execute(
                    text("""
                        SELECT account_id, date, txn_count, total_amount, avg_amount,
                               max_txn_count_24h, spike_count, distinct_merchants, distinct_cities
                        FROM account_features
                        WHERE account_id = :acc AND date = :d
                    """),
                    {"acc": account_id, "d": date},
                ).fetchone()
            else:
                row = conn.execute(
                    text("""
                        SELECT account_id, date, txn_count, total_amount, avg_amount,
                               max_txn_count_24h, spike_count, distinct_merchants, distinct_cities
                        FROM account_features
                        WHERE account_id = :acc
                        ORDER BY date DESC
                        LIMIT 1
                    """),
                    {"acc": account_id},
                ).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"Account '{account_id}' not found")

            feat = AccountFeatures(
                account_id         = row.account_id,
                txn_count          = row.txn_count,
                total_amount       = float(row.total_amount),
                avg_amount         = float(row.avg_amount),
                max_txn_count_24h  = row.max_txn_count_24h,
                spike_count        = row.spike_count,
                distinct_merchants = row.distinct_merchants,
                distinct_cities    = row.distinct_cities,
                latest_date        = str(row.date),
            )

            dev_row = conn.execute(
                text("""
                    SELECT COALESCE(MAX(df.distinct_accounts), 1) AS shared
                    FROM transactions t
                    JOIN device_features df ON df.device_id = t.device_id AND df.date = :d
                    WHERE t.account_id = :acc
                """),
                {"acc": account_id, "d": feat.latest_date},
            ).fetchone()
            device_shared = int(dev_row.shared) if dev_row else 1

    except HTTPException:
        raise
    except OperationalError as e:
        logger.error("DB error: %s", e)
        raise HTTPException(status_code=503, detail="Database unavailable")

    score, label, factors = compute_risk_score(feat, device_shared)
    return RiskResponse(
        account_id    = account_id,
        risk_score    = score,
        risk_label    = label,
        features      = feat,
        score_factors = factors,
        latency_ms    = round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/device/{device_id}", response_model=DeviceResponse, tags=["device"])
def get_device(
    device_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    t0 = time.perf_counter()

    if not device_id.startswith("DEV") or len(device_id) != 9:
        raise HTTPException(status_code=422, detail=f"Invalid device_id: '{device_id}'")

    try:
        with engine.connect() as conn:
            dev_row = conn.execute(
                text("""
                    SELECT device_id,
                           SUM(distinct_accounts)  AS distinct_accounts,
                           MAX(multi_account_flag) AS multi_account_flag
                    FROM device_features
                    WHERE device_id = :did
                    GROUP BY device_id
                """),
                {"did": device_id},
            ).fetchone()

            if not dev_row:
                raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")

            rows = conn.execute(
                text("""
                    SELECT account_id, device_id,
                           COUNT(*)    AS txn_count,
                           SUM(amount) AS total_amount
                    FROM transactions
                    WHERE device_id = :did
                    GROUP BY account_id, device_id
                    ORDER BY txn_count DESC
                    LIMIT :lim
                """),
                {"did": device_id, "lim": limit},
            ).fetchall()

    except HTTPException:
        raise
    except OperationalError as e:
        logger.error("DB error: %s", e)
        raise HTTPException(status_code=503, detail="Database unavailable")

    return DeviceResponse(
        device_id          = device_id,
        distinct_accounts  = int(dev_row.distinct_accounts or 0),
        linked_accounts    = [
            LinkedAccount(
                account_id   = r.account_id,
                device_id    = r.device_id,
                txn_count    = int(r.txn_count),
                total_amount = float(r.total_amount),
            )
            for r in rows
        ],
        multi_account_flag = bool(dev_row.multi_account_flag),
        latency_ms         = round((time.perf_counter() - t0) * 1000, 2),
    )
