"""Stage 0: Generate mock interest rate records and write to data/raw/interest_rates.json."""
import json
import uuid
import pathlib
import random
from datetime import datetime, timedelta, timezone
from pipeline.lineage_emitter import LineageEmitter

RATE_TYPES = ["SOFR", "LIBOR", "FED_FUNDS_RATE"]
TENORS = ["overnight", "1M", "3M", "6M"]
CURRENCY = "USD"
OUT_PATH = pathlib.Path(__file__).parent.parent.parent / "data" / "raw" / "interest_rates.json"

# Intentionally inject ~10% bad records for validation testing
_BAD_RATE_PROBABILITY = 0.10


def _random_date(start: datetime, days: int) -> str:
    delta = timedelta(days=random.randint(0, days - 1))
    return (start + delta).strftime("%Y-%m-%d")


def run(emitter: LineageEmitter, count: int = 50, seed: int = 42) -> list[dict]:
    random.seed(seed)
    base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    records = []

    for _ in range(count):
        record_id = str(uuid.uuid4())
        rate_type = random.choice(RATE_TYPES)
        tenor = random.choice(TENORS)

        # ~10% chance of out-of-range value to trigger validation failures
        if random.random() < _BAD_RATE_PROBABILITY:
            value = round(random.uniform(0.31, 0.50), 6)  # invalid: > 0.30
        else:
            value = round(random.uniform(0.001, 0.10), 6)

        record = {
            "record_id": record_id,
            "source": "MockFedProvider",
            "rate_type": rate_type,
            "value": value,
            "currency": CURRENCY,
            "tenor": tenor,
            "effective_date": _random_date(base_date, 30),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

        emitter.emit(
            operation="PRODUCE",
            data_type="RAW",
            label=f"RAW:{rate_type}:{tenor}",
            record_id=record_id,
            attributes={"rate_type": rate_type, "tenor": tenor, "value": value},
        )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"[provider] Generated {len(records)} records → {OUT_PATH}")
    return records
