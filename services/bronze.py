import pandas as pd
import json
import hashlib
from models import bronze_data
from database import SessionLocal

def sanitize_record(record_dict):
    """
    Convert non-JSON-serializable objects (timestamps, bytes, etc.) to strings.
    """
    clean_dict = {}
    for k, v in record_dict.items():
        if hasattr(v, "isoformat"):  # datetime or pandas Timestamp
            clean_dict[k] = v.isoformat()
        elif isinstance(v, (pd.Timestamp, pd.Timedelta)):
            clean_dict[k] = str(v)
        elif isinstance(v, bytes):
            clean_dict[k] = v.decode("utf-8", errors="ignore")
        else:
            clean_dict[k] = v
    return clean_dict


def ingest_bronze(file_path: str, upload_id: int, db=None):
    """
    Ingests a CSV or Excel file into the Bronze layer as raw JSON records with deduplication.
    """
    session = db or SessionLocal()

    # Read the file safely
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")

    df = df.where(pd.notnull(df), None)

    inserted, skipped = 0, 0

    for _, row in df.iterrows():
        record_dict = sanitize_record(row.to_dict())
        record_hash = hashlib.sha256(json.dumps(record_dict, sort_keys=True).encode()).hexdigest()

        # Check for duplicates
        if session.query(bronze_data).filter_by(record_hash=record_hash).first():
            skipped += 1
            continue

        bronze_record = bronze_data(
            upload_id=upload_id,
            raw_json=json.dumps(record_dict, ensure_ascii=False),
            record_hash=record_hash,
        )
        session.add(bronze_record)
        inserted += 1

    session.commit()
    session.close()

    return {"inserted": inserted, "skipped": skipped}
