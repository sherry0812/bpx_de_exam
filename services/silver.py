import json
from datetime import datetime
from models import bronze_data, silver_data
from database import SessionLocal

# Column mapping between JSON keys and Silver columns
COLUMN_MAP = {
    "unit_id": ["X_unit_id"],
    "source_created_at": ["X_created_at"],
    "source_id": ["X_id"],
    "started_at": ["X_started_at"],
    "tainted": ["X_tainted"],
    "channel": ["X_channel"],
    "trust": ["X_trust"],
    "worker_id": ["X_worker_id"],
    "country": ["X_country"],
    "region": ["X_region"],
    "city": ["X_city"],
    "ip_address": ["X_ip"],
    "appeal_to_reader": ["appeal_to_reader"],
    "conjunctions": ["conjunctions"],
    "connectivity": ["connectivity"],
    "narrative_perspective": ["narrative_perspective"],
    "sensory_language": ["sensory_language"],
    "setting": ["setting"],
    "ab": ["ab"],
    "appeal_to_reader_gold": ["appeal_to_reader_gold"],
    "conjunctions_gold": ["conjunctions_gold"],
    "connectivity_gold": ["connectivity_gold"],
    "narrative_perspective_gold": ["narrative_perspective_gold"],
    "pmid": ["pmid"],
    "py": ["py"],
    "sensory_language_gold": ["sensory_language_gold"],
    "setting_gold": ["setting_gold"],
    "so": ["so"],
    "tc": ["tc"],
    "cin_mas": ["cin_mas"],
    "first_author": ["firstauthor"],
    "number_authors": ["numberauthors"],
    "pid_mas": ["pid_mas"],
    "title": ["title"]
}


def get_value(data, keys):
    """
    Get the first non-empty value from possible JSON keys.
    """
    for key in keys:
        if key in data and data[key] not in [None, ""]:
            return data[key]
    return None


def parse_datetime(value):
    """
    Convert ISO or string timestamps into datetime objects if possible.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", ""))
    except Exception:
        return None


# Main transformation function
def transform_to_silver():
    session = SessionLocal()
    raw_records = session.query(bronze_data).all()
    inserted_count = 0
    skipped_count = 0

    for raw_record in raw_records:
        data = json.loads(raw_record.raw_json)

        # Skip if already transformed
        if session.query(silver_data).filter_by(raw_id=raw_record.id).first():
            skipped_count += 1
            continue

        # Build the Silver record
        silver_record = silver_data(
            raw_id=raw_record.id,
            unit_id=get_value(data, COLUMN_MAP["unit_id"]),
            source_created_at=parse_datetime(get_value(data, COLUMN_MAP["source_created_at"])),
            source_id=get_value(data, COLUMN_MAP["source_id"]),
            started_at=parse_datetime(get_value(data, COLUMN_MAP["started_at"])),
            tainted=get_value(data, COLUMN_MAP["tainted"]),
            channel=get_value(data, COLUMN_MAP["channel"]),
            trust=get_value(data, COLUMN_MAP["trust"]),
            worker_id=get_value(data, COLUMN_MAP["worker_id"]),
            country=get_value(data, COLUMN_MAP["country"]),
            region=get_value(data, COLUMN_MAP["region"]),
            city=get_value(data, COLUMN_MAP["city"]),
            ip_address=get_value(data, COLUMN_MAP["ip_address"]),
            appeal_to_reader=get_value(data, COLUMN_MAP["appeal_to_reader"]),
            conjunctions=get_value(data, COLUMN_MAP["conjunctions"]),
            connectivity=get_value(data, COLUMN_MAP["connectivity"]),
            narrative_perspective=get_value(data, COLUMN_MAP["narrative_perspective"]),
            sensory_language=get_value(data, COLUMN_MAP["sensory_language"]),
            setting=get_value(data, COLUMN_MAP["setting"]),
            ab=get_value(data, COLUMN_MAP["ab"]),
            appeal_to_reader_gold=get_value(data, COLUMN_MAP["appeal_to_reader_gold"]),
            conjunctions_gold=get_value(data, COLUMN_MAP["conjunctions_gold"]),
            connectivity_gold=get_value(data, COLUMN_MAP["connectivity_gold"]),
            narrative_perspective_gold=get_value(data, COLUMN_MAP["narrative_perspective_gold"]),
            pmid=get_value(data, COLUMN_MAP["pmid"]),
            py=int(get_value(data, COLUMN_MAP["py"])) if get_value(data, COLUMN_MAP["py"]) else None,
            sensory_language_gold=get_value(data, COLUMN_MAP["sensory_language_gold"]),
            setting_gold=get_value(data, COLUMN_MAP["setting_gold"]),
            so=get_value(data, COLUMN_MAP["so"]),
            tc=get_value(data, COLUMN_MAP["tc"]),
            cin_mas=get_value(data, COLUMN_MAP["cin_mas"]),
            first_author=get_value(data, COLUMN_MAP["first_author"]),
            number_authors=get_value(data, COLUMN_MAP["number_authors"]),
            pid_mas=get_value(data, COLUMN_MAP["pid_mas"]),
            title=get_value(data, COLUMN_MAP["title"])
        )

        session.add(silver_record)
        inserted_count += 1

    session.commit()
    session.close()

    print(f"Silver layer updated successfully. Inserted: {inserted_count}, Skipped: {skipped_count}")
    return {"inserted": inserted_count, "skipped": skipped_count}


if __name__ == "__main__":
    from database import Base, engine
    Base.metadata.create_all(bind=engine)
    transform_to_silver()
