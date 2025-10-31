import json
from openai import OpenAI
from models import silver_data, gold_data
from database import SessionLocal

client = OpenAI(api_key="api_key")

def enrich_to_gold():
    session = SessionLocal()
    silver_records = session.query(silver_data).all()

    for s in silver_records:
        # Skip if already enriched
        if session.query(gold_data).filter_by(silver_id=s.id).first():
            continue

        prompt = f"Summarize and extract key insights from this research abstract:\n\n{s.ab}"
        # Mock enrichment (no API key needed)
        enrichment = {
        "summary": f"Mock summary for record ID {s.id}",
        "insight": f"Generated placeholder enrichment for '{s.title}'"
        }

        # response = client.chat.completions.create(
        #     model="gpt-4o-mini",
        #     messages=[{"role": "user", "content": prompt}]
        # )
        # enrichment = {"summary": response.choices[0].message.content}
        # session.add(gold_data(silver_id=s.id, enrichment_json=json.dumps(enrichment)))

    session.commit()
    session.close()
    print("Gold layer enrichment completed.")

if __name__ == "__main__":
    from database import Base, engine
    Base.metadata.create_all(bind=engine)
    enrich_to_gold()

