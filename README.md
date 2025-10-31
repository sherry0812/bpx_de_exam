# Read Me

This project builds an ETL pipeline using FastAPI and SQLAlchemy, with three data layers:

1. **Bronze** – Raw uploaded files are stored as JSON.
2. **Silver** – Cleaned and structured data from Bronze.
3. **Gold** – Enriched data (optionally using AI for summaries).

## How to Run:
1. Install dependencies:

```pip install -r requirements.txt```

2. Run the app

```uvicorn main:app --reload```

3. Open in browser

```http://localhost:8000/docs```


## Workflow:
- Upload file → /upload
- Bronze → Silver → Gold happens automatically

## Check Data in DBeaver
You can explore the SQLite database visually using DBeaver:
1. Open DBeaver → Click Database → New Connection → SQLite
2. Browse and select your database file (e.g. uploads.db or whatever path you used)
3. Click Finish
4. You can now view the file_uploads, bronze_data, silver_data, and gold_data tables.
