from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
import os

from database import Base, engine, SessionLocal
from models import file_upload
import services.bronze as bronze_service
import services.silver as silver_service
import services.gold as gold_service

app = FastAPI(title="File Upload & ETL Pipeline")
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

upload_dir = "uploaded_files"
os.makedirs(upload_dir, exist_ok=True)

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    filename = file.filename
    file_ext = os.path.splitext(filename)[1].lower()
    if file_ext not in [".csv", ".xlsx"]:
        raise HTTPException(status_code=400, detail="Only CSV or Excel files allowed")

    file_path = os.path.join(upload_dir, filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    file_size_kb = round(os.path.getsize(file_path) / 1024, 2)

    # Store upload metadata
    metadata = file_upload(filename=filename, filetype=file_ext, filesize_kb=file_size_kb)
    db.add(metadata)
    db.commit()
    db.refresh(metadata)

    # Bronze Ingestion
    try:
        result = bronze_service.ingest_bronze(file_path, metadata.id, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during bronze ingestion: {str(e)}")

    # Silver Transformation
    try:
        silver_service.transform_to_silver()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during silver transformation: {str(e)}")

    # Gold Enrichment
    try:
        gold_service.enrich_to_gold()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during gold enrichment: {str(e)}")

    return {
        "message": "File uploaded and processed successfully",
        "metadata": {
            "filename": filename,
            "rows_inserted_bronze": result["inserted"],
            "rows_skipped_duplicates_bronze": result["skipped"],
        },
    }
