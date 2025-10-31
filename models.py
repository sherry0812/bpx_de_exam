from sqlalchemy import (
    Column, Integer, String, Float, Text, DateTime, ForeignKey, Boolean
)
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base, engine, SessionLocal


# File Upload (Metadata)
class file_upload(Base):
    __tablename__ = "file_uploads"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    filetype = Column(String, nullable=False)
    filesize_kb = Column(Float)
    upload_time = Column(DateTime, default=datetime.utcnow)

    # Relationships
    raw_records = relationship("bronze_data", back_populates="upload")


    def __repr__(self):
        return f"<FileUpload(id={self.id}, filename='{self.filename}', type='{self.filetype}', size={self.filesize_kb}KB)>"


# Bronze - Raw Data
class bronze_data(Base):
    __tablename__ = "bronze_data"

    id = Column(Integer, primary_key=True, index=True)
    upload_id = Column(Integer, ForeignKey("file_uploads.id"))
    raw_json = Column(Text, nullable=False)
    record_hash = Column(String(64), index=True, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    upload = relationship("file_upload", back_populates="raw_records")
    silver_record = relationship("silver_data", back_populates="raw_record", uselist=False)

    def __repr__(self):
        return f"<BronzeData(id={self.id}, upload_id={self.upload_id}, hash='{self.record_hash[:8]}...', created_at={self.created_at:%Y-%m-%d})>"


# Silver - Normalized Data
class silver_data(Base):
    __tablename__ = "silver_data"

    id = Column(Integer, primary_key=True, index=True)
    raw_id = Column(Integer, ForeignKey("bronze_data.id"))
    
    # Standardized the column names, and deleted columns af to z9
    unit_id = Column(Integer)
    source_created_at = Column(DateTime)
    source_id = Column(Integer)
    started_at = Column(DateTime)
    tainted = Column(Boolean)
    channel = Column(String)
    trust = Column(Integer)
    worker_id = Column(Integer)
    country = Column(String)
    region = Column(String)
    city = Column(String)
    ip_address = Column(String(45))
    appeal_to_reader = Column(String)
    conjunctions = Column(Integer)
    connectivity = Column(Integer)
    narrative_perspective = Column(String)
    sensory_language = Column(Integer)
    setting = Column(String)
    ab = Column(Text)
    appeal_to_reader_gold = Column(String)
    conjunctions_gold = Column(String)
    connectivity_gold = Column(String)
    narrative_perspective_gold = Column(String)
    pmid = Column(Integer)
    py = Column(Integer)
    sensory_language_gold = Column(String)
    setting_gold = Column(String)
    so = Column(Text)
    tc = Column(String)
    cin_mas = Column(Integer)
    first_author = Column(String)
    number_authors = Column(Integer)
    pid_mas = Column(Integer)
    title = Column(Text)

    # Relationships
    raw_record = relationship("bronze_data", back_populates="silver_record")
    gold_record = relationship("gold_data", back_populates="silver_record", uselist=False)

    def __repr__(self):
        return f"<SilverData(id={self.id}, raw_id={self.raw_id}, title='{self.title[:30] if self.title else None}', year={self.py})>"

# Gold - Enriched Data
class gold_data(Base):
    __tablename__ = "gold_data"

    id = Column(Integer, primary_key=True, index=True)
    silver_id = Column(Integer, ForeignKey("silver_data.id"))
    enrichment_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    silver_record = relationship("silver_data", back_populates="gold_record")

    def __repr__(self):
        return f"<GoldData(id={self.id}, silver_id={self.silver_id}, created_at={self.created_at:%Y-%m-%d})>"

