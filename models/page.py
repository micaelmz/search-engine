from sqlalchemy import Column, Integer, Text, DateTime, func
from sqlalchemy.dialects.postgresql import TSVECTOR
from pgvector.sqlalchemy import Vector
from .base import Base

class Page(Base):
    __tablename__ = "pages"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, nullable=False)
    domain = Column(Text, nullable=False)
    title = Column(Text)
    summary = Column(Text)
    raw_text = Column(Text)
    embedding = Column(Vector(768))
    language = Column(Text)
    status = Column(Text, default="pending")
    search_vector = Column(TSVECTOR)
    indexed_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Page url={self.url} status={self.status}>"