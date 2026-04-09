from sqlalchemy import Column, Integer, Text, DateTime, func
from .base import Base

class CrawlerSeed(Base):
    __tablename__ = "crawler_seeds"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, nullable=False)
    priority = Column(Integer, default=0)
    label = Column(Text)
    added_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<Seed url={self.url} label={self.label}>"