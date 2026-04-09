from sqlalchemy import Column, Integer, Text, Boolean, DateTime, func
from .base import Base

class CrawlerQueue(Base):
    __tablename__ = "crawler_queue"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, nullable=False)
    domain = Column(Text, nullable=False)
    priority = Column(Integer, default=0)
    depth = Column(Integer, default=0)
    status = Column(Text, default="pending")
    attempts = Column(Integer, default=0)
    needs_js = Column(Boolean, default=False)
    queued_at = Column(DateTime, server_default=func.now())
    last_attempt_at = Column(DateTime)

    def __repr__(self):
        return f"<Queue url={self.url} priority={self.priority} depth={self.depth}>"