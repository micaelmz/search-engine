from sqlalchemy import Column, Text, Boolean, Integer, DateTime
from .base import Base

class DomainRule(Base):
    __tablename__ = "domain_rules"

    domain = Column(Text, primary_key=True)
    blocked = Column(Boolean, default=False)
    crawl_delay_ms = Column(Integer, default=1000)
    last_crawled_at = Column(DateTime)
    robots_txt = Column(Text)

    def __repr__(self):
        return f"<DomainRule domain={self.domain} blocked={self.blocked}>"