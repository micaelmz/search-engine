from sqlalchemy import Column, Text
from .base import Base

class PageLink(Base):
    __tablename__ = "page_links"

    source_url = Column(Text, primary_key=True)
    target_url = Column(Text, primary_key=True)

    def __repr__(self):
        return f"<PageLink {self.source_url} → {self.target_url}>"