from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData
from config import DB_SCHEMA

class Base(DeclarativeBase):
    metadata = MetaData(schema=DB_SCHEMA)