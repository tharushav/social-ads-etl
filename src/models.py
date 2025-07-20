from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, DateTime, String
from datetime import datetime, timezone

# Create a metadata instance to hold table definitions
metadata = MetaData()

# Define the 'social_ads' table schema
social_ads = Table(
    'social_ads', metadata, # Table name
    Column('id', Integer, primary_key=True, autoincrement=True), # Primary key column with auto-incremented integer
    Column('age', Integer, nullable=False),
    Column('estimated_salary', Float, nullable=False),
    Column('purchased', Boolean, nullable=False),
    Column('age_group', String(20), nullable=True),
    Column('salary_bracket', String(20), nullable=True),
    Column('created_at', DateTime, default=lambda: datetime.now(timezone.utc)),
    Column('updated_at', DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
)