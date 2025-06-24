from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, DateTime, String
from datetime import datetime

metadata = MetaData()

social_ads = Table(
    'social_ads', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('age', Integer, nullable=False),
    Column('estimated_salary', Float, nullable=False),
    Column('purchased', Boolean, nullable=False),
    Column('age_group', String(20), nullable=True),
    Column('salary_bracket', String(20), nullable=True),
    Column('created_at', DateTime, default=datetime.utcnow),
    Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)