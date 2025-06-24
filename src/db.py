from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .config import SQLITE_URL
from .models import metadata
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLite engine; disable thread check for notebooks
engine = create_engine(SQLITE_URL, connect_args={'check_same_thread': False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def create_tables():
    """Create all database tables"""
    try:
        metadata.create_all(engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()