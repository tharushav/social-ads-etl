import pandas as pd
import numpy as np
from sqlalchemy import insert, text
from datetime import datetime
import logging
import sys
import os

# Handle both relative and absolute imports
try:
    # Try relative imports first (when used as module)
    from .config import RAW_CSV
    from .db import engine, SessionLocal, create_tables
    from .models import metadata, social_ads
except ImportError:
    # Fall back to absolute imports (when run as script)
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.config import RAW_CSV
    from src.db import engine, SessionLocal, create_tables
    from src.models import metadata, social_ads

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SocialAdsETL:
    """ETL Pipeline for Social Ads Data"""
    
    def __init__(self):
        self.df_raw = None
        self.df_transformed = None
        
    def extract(self) -> pd.DataFrame:
        """Extract data from CSV file"""
        try:
            logger.info(f"Extracting data from {RAW_CSV}")
            self.df_raw = pd.read_csv(RAW_CSV)
            logger.info(f"Extracted {len(self.df_raw)} records")
            return self.df_raw
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and enrich the data"""
        try:
            logger.info("Starting data transformation")
            df_clean = df.copy()
            
            # Clean column names (lowercase, snake_case)
            df_clean.columns = df_clean.columns.str.lower()
            df_clean = df_clean.rename(columns={'estimatedsalary': 'estimated_salary'})
            
            # Data validation and cleaning
            df_clean = self._validate_data(df_clean)
            
            # Feature engineering
            df_clean = self._add_derived_features(df_clean)
            
            # Convert purchased to boolean
            df_clean['purchased'] = df_clean['purchased'].astype(bool)
            
            logger.info(f"Transformation completed. Final dataset: {len(df_clean)} records")
            self.df_transformed = df_clean
            return df_clean
            
        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean data"""
        initial_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Remove records with missing values
        df = df.dropna()
        
        # Validate age range (18-100)
        df = df[(df['age'] >= 18) & (df['age'] <= 100)]
        
        # Validate salary range (positive values)
        df = df[df['estimated_salary'] > 0]
        
        # Validate purchased values (0 or 1)
        df = df[df['purchased'].isin([0, 1])]
        
        final_count = len(df)
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} invalid records during validation")
        
        return df
    
    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features for better analysis"""
        
        # Age groups
        def categorize_age(age):
            if age < 25:
                return "Young (18-24)"
            elif age < 35:
                return "Adult (25-34)"
            elif age < 45:
                return "Middle Age (35-44)"
            else:
                return "Senior (45+)"
        
        df['age_group'] = df['age'].apply(categorize_age)
        
        # Salary brackets
        def categorize_salary(salary):
            if salary < 30000:
                return "Low (<30K)"
            elif salary < 60000:
                return "Medium (30K-60K)"
            elif salary < 100000:
                return "High (60K-100K)"
            else:
                return "Very High (100K+)"
        
        df['salary_bracket'] = df['estimated_salary'].apply(categorize_salary)
        
        return df
    
    def load(self, df: pd.DataFrame):
        """Load transformed data into database"""
        try:
            logger.info("Loading data into database")
            
            # Create tables if they don't exist
            create_tables()
            
            # Insert new data (no need to clear if creating fresh)
            session = SessionLocal()
            try:
                records = df.to_dict(orient='records')
                stmt = insert(social_ads).values(records)
                session.execute(stmt)
                session.commit()
                logger.info(f"Successfully loaded {len(records)} records")
            except Exception as e:
                session.rollback()
                logger.error(f"Error loading data: {e}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error in load phase: {e}")
            raise
    
    def run_etl(self):
        """Run the complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline")
            
            # Extract
            df_raw = self.extract()
            
            # Transform
            df_clean = self.transform(df_raw)
            
            # Load
            self.load(df_clean)
            
            logger.info("ETL pipeline completed successfully")
            self._print_summary()
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def _print_summary(self):
        """Print ETL summary"""
        if self.df_raw is not None and self.df_transformed is not None:
            print("\n" + "="*50)
            print("ETL PIPELINE SUMMARY")
            print("="*50)
            print(f"Records extracted: {len(self.df_raw)}")
            print(f"Records loaded: {len(self.df_transformed)}")
            print(f"Data quality: {len(self.df_transformed)/len(self.df_raw)*100:.1f}%")
            print("="*50)

def run_etl():
    """Convenience function to run ETL"""
    etl = SocialAdsETL()
    etl.run_etl()

if __name__ == '__main__':
    run_etl()