import pytest
import pandas as pd
import sys
import os

# Add src to path - fix the path resolution
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from etl import SocialAdsETL
from config import RAW_CSV

class TestSocialAdsETL:
    """Test cases for Social Ads ETL pipeline"""
    
    def setup_method(self):
        """Setup for each test"""
        self.etl = SocialAdsETL()
    
    def test_extract(self):
        """Test data extraction"""
        df = self.etl.extract()
        
        # Check if data is loaded
        assert df is not None
        assert len(df) > 0
        
        # Check expected columns
        expected_columns = ['Age', 'EstimatedSalary', 'Purchased']
        assert all(col in df.columns for col in expected_columns)
        
        # Check data types
        assert df['Age'].dtype in ['int64', 'int32']
        assert df['EstimatedSalary'].dtype in ['int64', 'int32', 'float64']
        assert df['Purchased'].dtype in ['int64', 'int32', 'bool']
    
    def test_data_validation(self):
        """Test data validation logic"""
        # Create test data with invalid records
        test_data = pd.DataFrame({
            'age': [25, 150, 15, 30, 25],  # 150 and 15 are invalid
            'estimated_salary': [50000, 60000, -1000, 70000, 50000],  # -1000 is invalid
            'purchased': [1, 0, 1, 2, 0]  # 2 is invalid
        })
        
        cleaned_data = self.etl._validate_data(test_data)
        
        # Should only have 2 valid records (25, 50000, 1) and (30, 70000, 0 becomes False)
        assert len(cleaned_data) == 2
        assert all(cleaned_data['age'] >= 18)
        assert all(cleaned_data['age'] <= 100)
        assert all(cleaned_data['estimated_salary'] > 0)
        assert all(cleaned_data['purchased'].isin([0, 1]))
    
    def test_feature_engineering(self):
        """Test derived feature creation"""
        test_data = pd.DataFrame({
            'age': [22, 30, 40, 50],
            'estimated_salary': [25000, 45000, 75000, 120000],
            'purchased': [0, 1, 1, 0]
        })
        
        enriched_data = self.etl._add_derived_features(test_data)
        
        # Check if new columns are created
        assert 'age_group' in enriched_data.columns
        assert 'salary_bracket' in enriched_data.columns
        
        # Check age group categorization
        assert enriched_data.iloc[0]['age_group'] == "Young (18-24)"
        assert enriched_data.iloc[1]['age_group'] == "Adult (25-34)"
        assert enriched_data.iloc[2]['age_group'] == "Middle Age (35-44)"
        assert enriched_data.iloc[3]['age_group'] == "Senior (45+)"
        
        # Check salary bracket categorization
        assert enriched_data.iloc[0]['salary_bracket'] == "Low (<30K)"
        assert enriched_data.iloc[1]['salary_bracket'] == "Medium (30K-60K)"
        assert enriched_data.iloc[2]['salary_bracket'] == "High (60K-100K)"
        assert enriched_data.iloc[3]['salary_bracket'] == "Very High (100K+)"
    
    def test_transform_pipeline(self):
        """Test complete transformation pipeline"""
        # Extract real data
        df_raw = self.etl.extract()
        
        # Transform
        df_transformed = self.etl.transform(df_raw)
        
        # Check basic properties
        assert df_transformed is not None
        assert len(df_transformed) > 0
        
        # Check column names are lowercase and snake_case
        expected_columns = ['age', 'estimated_salary', 'purchased', 'age_group', 'salary_bracket']
        assert all(col in df_transformed.columns for col in expected_columns)
        
        # Check boolean conversion
        assert df_transformed['purchased'].dtype == bool
        
        # Check derived features exist
        assert df_transformed['age_group'].notna().all()
        assert df_transformed['salary_bracket'].notna().all()

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
