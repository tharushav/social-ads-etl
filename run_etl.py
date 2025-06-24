#!/usr/bin/env python3
"""
Simple script to run the ETL pipeline from the project root.
Usage: python run_etl.py
"""

import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Now import and run the ETL
from etl import run_etl

if __name__ == '__main__':
    print("🚀 Starting Social Ads ETL Pipeline...")
    print("=" * 50)
    run_etl()
    print("=" * 50)
    print("🎉 ETL Pipeline completed!")
