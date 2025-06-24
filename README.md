# Social Ads ETL Pipeline

ETL pipeline for analyzing social media advertising data with automated data processing and insights generation.

## 📊 Dataset

400 customer records with:
- **Age**: 18-60 years
- **EstimatedSalary**: $15K-$150K  
- **Purchased**: 0/1 (purchase after seeing ad)

## 🚀 Quick Start

### 1. Setup
```bash
# Navigate to project
cd social-ads-etl

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Analysis (Recommended)
Execute notebooks in order:
```bash
jupyter lab
```

1. **`01_data_exploration.ipynb`** - Explore raw data
2. **`02_etl_pipeline.ipynb`** - Process data (Extract → Transform → Load)  
3. **`03_analysis.ipynb`** - Generate insights and visualizations

### 3. Run ETL Programmatically
```bash
# Direct ETL execution
python run_etl.py

# Or via module
python -c "from src.etl import run_etl; run_etl()"
```

### 4. Verify Setup
```bash
# Run tests
python -m pytest tests/ -v
```

## � Project Structure
```
social-ads-etl/
├── data/
│   ├── raw/social_ads.csv           # Original dataset
│   └── processed/social_ads.db      # SQLite output
├── notebooks/                       # Analysis workflow
├── src/                            # ETL modules  
├── tests/                          # Unit tests
└── requirements.txt
```

## What It Does

**Extract**: Load CSV data  
**Transform**: Clean data, add age groups & salary brackets  
**Load**: Store in SQLite database  
**Analyze**: Generate business insights

## Key Results

- **Data Quality**: 91.8% (367/400 records pass validation)
- **Best Segment**: Senior customers (45+) with 84.9% conversion
- **Overall Conversion**: ~36% purchase rate

Name - Tharusha Vihanga