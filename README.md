# DS Search EDA

A data science project for search exploratory data analysis.

## Project Structure

```
ds-search-eda/
├── scripts/           # Python scripts for data processing & analysis
├── sql/
│   └── queries/       # SQL query files
├── notebooks/         # Jupyter notebooks for exploration
├── data/
│   ├── raw/           # Raw data (not committed)
│   └── processed/     # Processed data (not committed)
├── config/            # Configuration files
├── tests/             # Unit tests
└── requirements.txt   # Python dependencies
```

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

- Place SQL queries in `sql/queries/`
- Python scripts go in `scripts/`
- Use `notebooks/` for exploratory analysis
