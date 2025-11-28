# Setup Instructions

## Local Development

1. Clone repository
2. Create virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Run: `python run.py`

## GitHub Actions

The workflow automatically:
- Runs daily at 4:20 PM IST
- Analyzes latest NSE data
- Generates opportunity reports
- Commits results back to repository

## Customization

Modify these files for customization:
- `config/settings.py` - Strategy parameters
- `src/strategy_engine.py` - Add new strategies
- `src/report_generator.py` - Customize reports
