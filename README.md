# MAP Compliance Monitoring System

## Overview
This project monitors sellers' compliance with MAP policies by scraping data from Flipkart and Amazon and storing it in a SQL Server database.

## Requirements
- Python 3.x
- Required libraries (install using `pip install -r requirements.txt`)

## Setup
1. Set up your SQL Server database and create the necessary tables.
2. Update the connection details in `db_operations.py` and `map_monitor.py`.
3. Run the scrapers:
   - `python flipkart_scraper.py`
   - `python amazon_scraper.py`
4. Load data into the database:
   - `python db_operations.py`
5. Check MAP compliance:
   - `python map_monitor.py`
