# Grocery Scraper

A simple, lightweight grocery price scraper for Ukrainian stores.

## Features

- Scrapes prices from 4 major Ukrainian grocery stores:
  - Varus
  - Silpo
  - ATB
  - Metro
- Simple one-file architecture
- Easy to understand and modify
- Uses Playwright for JavaScript-heavy sites
- Direct API calls for ATB

## Project Structure

```
grocery_scraper/
├── config.py          # Simple configuration file
├── scraper.py         # Main scraper (all logic in one file)
├── requirements.txt   # Python dependencies
└── README.md          # This file
```

That's it! No complex frameworks, no scattered files.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

## Configuration

Edit `config.py` to:
- Change database path
- Add/remove stores
- Adjust timeouts and delays
- Modify resource blocking patterns

## Usage

### Simple Scraper (scraper.py)
```bash
# Show help
python scraper.py

# Scrape all stores
python scraper.py all

# Scrape specific store
python scraper.py varus
python scraper.py silpo
python scraper.py atb
python scraper.py metro

# Scrape specific category
python scraper.py varus "Молочні"
python scraper.py all "М'ясо"
```

### Scrapy Spiders (Advanced)
```bash
# List available spiders
scrapy list

# Scrape all categories from main page
scrapy crawl varus
scrapy crawl silpo
scrapy crawl metro

# Scrape specific category (looks up URL in database)
scrapy crawl varus -a category_name="Сири"
scrapy crawl varus -a category_name="Алкоголь"
scrapy crawl varus -a category_name="Бакалія"

# Scrape specific URL directly
scrapy crawl varus -a start_url="https://varus.ua/siri" -a category_name="Сири"
scrapy crawl varus -a start_url="https://varus.ua/bakaliya"

# Combine with Scrapy settings
scrapy crawl varus -a category_name="Сири" -s DOWNLOAD_DELAY=2
scrapy crawl varus -a category_name="Сири" -s CONCURRENT_REQUESTS=1
scrapy crawl varus -a category_name="Сири" -s CLOSESPIDER_PAGECOUNT=5

# Save results to file
scrapy crawl varus -a category_name="Сири" -o products.json
scrapy crawl varus -a category_name="Сири" -o products.csv
```

### Available Categories (Examples)
From database for Varus:
- `Алкоголь` - Alcoholic beverages
- `Бакалія` - Groceries 
- `Сири` - Cheeses
- `Власна випічка та десерти VARUS` - Bakery and desserts
- `Вода, соки, напої` - Water, juices, drinks
- `Для дітей` - For children
- `Для дому` - For home
- `М'ясо, птиця, риба` - Meat, poultry, fish
- `Молочні продукти та яйця` - Dairy and eggs

```bash
# List all available categories in database
sqlite3 /Users/dkovtunov/shared/grocery_data.db "SELECT DISTINCT category FROM categories WHERE store='Varus';"
```

## Database

SQLite database with simple schema:
- `products` table with name, price, category, store, URL, and timestamp

Database location: `/Users/dkovtunov/shared/grocery_data.db`

## How It Works

1. **Database Class**: Handles all database operations
2. **BaseScraper**: Common functionality for all scrapers  
3. **Store Scrapers**: One class per store (Varus, Silpo, ATB, Metro)
4. **GroceryScraper**: Main manager that coordinates everything

## Adding New Stores

1. Create a new scraper class inheriting from `PlaywrightScraper` or `BaseScraper`
2. Implement the `scrape()` method
3. Add store config to `config.py`
4. Register in `GroceryScraper.scrapers` dict

Example:
```python
class NewStoreScraper(PlaywrightScraper):
    def __init__(self, db: Database):
        super().__init__(db)
        self.store_name = "NewStore"
        self.base_url = "https://newstore.ua"
    
    async def scrape(self, category=None):
        # Your scraping logic here
        pass
```

## Performance Optimizations

- Blocks images, fonts, and tracking scripts
- Configurable timeouts and delays
- Resource blocking patterns in config
- Headless browser mode

## Notes

- The old Scrapy-based architecture files are kept in `grocery_scraper/` folder but are not used
- All active code is in `scraper.py` and `config.py`