#!/usr/bin/env python3
"""
Grocery Scraper - All stores in one clean file.
No complex frameworks, just straightforward code.
"""

import asyncio
import json
import re
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from playwright.async_api import async_playwright, Page, Route

# Import configuration
from config import DATABASE_PATH, STORES, SCRAPING, BLOCKED_RESOURCES, BROWSER_ARGS


# ===== DATABASE =====
class Database:
    """Simple database handler."""
    
    def __init__(self, path: str = DATABASE_PATH):
        self.path = path
        self.init_db()
    
    def init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT,
                subcategory TEXT,
                store TEXT NOT NULL,
                url TEXT,
                scraped_at TIMESTAMP,
                UNIQUE(name, store, url)
            )
        ''')
        conn.commit()
        conn.close()
    
    def save_products(self, products: List[Dict], store: str):
        """Save products to database."""
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        
        saved = 0
        for product in products:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO products 
                    (name, price, category, subcategory, store, url, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product['name'],
                    product['price'],
                    product.get('category', 'Інше'),
                    product.get('subcategory'),
                    store,
                    product.get('url'),
                    datetime.now().isoformat()
                ))
                saved += 1
            except Exception as e:
                print(f"Error saving product: {e}")
        
        conn.commit()
        conn.close()
        return saved


# ===== BASE SCRAPER =====
class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, db: Database):
        self.db = db
        self.store_name = ""
        self.base_url = ""
    
    @abstractmethod
    async def scrape(self, category: Optional[str] = None):
        """Main scraping method."""
        pass
    
    def clean_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text."""
        if not price_text:
            return None
        clean = re.sub(r'[^\d,.]', '', price_text)
        clean = clean.replace(',', '.')
        try:
            return float(clean)
        except:
            return None
    
    def clean_text(self, text: str) -> str:
        """Clean text from extra spaces and newlines."""
        if not text:
            return ""
        return ' '.join(text.strip().split())


# ===== PLAYWRIGHT-BASED SCRAPER =====
class PlaywrightScraper(BaseScraper):
    """Base for scrapers that need JavaScript rendering."""
    
    async def block_resources(self, route: Route):
        """Block unnecessary resources."""
        resource_type = route.request.resource_type
        url = route.request.url
        
        # Block images, media, fonts, and tracking
        if resource_type in ['image', 'media', 'font', 'stylesheet']:
            await route.abort()
            return
            
        # Block based on URL patterns from config
        import re
        for pattern in BLOCKED_RESOURCES:
            if re.search(pattern, url, re.I):
                await route.abort()
                return
                
        await route.continue_()
    
    async def setup_page(self, page: Page):
        """Set up page with optimizations."""
        await page.route('**/*', self.block_resources)
        await page.set_viewport_size({"width": 1920, "height": 1080})


# ===== VARUS SCRAPER =====
class VarusScraper(PlaywrightScraper):
    """Scraper for Varus store."""
    
    def __init__(self, db: Database):
        super().__init__(db)
        self.store_name = "Varus"
        self.base_url = "https://varus.ua"
    
    async def get_categories(self, page: Page) -> List[Dict[str, str]]:
        """Get category links."""
        await page.goto(self.base_url, wait_until='domcontentloaded')
        await page.wait_for_selector('.a-megamenu-item')
        
        return await page.evaluate('''() => {
            const items = document.querySelectorAll('.a-megamenu-item--main a');
            return Array.from(items).map(item => ({
                name: item.textContent.trim(),
                url: item.href
            }));
        }''')
    
    async def scrape_page(self, page: Page) -> List[Dict]:
        """Scrape products from current page."""
        try:
            await page.wait_for_selector('.sf-product-card', timeout=10000)
        except:
            return []
        
        products = await page.evaluate('''() => {
            const cards = document.querySelectorAll('.sf-product-card');
            return Array.from(cards).map(card => ({
                name: card.querySelector('.sf-product-card__title')?.textContent?.trim(),
                price: card.querySelector('.sf-price__regular, .sf-price__special')?.textContent?.trim(),
                url: card.querySelector('a')?.href
            })).filter(p => p.name && p.price);
        }''')
        
        # Clean prices
        for product in products:
            product['price'] = self.clean_price(product['price'])
        
        return [p for p in products if p['price']]
    
    async def scrape(self, category: Optional[str] = None):
        """Scrape Varus store."""
        print(f"🛒 Scraping {self.store_name}...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=SCRAPING['headless'],
                args=BROWSER_ARGS
            )
            page = await browser.new_page()
            await self.setup_page(page)
            
            categories = await self.get_categories(page)
            if category:
                categories = [c for c in categories if category.lower() in c['name'].lower()]
            
            for cat in categories:
                print(f"📁 Category: {cat['name']}")
                await page.goto(cat['url'], wait_until='domcontentloaded')
                
                products = await self.scrape_page(page)
                for product in products:
                    product['category'] = cat['name']
                
                saved = self.db.save_products(products, self.store_name)
                print(f"✅ Saved {saved} products")
                
                await asyncio.sleep(SCRAPING['wait_between_requests'])
            
            await browser.close()


# ===== SILPO SCRAPER =====
class SilpoScraper(PlaywrightScraper):
    """Scraper for Silpo store."""
    
    def __init__(self, db: Database):
        super().__init__(db)
        self.store_name = "Silpo"
        self.base_url = "https://silpo.ua"
    
    async def scrape_page(self, page: Page) -> List[Dict]:
        """Scrape products from current page."""
        try:
            await page.wait_for_selector('[class*="product-card"]', timeout=10000)
        except:
            return []
        
        products = await page.evaluate('''() => {
            const cards = document.querySelectorAll('[class*="product-card"]');
            return Array.from(cards).map(card => ({
                name: card.querySelector('[class*="product-title"], [class*="product-name"]')?.textContent?.trim(),
                price: card.querySelector('[class*="price"]')?.textContent?.trim(),
                url: card.querySelector('a')?.href
            })).filter(p => p.name && p.price);
        }''')
        
        for product in products:
            product['price'] = self.clean_price(product['price'])
        
        return [p for p in products if p['price']]
    
    async def scrape(self, category: Optional[str] = None):
        """Scrape Silpo store."""
        print(f"🛒 Scraping {self.store_name}...")
        
        # Main categories to scrape
        categories = [
            {"name": "Бакалія", "url": f"{self.base_url}/categories/bakaliya"},
            {"name": "М'ясо та риба", "url": f"{self.base_url}/categories/myaso-ryba"},
            {"name": "Молочні продукти", "url": f"{self.base_url}/categories/molochni"},
            {"name": "Овочі та фрукти", "url": f"{self.base_url}/categories/ovochi-frukty"},
            {"name": "Напої", "url": f"{self.base_url}/categories/napoi"},
        ]
        
        if category:
            categories = [c for c in categories if category.lower() in c['name'].lower()]
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=SCRAPING['headless'],
                args=BROWSER_ARGS
            )
            page = await browser.new_page()
            await self.setup_page(page)
            
            for cat in categories:
                print(f"📁 Category: {cat['name']}")
                await page.goto(cat['url'], wait_until='domcontentloaded')
                
                products = await self.scrape_page(page)
                for product in products:
                    product['category'] = cat['name']
                
                saved = self.db.save_products(products, self.store_name)
                print(f"✅ Saved {saved} products")
                
                await asyncio.sleep(SCRAPING['wait_between_requests'])
            
            await browser.close()


# ===== ATB SCRAPER (using requests) =====
class ATBScraper(BaseScraper):
    """Scraper for ATB store using direct API calls."""
    
    def __init__(self, db: Database):
        super().__init__(db)
        self.store_name = "ATB"
        self.base_url = "https://www.atbmarket.com"
        self.api_url = "https://www.atbmarket.com/api/v1/products"
    
    def get_categories(self) -> List[Dict[str, str]]:
        """Get ATB categories."""
        return [
            {"name": "Бакалія", "id": "bakaleya"},
            {"name": "М'ясо", "id": "myaso"},
            {"name": "Молочні продукти", "id": "moloko"},
            {"name": "Овочі та фрукти", "id": "ovochi"},
            {"name": "Напої", "id": "napoi"},
            {"name": "Алкоголь", "id": "alkogol"},
        ]
    
    def scrape_category(self, category: Dict[str, str]) -> List[Dict]:
        """Scrape products from category."""
        products = []
        page = 1
        
        while True:
            try:
                response = requests.get(
                    self.api_url,
                    params={"category": category['id'], "page": page},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30
                )
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                if not data.get('products'):
                    break
                
                for item in data['products']:
                    products.append({
                        'name': self.clean_text(item.get('name', '')),
                        'price': float(item.get('price', 0)),
                        'category': category['name'],
                        'url': f"{self.base_url}/product/{item.get('id')}"
                    })
                
                page += 1
                
            except Exception as e:
                print(f"Error: {e}")
                break
        
        return products
    
    async def scrape(self, category: Optional[str] = None):
        """Scrape ATB store."""
        print(f"🛒 Scraping {self.store_name}...")
        
        categories = self.get_categories()
        if category:
            categories = [c for c in categories if category.lower() in c['name'].lower()]
        
        for cat in categories:
            print(f"📁 Category: {cat['name']}")
            products = self.scrape_category(cat)
            saved = self.db.save_products(products, self.store_name)
            print(f"✅ Saved {saved} products")


# ===== MAIN SCRAPER MANAGER =====
class GroceryScraper:
    """Main scraper that manages all store scrapers."""
    
    def __init__(self):
        self.db = Database()
        self.scrapers = {
            'varus': VarusScraper(self.db),
            'silpo': SilpoScraper(self.db),
            'atb': ATBScraper(self.db),
        }
    
    async def scrape_store(self, store: str, category: Optional[str] = None):
        """Scrape specific store."""
        if store not in self.scrapers:
            print(f"❌ Unknown store: {store}")
            print(f"Available stores: {', '.join(self.scrapers.keys())}")
            return
        
        await self.scrapers[store].scrape(category)
    
    async def scrape_all(self, category: Optional[str] = None):
        """Scrape all stores."""
        for store_name, scraper in self.scrapers.items():
            print(f"\n{'='*50}")
            print(f"Starting {store_name.upper()}")
            print('='*50)
            await scraper.scrape(category)
            print(f"✅ {store_name.upper()} complete!")


# ===== COMMAND LINE INTERFACE =====
async def main():
    """Main entry point."""
    import sys
    
    scraper = GroceryScraper()
    
    # Parse command line arguments
    args = sys.argv[1:]
    
    if not args:
        print("Usage:")
        print("  python scraper.py all                    # Scrape all stores")
        print("  python scraper.py varus                  # Scrape only Varus")
        print("  python scraper.py silpo 'Молочні'        # Scrape Silpo dairy")
        print("  python scraper.py all 'М\\'ясо'           # Scrape meat from all stores")
        return
    
    store = args[0].lower()
    category = args[1] if len(args) > 1 else None
    
    if store == 'all':
        await scraper.scrape_all(category)
    else:
        await scraper.scrape_store(store, category)


if __name__ == "__main__":
    asyncio.run(main())