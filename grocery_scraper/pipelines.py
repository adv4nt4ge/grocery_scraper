#!/usr/bin/env python3
"""
Enhanced Scrapy pipelines with improved error handling and data validation.
"""

import re
from datetime import datetime
from typing import Dict, Any, Set

from itemadapter import ItemAdapter
from scrapy import Spider
from scrapy.exceptions import DropItem

import sqlite3
import hashlib
import logging
from urllib.parse import urlparse

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DATABASE_PATH

logger = logging.getLogger(__name__)


# Simple utility functions
def clean_text(text):
    """Clean text from extra spaces and newlines."""
    if not text:
        return ""
    return ' '.join(text.strip().split())


def clean_price(price_text):
    """Extract numeric price from text."""
    if not price_text:
        return None
    clean = re.sub(r'[^\d,.]', '', str(price_text))
    clean = clean.replace(',', '.')
    try:
        return float(clean)
    except:
        return None


def generate_product_id(store, url):
    """Generate unique product ID."""
    text = f"{store}:{url}"
    return hashlib.md5(text.encode()).hexdigest()


def normalize_url(url):
    """Normalize URL."""
    if not url:
        return url
    return url.strip()


def validate_product_data(data):
    """Simple validation."""
    errors = []
    if not data.get('name'):
        errors.append("Missing name")
    if not data.get('price'):
        errors.append("Missing price")
    if not data.get('store'):
        errors.append("Missing store")
    return len(errors) == 0, errors


def calculate_discount_percentage(original, current):
    """Calculate discount percentage."""
    if not original or not current or original <= current:
        return 0
    return ((original - current) / original) * 100




# Simple database class
class SimpleDB:
    def __init__(self):
        self.db_path = DATABASE_PATH
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
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
        conn.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store TEXT NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT,
                category_url TEXT,
                UNIQUE(store, category, subcategory)
            )
        ''')
        conn.commit()
        conn.close()
    
    def insert_products_batch(self, products):
        conn = sqlite3.connect(self.db_path)
        saved = 0
        for product in products:
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO products 
                    (name, price, category, subcategory, store, url, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product.name, product.price, product.category,
                    product.subcategory, product.store, product.url,
                    product.scraped_at.isoformat() if product.scraped_at else None
                ))
                saved += 1
            except Exception as e:
                logger.error(f"Error saving product: {e}")
        conn.commit()
        conn.close()
        return saved
    
    def insert_category(self, store, category, subcategory=None, category_url=None):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR REPLACE INTO categories 
                (store, category, subcategory, category_url)
                VALUES (?, ?, ?, ?)
            ''', (store, category, subcategory, category_url))
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving category: {e}")
        finally:
            conn.close()


# Product class
class Product:
    def __init__(self, name, price, category, subcategory, store, url, 
                 image_url=None, brand=None, description=None, scraped_at=None):
        self.name = name
        self.price = price
        self.category = category
        self.subcategory = subcategory
        self.store = store
        self.url = url
        self.image_url = image_url
        self.brand = brand
        self.description = description
        self.scraped_at = scraped_at or datetime.now()

# Global database instance
db = SimpleDB()


class ValidationPipeline:
    """Pipeline for validating and cleaning scraped data."""
    
    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Process and validate item data."""
        adapter = ItemAdapter(item)
        
        try:
            # Validate required fields
            is_valid, errors = validate_product_data(dict(adapter))
            if not is_valid:
                spider.logger.warning(f"Invalid item data: {', '.join(errors)}")
                raise DropItem(f"Invalid item: {', '.join(errors)}")
            
            # Clean and normalize fields
            self._clean_item_fields(adapter, spider)
            
            # Generate product ID
            if not adapter.get('product_id'):
                adapter['product_id'] = generate_product_id(
                    adapter.get('store', ''),
                    adapter.get('url', '')
                )
            
            # Set scraped timestamp
            if not adapter.get('scraped_at'):
                adapter['scraped_at'] = datetime.now()
            
            return item
            
        except Exception as e:
            spider.logger.error(f"Error processing item: {e}")
            raise DropItem(f"Processing error: {str(e)}")
    
    def _clean_item_fields(self, adapter: ItemAdapter, spider: Spider) -> None:
        """Clean individual item fields."""
        # Clean text fields
        text_fields = ['name', 'category', 'subcategory', 'brand', 'description']
        for field in text_fields:
            if adapter.get(field):
                adapter[field] = clean_text(adapter[field])
        
        # Clean and validate price
        if adapter.get('price'):
            price = clean_price(adapter['price'])
            if price is None or price <= 0:
                spider.logger.warning(f"Invalid price {adapter['price']} for {adapter.get('name')}")
                raise ValueError(f"Invalid price: {adapter['price']}")
            adapter['price'] = price
        
        # Clean and validate original price
        if adapter.get('original_price'):
            original_price = clean_price(adapter['original_price'])
            if original_price and original_price > 0:
                adapter['original_price'] = original_price
                
                # Calculate discount percentage
                if adapter.get('price'):
                    discount_pct = calculate_discount_percentage(
                        original_price, adapter['price']
                    )
                    if discount_pct:
                        adapter['discount_percentage'] = discount_pct
            else:
                adapter['original_price'] = None
        
        # Clean URLs
        url_fields = ['url', 'image_url']
        for field in url_fields:
            if adapter.get(field):
                try:
                    adapter[field] = normalize_url(adapter[field])
                except Exception as e:
                    spider.logger.warning(f"Invalid URL in {field}: {adapter[field]}")
                    if field == 'url':  # URL is required
                        raise ValueError(f"Invalid URL: {adapter[field]}")
                    adapter[field] = None
        


class DeduplicationPipeline:
    """Pipeline for removing duplicate products within a session."""
    
    def __init__(self):
        self.seen_products: Set[str] = set()
        self.duplicate_count = 0
    
    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Remove duplicate items based on product ID."""
        adapter = ItemAdapter(item)
        
        product_id = adapter.get('product_id')
        if not product_id:
            # Generate ID if not present
            product_id = generate_product_id(
                adapter.get('store', ''),
                adapter.get('url', '')
            )
            adapter['product_id'] = product_id
        
        if product_id in self.seen_products:
            self.duplicate_count += 1
            spider.logger.debug(f"Duplicate product found: {adapter.get('name')}")
            raise DropItem(f"Duplicate product: {product_id}")
        
        self.seen_products.add(product_id)
        return item
    
    def close_spider(self, spider: Spider) -> None:
        """Log deduplication statistics."""
        spider.logger.info(
            f"Deduplication complete. Removed {self.duplicate_count} duplicates. "
            f"Processed {len(self.seen_products)} unique products."
        )


class DatabasePipeline:
    """Enhanced pipeline for storing data in the database."""
    
    def __init__(self):
        self.items_processed = 0
        self.items_saved = 0
        self.items_failed = 0
        self.batch_size = 100
        self.batch_items = []
    
    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Process item and add to batch for database insertion."""
        self.items_processed += 1
        self.batch_items.append(item)
        
        # Process batch when it reaches the specified size
        if len(self.batch_items) >= self.batch_size:
            self._process_batch(spider)
        
        return item
    
    def close_spider(self, spider: Spider) -> None:
        """Process remaining items and log statistics."""
        if self.batch_items:
            self._process_batch(spider)
        
        spider.logger.info(
            f"Database pipeline complete. "
            f"Processed: {self.items_processed}, "
            f"Saved: {self.items_saved}, "
            f"Failed: {self.items_failed}"
        )
    
    def _process_batch(self, spider: Spider) -> None:
        """Process a batch of items."""
        if not self.batch_items:
            return
        
        try:
            products = []
            for item in self.batch_items:
                try:
                    product = self._create_product_from_item(item)
                    products.append(product)
                except Exception as e:
                    self.items_failed += 1
                    spider.logger.error(f"Error creating product from item: {e}")
            
            # Batch insert products
            saved_count = db.insert_products_batch(products)
            self.items_saved += saved_count
            
            spider.logger.debug(f"Saved batch of {saved_count} products")
            
        except Exception as e:
            self.items_failed += len(self.batch_items)
            spider.logger.error(f"Error processing batch: {e}")
        
        finally:
            self.batch_items.clear()
    
    def _create_product_from_item(self, item: Dict[str, Any]) -> Product:
        """Create Product instance from scraped item."""
        adapter = ItemAdapter(item)
        
        return Product(
            name=adapter.get('name'),
            price=adapter.get('price'),
            category=adapter.get('category'),
            subcategory=adapter.get('subcategory'),
            store=adapter.get('store'),
            url=adapter.get('url'),
            image_url=adapter.get('image_url'),
            brand=adapter.get('brand'),
            description=adapter.get('description'),
            scraped_at=adapter.get('scraped_at', datetime.now())
        )


class CategoryPipeline:
    """Pipeline for processing category items only."""
    
    def __init__(self):
        self.categories_saved = 0
        self.categories_failed = 0
    
    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Process category information from items."""
        adapter = ItemAdapter(item)
        
        # Check if this is a CategoryItem
        if adapter.get('_type') == 'category':
            # This is a category item, save it to database
            try:
                store = adapter.get('store')
                category = adapter.get('category')
                subcategory = adapter.get('subcategory')
                category_url = adapter.get('category_url')
                
                if store and category:
                    db.insert_category(store, category, subcategory, category_url)
                    self.categories_saved += 1
                    spider.logger.debug(f"Saved category: {store} - {category}")
            except Exception as e:
                self.categories_failed += 1
                spider.logger.error(f"Error saving category: {e}")
            
            # Don't pass category items to other pipelines
            raise DropItem("Category item processed")
        
        # For regular product items, category info is only saved to products table
        # Do not save to categories table
        
        return item
    
    def close_spider(self, spider: Spider) -> None:
        """Log category pipeline statistics."""
        spider.logger.info(
            f"Category pipeline complete. "
            f"Saved: {self.categories_saved}, "
            f"Failed: {self.categories_failed}"
        )


class PriceAnalysisPipeline:
    """Pipeline for analyzing price data and trends."""
    
    def __init__(self):
        self.products_by_name: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.price_stats = {
            'total_products': 0,
            'with_discounts': 0,
            'multi_store_products': 0
        }
    
    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Analyze price data for the item."""
        adapter = ItemAdapter(item)
        
        self.price_stats['total_products'] += 1
        
        # Track discount information
        if adapter.get('discount_percentage', 0) > 0:
            self.price_stats['with_discounts'] += 1
        
        # Group products by normalized name for cross-store comparison
        product_name = PriceAnalysisPipeline._normalize_product_name(adapter.get('name', ''))
        if product_name:
            if product_name not in self.products_by_name:
                self.products_by_name[product_name] = {}
            
            store = adapter.get('store')
            if store:
                self.products_by_name[product_name][store] = {
                    'price': adapter.get('price'),
                    'original_price': adapter.get('original_price'),
                    'url': adapter.get('url'),
                    'discount_percentage': adapter.get('discount_percentage', 0)
                }
        
        return item
    
    def close_spider(self, spider: Spider) -> None:
        """Log price analysis results."""
        # Count multi-store products
        multi_store_products = {
            name: stores for name, stores in self.products_by_name.items()
            if len(stores) > 1
        }
        self.price_stats['multi_store_products'] = len(multi_store_products)
        
        # Log statistics
        spider.logger.info(f"Price Analysis Results:")
        spider.logger.info(f"  Total products: {self.price_stats['total_products']}")
        spider.logger.info(f"  Products with discounts: {self.price_stats['with_discounts']}")
        spider.logger.info(f"  Multi-store products: {self.price_stats['multi_store_products']}")
        
        # Log price comparison examples
        self._log_price_comparisons(spider, multi_store_products)
    
    @staticmethod
    def _normalize_product_name(name: str) -> str:
        """Normalize product name for comparison."""
        if not name:
            return ''
        
        # Remove brand names, sizes, and other variations
        normalized = clean_text(name.lower())
        # Remove common words that don't affect product identity
        stop_words = ['органічний', 'органический', 'fresh', 'свіжий', 'свежий']
        for word in stop_words:
            normalized = re.sub(rf'\b{word}\b', '', normalized, flags=re.IGNORECASE)
        
        return normalized.strip()
    
    def _log_price_comparisons(self, spider: Spider, 
                              multi_store_products: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        """Log interesting price comparisons."""
        # Show top 5 products with biggest price differences
        price_differences = []
        
        for name, stores in multi_store_products.items():
            prices = [(store, data['price']) for store, data in stores.items() 
                     if data.get('price')]
            
            if len(prices) >= 2:
                prices.sort(key=lambda x: x[1])
                cheapest = prices[0]
                most_expensive = prices[-1]
                difference = most_expensive[1] - cheapest[1]
                difference_pct = (difference / most_expensive[1]) * 100
                
                price_differences.append({
                    'name': name,
                    'cheapest': cheapest,
                    'most_expensive': most_expensive,
                    'difference': difference,
                    'difference_pct': difference_pct
                })
        
        # Sort by percentage difference and show top 5
        price_differences.sort(key=lambda x: x['difference_pct'], reverse=True)
        
        spider.logger.info("Top price differences across stores:")
        for i, diff in enumerate(price_differences[:5], 1):
            spider.logger.info(
                f"  {i}. {diff['name'][:50]}..."
                f" | Cheapest: {diff['cheapest'][0]} (₴{diff['cheapest'][1]:.2f})"
                f" | Most expensive: {diff['most_expensive'][0]} (₴{diff['most_expensive'][1]:.2f})"
                f" | Difference: ₴{diff['difference']:.2f} ({diff['difference_pct']:.1f}%)"
            )


# Pipeline configuration for settings.py
ITEM_PIPELINES = {
    'grocery_scraper.pipelines.ValidationPipeline': 100,
    'grocery_scraper.pipelines.DeduplicationPipeline': 200,
    'grocery_scraper.pipelines.DatabasePipeline': 300,
    'grocery_scraper.pipelines.PriceAnalysisPipeline': 400,
}