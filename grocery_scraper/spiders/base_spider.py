#!/usr/bin/env python3
"""
Base spider class with common functionality for all grocery store spiders.
"""

import scrapy
from scrapy import Request
from scrapy.http import Response
from typing import Dict, Any, Iterator, Optional, List
from datetime import datetime
import re
import sqlite3
from urllib.parse import urljoin

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config import STORES, DATABASE_PATH


class BaseGrocerySpider(scrapy.Spider):
    """Base spider with common functionality for grocery scrapers."""
    
    # Override these in subclasses
    store_name: str = None
    allowed_domains: List[str] = []
    start_urls: List[str] = []
    
    # Pagination settings
    pagination_enabled: bool = True
    max_pages: Optional[int] = None
    
    # Category extraction patterns (can be overridden)
    breadcrumb_selector: str = None
    category_separator: str = ' > '
    
    # Use Playwright for JavaScript sites
    use_playwright: bool = False
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Get store configuration
        self.store_config = STORES.get(self.store_name.lower())
        if not self.store_config:
            raise ValueError(f"No configuration found for store: {self.store_name}")
        
        # Set up database connection
        self.db_path = DATABASE_PATH
        self.init_db()
        
        # Initialize counters
        self.items_scraped = 0
    
    def init_db(self):
        """Initialize database tables."""
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
                image_url TEXT,
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
    
    def save_product(self, item: Dict[str, Any]):
        """Save product to database."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR REPLACE INTO products 
                (name, price, category, subcategory, store, url, image_url, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('name'),
                item.get('price'),
                item.get('category'),
                item.get('subcategory'),
                item.get('store'),
                item.get('url'),
                item.get('image_url'),
                datetime.now().isoformat()
            ))
            conn.commit()
            self.items_scraped += 1
        except Exception as e:
            self.logger.error(f"Error saving product: {e}")
        finally:
            conn.close()
    
    def save_category(self, store: str, category: str, subcategory: str = None, category_url: str = None):
        """Save category to database."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR REPLACE INTO categories 
                (store, category, subcategory, category_url)
                VALUES (?, ?, ?, ?)
            ''', (store, category, subcategory, category_url))
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error saving category: {e}")
        finally:
            conn.close()
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text from extra spaces and newlines."""
        if not text:
            return ""
        return ' '.join(text.strip().split())
    
    @staticmethod
    def clean_price(price_text: str) -> Optional[float]:
        """Extract numeric price from text."""
        if not price_text:
            return None
        clean = re.sub(r'[^\d,.]', '', str(price_text))
        clean = clean.replace(',', '.')
        try:
            return float(clean)
        except:
            return None
    
    
    def extract_category_from_breadcrumbs(self, response: Response) -> tuple:
        """Extract category and subcategory from breadcrumbs."""
        if not self.breadcrumb_selector:
            return None, None
        
        breadcrumbs = response.css(self.breadcrumb_selector + '::text').getall()
        breadcrumbs = [self.clean_text(bc) for bc in breadcrumbs if bc.strip()]
        
        if len(breadcrumbs) >= 2:
            category = breadcrumbs[-2]  # Second to last is usually category
            subcategory = breadcrumbs[-1] if len(breadcrumbs) > 2 else None
            return category, subcategory
        elif len(breadcrumbs) == 1:
            return breadcrumbs[0], None
        
        return None, None
    
    def make_request(self, url: str, callback=None, meta: Optional[Dict] = None, **kwargs) -> Request:
        """Create request with common settings."""
        if not callback:
            callback = self.parse
        
        request_meta = meta or {}
        
        # Add Playwright settings if enabled
        if self.use_playwright:
            request_meta['playwright'] = True
            request_meta['playwright_include_page'] = True
        
        return Request(
            url=url,
            callback=callback,
            meta=request_meta,
            **kwargs
        )
    
    def parse(self, response: Response):
        """Default parse method - should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement parse method")
    
    def closed(self, reason):
        """Called when spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Items scraped: {self.items_scraped}")