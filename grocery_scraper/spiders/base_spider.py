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
from config import STORES, DATABASE_PATH, EXCLUDED_URL_PATTERNS


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

    # Category discovery settings (can be overridden)
    category_selectors: str = None  # CSS selector for category links
    category_menu_button: str = None  # CSS selector for category menu button (if needed)

    def __init__(self, discover_categories=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse discover_categories flag
        self.discover_categories_mode = discover_categories == 'true' if discover_categories else False

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
                     CREATE TABLE IF NOT EXISTS products
                     (
                         id
                         INTEGER
                         PRIMARY
                         KEY
                         AUTOINCREMENT,
                         name
                         TEXT
                         NOT
                         NULL,
                         price
                         REAL
                         NOT
                         NULL,
                         category
                         TEXT,
                         subcategory
                         TEXT,
                         store
                         TEXT
                         NOT
                         NULL,
                         url
                         TEXT,
                         image_url
                         TEXT,
                         scraped_at
                         TIMESTAMP,
                         UNIQUE
                     (
                         name,
                         store,
                         url
                     )
                         )
                     ''')
        conn.execute('''
                     CREATE TABLE IF NOT EXISTS categories
                     (
                         id
                         INTEGER
                         PRIMARY
                         KEY
                         AUTOINCREMENT,
                         store
                         TEXT
                         NOT
                         NULL,
                         category
                         TEXT
                         NOT
                         NULL,
                         subcategory
                         TEXT,
                         category_url
                         TEXT,
                         UNIQUE
                     (
                         store,
                         category,
                         subcategory
                     )
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

            # Add resource blocking if not already specified
            if 'playwright_page_init_callback' not in request_meta:
                from ..utils import init_page_with_blocking
                request_meta['playwright_page_init_callback'] = init_page_with_blocking

        return Request(
            url=url,
            callback=callback,
            meta=request_meta,
            **kwargs
        )

    def parse(self, response: Response):
        """Default parse method - handles category discovery mode."""
        # If in discovery mode, only discover categories and exit
        if self.discover_categories_mode:
            self.logger.info(f"[{self.store_name.upper()}] Running in category discovery mode")
            yield from self.discover_categories(response)
            return

        # Otherwise, subclasses should implement their own parse logic
        raise NotImplementedError("Subclasses must implement parse method for product scraping")

    def check_categories_in_db(self) -> bool:
        """Check if categories exist in the database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                           SELECT COUNT(*)
                           FROM categories
                           WHERE store = ?
                           ''', (self.store_name,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking categories: {e}")
            return False
        finally:
            conn.close()

    def save_category_to_db(self, category_name: str, category_url: str, subcategory: str = None) -> bool:
        """Save category to database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                           INSERT
                           OR IGNORE INTO categories (store, category, subcategory, category_url)
                VALUES (?, ?, ?, ?)
                           ''', (self.store_name, category_name, subcategory, category_url))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error saving category: {e}")
            return False
        finally:
            conn.close()

    def clear_categories_for_store(self):
        """Clear all existing categories for this store before discovery."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                           DELETE
                           FROM categories
                           WHERE store = ?
                           ''', (self.store_name,))
            deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                self.logger.info(f"[{self.store_name.upper()}] Cleared {deleted} existing categories")
            return deleted
        except Exception as e:
            self.logger.error(f"Error clearing categories: {e}")
            return 0
        finally:
            conn.close()

    def discover_categories(self, response: Response):
        """
        Base method for category discovery. Should be overridden by subclasses.
        This method should:
        1. Clear existing categories for this store
        2. Extract category links from the page
        3. Save them to the database
        4. NOT start parsing products - just save categories
        """
        self.logger.info(f"[{self.store_name.upper()}] Starting category discovery")

        # Clear existing categories first
        self.clear_categories_for_store()

        # Try to find categories directly
        if not self.category_selectors:
            raise NotImplementedError("Subclasses must define category_selectors or override discover_categories")

        # Extract category links
        categories = response.css(f'{self.category_selectors}')
        self.logger.info(f"[{self.store_name.upper()}] Found {len(categories)} categories")

        saved_count = 0
        skipped_count = 0
        for category in categories:
            category_links = category.css('::attr(href)').get()
            category_name = category.css('::text').get().strip()
            full_url = urljoin(response.url, category_links)

            # Check if URL should be excluded
            if self.is_category_excluded(category_name, None, full_url):
                skipped_count += 1
                continue

            # Save to database
            if self.save_category_to_db(category_name, full_url):
                saved_count += 1

        self.logger.info(
            f"[{self.store_name.upper()}] Category discovery complete. Saved {saved_count} categories to database")
        if skipped_count > 0:
            self.logger.info(
                f"[{self.store_name.upper()}] Skipped {skipped_count} categories based on exclusion filters")
        self.logger.info(
            f"[{self.store_name.upper()}] Run spider again without discover_categories flag to parse products")
        return []  # Return empty list for yield from compatibility

    def parse_all_categories_from_db(self):
        """Parse all categories stored in database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                           SELECT category, subcategory, category_url
                           FROM categories
                           WHERE store = ?
                             AND category_url IS NOT NULL
                             AND category_url != ''
                           ''', (self.store_name,))
            categories = cursor.fetchall()

            self.logger.info(f"[{self.store_name.upper()}] Found {len(categories)} categories in database")

            excluded_count = 0
            for category_name, subcategory, category_url in categories:
                # Check if category should be excluded
                if self.is_category_excluded(category_name, subcategory, category_url):
                    excluded_count += 1
                    continue

                self.logger.info(f"[{self.store_name.upper()}] Queuing category: {category_name} -> {category_url}")
                yield self.make_request(
                    category_url,
                    callback=self.parse_category,
                    meta={'category_name': category_name, 'subcategory': subcategory, 'page': 1}
                )

            if excluded_count > 0:
                self.logger.info(f"[{self.store_name.upper()}] Excluded {excluded_count} categories based on filter")

        except Exception as e:
            self.logger.error(f"Error loading categories from database: {e}")
        finally:
            conn.close()

    def parse_category(self, response: Response):
        """Parse category page - should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement parse_category method")

    def is_category_excluded(self, category: str, subcategory: str = None, url: str = None) -> bool:
        """Check if a URL should be excluded based on URL patterns."""
        # Only check URL patterns if URL is provided
        if not url:
            return False

        url_lower = url.lower()

        # Check store-specific URL patterns
        store_url_patterns = EXCLUDED_URL_PATTERNS.get(self.store_name.lower(), [])
        for pattern in store_url_patterns:
            if pattern.lower() in url_lower:
                self.logger.info(
                    f"[{self.store_name.upper()}] Excluding category '{category}' - URL '{url}' (matched pattern: '{pattern}')")
                return True

        return False

    def closed(self, reason):
        """Called when spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Items scraped: {self.items_scraped}")
