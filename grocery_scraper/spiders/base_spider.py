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

            # Add default page methods if not specified
            if 'playwright_page_methods' not in request_meta:
                from scrapy_playwright.page import PageMethod
                request_meta['playwright_page_methods'] = [
                    PageMethod('wait_for_load_state', 'domcontentloaded'),
                    PageMethod('wait_for_timeout', 300),
                    PageMethod('wait_for_function', self.get_wait_function(), timeout=5000),
                ]

        # Add errback to ensure page cleanup
        if 'errback' not in kwargs:
            kwargs['errback'] = self.close_page_on_error

        return Request(
            url=url,
            callback=callback,
            meta=request_meta,
            **kwargs
        )

    def get_wait_function(self) -> str:
        """Get JavaScript wait function for page loading. Override in subclasses."""
        return '''() => {
            const hasProducts = document.querySelectorAll('[data-testid="product-tile"], [data-autotestid="shop-silpo-product-card"], .sf-product-card').length > 0;
            const hasCategories = document.querySelectorAll('a[href*="/categories/"], a[data-autotestid="ssr-menu-categories__link"], .a-megamenu-item a').length > 0;
            const hasContent = document.body.innerText.length > 100;
            const isEmpty = document.querySelector('.empty-results') !== null;
            return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
        }'''

    async def close_page_on_error(self, failure):
        """Close Playwright page on request failure."""
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
            self.logger.info(f"[{self.store_name.upper()}] Closed page due to error: {failure.value}")

    def get_category_url_from_db(self, category_name: str) -> Optional[str]:
        """Get category URL from database by name."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT category_url
                FROM categories
                WHERE store = ?
                  AND category LIKE ?
            ''', (self.store_name, f'%{category_name}%'))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error querying database: {e}")
            return None
        finally:
            conn.close()

    def parse(self, response: Response):
        """Default parse method - handles category discovery mode."""
        # Check if we have a specific category to scrape
        category_name = getattr(self, 'category_name', None)
        start_url = getattr(self, 'start_url', None)
        
        # If we have a specific category name, only scrape that category
        if category_name:
            self.logger.info(f"Scraping specific category: {category_name}")
            # If we have a direct category URL, parse it directly
            if start_url and start_url != response.url:
                yield self.make_request(
                    start_url,
                    callback=self.parse_category,
                    meta={'category_name': category_name, 'page': 1}
                )
                return
            else:
                # Try to parse the current page as a category page
                yield from self.parse_category(response)
                return

        # If in discovery mode, only discover categories and exit
        if self.discover_categories_mode:
            self.logger.info(f"[{self.store_name.upper()}] Running in category discovery mode")
            yield from self.discover_categories(response)
            return

        # Otherwise, check if we should load from DB or discover
        categories_exist = self.check_categories_in_db()

        if categories_exist:
            self.logger.info("Loading categories from database")
            yield from self.parse_all_categories_from_db()
        else:
            self.logger.info("No categories in database, starting category discovery")
            yield from self.discover_categories(response)

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
        """Parse category page and extract products."""
        category_name = response.meta.get('category_name') or getattr(self, 'category_name', None) or 'Unknown'
        page = response.meta.get('page', 1)

        self.logger.info(f"[{self.store_name.upper()}] Parsing category: {category_name} (page {page})")
        self.logger.info(f"[{self.store_name.upper()}] Response URL: {response.url}")
        self.logger.info(f"[{self.store_name.upper()}] Response status: {response.status}")
        self.logger.info(f"[{self.store_name.upper()}] Response size: {len(response.text)} chars")

        # Extract products using selectors
        product_cards = self.get_product_cards(response)
        products_found = 0
        self.logger.info(f"[{self.store_name.upper()}] Total product cards found: {len(product_cards)}")

        for card in product_cards:
            product = self.extract_product(card, response, category_name)
            if product:
                self.save_product(product)
                yield product
                products_found += 1

        self.logger.info(f"[{self.store_name.upper()}] Found {products_found} products on page {page}")

        # Close Playwright page if included
        yield from self.close_page_if_needed(response)

        # Handle pagination if on first page
        if page == 1:
            yield from self.handle_pagination(response, category_name)

    def get_product_cards(self, response: Response):
        """Get product cards from response. Override in subclasses if needed."""
        product_cards = []
        if isinstance(self.product_card_selector, list):
            for selector in self.product_card_selector:
                cards = response.css(selector)
                product_cards.extend(cards)
                self.logger.info(f"[{self.store_name.upper()}] Found {len(cards)} cards with selector: {selector}")
        else:
            product_cards = response.css(self.product_card_selector)
            self.logger.info(f"[{self.store_name.upper()}] Found {len(product_cards)} cards with selector: {self.product_card_selector}")
        return product_cards

    def extract_product(self, card, response: Response, category_name: str) -> Optional[Dict[str, Any]]:
        """Extract product data from product card."""
        # Product name
        name = card.css(f'{self.product_title_selector}::text').get()
        if not name and hasattr(self, 'product_title_alt_selector'):
            name = card.css(f'{self.product_title_alt_selector}::text').get()
        if not name:
            name = card.css('::attr(title)').get()
        name = self.clean_text(name)

        # Product price
        price = self.extract_price_from_card(card)
        if not price:
            return None

        # Product URL
        product_url = self.extract_url_from_card(card, response)

        # Extract image URL
        image_url = self.extract_image_url_from_card(card, response)

        # Extract category and subcategory from breadcrumbs if available
        category, subcategory = self.extract_category_from_breadcrumbs(response)
        if not category:
            category = category_name

        return {
            'name': name,
            'price': price,
            'category': category,
            'subcategory': subcategory,
            'store': self.store_name,
            'url': product_url,
            'image_url': image_url,
        }

    def extract_price_from_card(self, card) -> Optional[float]:
        """Extract price from product card."""
        price_text = None
        if isinstance(self.product_price_selectors, list):
            for price_selector in self.product_price_selectors:
                price_text = card.css(f'{price_selector}::text').get()
                if price_text:
                    break
        else:
            price_text = card.css(f'{self.product_price_selectors}::text').get()
        return self.clean_price(price_text)

    def extract_url_from_card(self, card, response: Response) -> Optional[str]:
        """Extract product URL from card."""
        product_url = None
        if self.product_link_selector:
            product_url = card.css(f'{self.product_link_selector}::attr(href)').get()
        else:
            # Card itself might be a link
            product_url = card.css('::attr(href)').get()
        
        if product_url:
            product_url = urljoin(response.url, product_url)
        return product_url

    def extract_image_url_from_card(self, card, response: Response) -> Optional[str]:
        """Extract image URL from card."""
        image_url = card.css('[data-autotestid="img"]::attr(src)').get()
        if not image_url:
            image_url = card.css('img::attr(src)').get()
        if not image_url:
            image_url = card.css('img::attr(data-src)').get()
        if image_url:
            image_url = urljoin(response.url, image_url)
        return image_url

    def close_page_if_needed(self, response: Response):
        """Close Playwright page if included."""
        page_obj = response.meta.get('playwright_page')
        if page_obj:
            import asyncio
            asyncio.create_task(page_obj.close())
            self.logger.debug(f"[{self.store_name.upper()}] Scheduled page close for {response.url}")
        return []

    def handle_pagination(self, response: Response, category_name: str):
        """Handle pagination for categories."""
        # Get pagination block
        pag_block = response.css(self.pagination_selector)

        if pag_block:
            # Look for pagination items
            pagination_items = self.get_pagination_items(pag_block)

            if pagination_items:
                page_numbers = self.extract_page_numbers(pagination_items)

                if page_numbers:
                    last_page_num = max(page_numbers)
                    self.logger.info(f"Found {last_page_num} total pages for {category_name}")

                    # Generate requests for remaining pages
                    for page_num in range(2, last_page_num + 1):
                        page_url = self.build_page_url(response.url, page_num)
                        yield self.make_request(
                            page_url,
                            callback=self.parse_category,
                            meta={'category_name': category_name, 'page': page_num}
                        )

    def get_pagination_items(self, pag_block):
        """Get pagination items from pagination block. Override if needed."""
        # Try common pagination patterns
        items = pag_block.css('.pagination-item.ng-star-inserted, .pagination-item')
        if not items:
            items = pag_block.css('.Pagination__item')
        if not items and self.store_name.lower() == 'varus':
            # Varus specific pagination
            last_page_element = pag_block.css('[data-transaction-name="Pagination - Go To Last"]')
            return last_page_element
        return items

    def extract_page_numbers(self, pagination_items):
        """Extract page numbers from pagination items."""
        page_numbers = []
        
        # Special case for Varus "Go To Last" button
        if (len(pagination_items) == 1 and 
            pagination_items[0].css('::attr(href)').get() and 
            'page=' in pagination_items[0].css('::attr(href)').get()):
            
            last_page_url = pagination_items[0].css('::attr(href)').get()
            page_match = re.search(r'[?&]page=(\d+)', last_page_url)
            if page_match:
                return [int(page_match.group(1))]
        
        # Standard pagination items
        for item in pagination_items:
            page_text = item.css('a::text').get()
            if not page_text:
                page_text = item.css('::text').get()
            if page_text and page_text.strip().isdigit():
                page_numbers.append(int(page_text.strip()))
        
        return page_numbers

    def build_page_url(self, current_url: str, page_num: int) -> str:
        """Build URL for specific page number."""
        if '?page=' in current_url:
            return re.sub(r'page=\d+', f'page={page_num}', current_url)
        else:
            separator = '&' if '?' in current_url else '?'
            return f"{current_url}{separator}page={page_num}"

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
