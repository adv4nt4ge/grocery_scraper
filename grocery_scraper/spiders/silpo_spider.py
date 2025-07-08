#!/usr/bin/env python3
"""
Silpo spider with store-specific logic and selectors.
"""

from typing import Any, Optional, Union
from urllib.parse import urljoin, quote
import re

from scrapy.http import Response
from scrapy_playwright.page import PageMethod

from .base_spider import BaseGrocerySpider
from ..utils import init_page_with_blocking


class SilpoSpider(BaseGrocerySpider):
    """Spider for Silpo grocery store."""

    name = 'silpo'
    store_name = 'Silpo'
    allowed_domains = ['silpo.ua']
    start_urls = ['https://silpo.ua']

    # Use Playwright for JavaScript rendering
    use_playwright = True

    # Silpo-specific selectors (preserved from original)
    category_selectors = 'a[data-autotestid="ssr-menu-categories__link"]'
    product_card_selector = ['[data-autotestid="shop-silpo-product-card"]', '.product-card-list__item', '.product-card']
    product_title_selector = '.product-card__title'
    product_price_selectors = '.product-card-price__displayPrice'
    product_link_selector = '.product-card a, a'
    pagination_selector = '.pagination'

    # Breadcrumb settings - target the link inside, not the whole li element
    breadcrumb_selector = '.breadcrumbs-list__item breadcrumbs-list__item--active ng-star-inserted a'

    def __init__(self, category_name=None, start_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_name = category_name
        self.start_url = start_url

        # If specific URL provided, use it
        if start_url:
            self.start_urls = [start_url]
        # If category name provided, look it up in database
        elif category_name:
            category_url = self.get_category_url_from_db(category_name)
            if category_url:
                self.start_urls = [category_url]
                self.start_url = category_url
                self.logger.info(f"Found category URL in database: {category_url}")
            else:
                self.logger.warning(f"Category '{category_name}' not found in database")

    def get_category_url_from_db(self, category_name):
        """Get category URL from database by name."""
        import sqlite3
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

    def make_request(self, url: str, callback=None, meta=None, **kwargs):
        """Create request with Silpo-specific Playwright settings."""
        self.logger.info(f"[SILPO] Creating custom request for: {url}")
        request_meta = meta or {}

        # Add Playwright settings if enabled
        if self.use_playwright:
            request_meta['playwright'] = True
            request_meta['playwright_include_page'] = True
            # Use custom page initialization with resource blocking
            request_meta['playwright_page_init_callback'] = init_page_with_blocking
            # Wait for products to load
            request_meta['playwright_page_methods'] = [
                PageMethod('wait_for_load_state', 'domcontentloaded'),
                PageMethod('wait_for_timeout', 300),
                PageMethod('wait_for_function',
                           '''() => {
                               // Quick check if essential content loaded
                               const hasProducts = document.querySelector('[data-autotestid="shop-silpo-product-card"]') !== null;
                               const hasCategories = document.querySelector('[data-autotestid="menu-categories__link"]') !== null;
                               const hasContent = document.body.innerText.length > 100;
                               const isEmpty = document.querySelector('.empty-results') !== null;
                               return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
                           }''', timeout=5000),
            ]

        # Add errback to ensure page cleanup
        return super().make_request(url, callback, request_meta, errback=self.close_page_on_error, **kwargs)

    async def close_page_on_error(self, failure):
        """Close Playwright page on request failure."""
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
            self.logger.info(f"[SILPO] Closed page due to error: {failure.value}")

    def parse(self, response: Response):
        """Parse main page and discover categories."""
        # If we have a specific category name, only scrape that category
        if self.category_name:
            self.logger.info(f"Scraping specific category: {self.category_name}")
            # If we have a direct category URL, parse it directly
            if self.start_url and self.start_url != response.url:
                yield self.make_request(
                    self.start_url,
                    callback=self.parse_category,
                    meta={'category_name': self.category_name, 'page': 1}
                )
                return
            else:
                # Try to parse the current page as a category page
                yield from self.parse_category(response)
                return

        # If discover_categories flag is set, always discover categories
        if self.discover_categories_mode:
            self.logger.info("Category discovery mode enabled")
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

    def parse_category(self, response: Response):
        """Parse category page and extract products."""
        category_name = response.meta.get('category_name') or self.category_name or 'Unknown'
        page = response.meta.get('page', 1)

        self.logger.info(f"[SILPO] Parsing category: {category_name} (page {page})")
        self.logger.info(f"[SILPO] Response URL: {response.url}")
        self.logger.info(f"[SILPO] Response status: {response.status}")
        self.logger.info(f"[SILPO] Response size: {len(response.text)} chars")

        # Extract products using multiple CSS selectors
        product_cards = []
        for selector in self.product_card_selector:
            cards = response.css(selector)
            product_cards.extend(cards)
            self.logger.info(f"[SILPO] Found {len(cards)} cards with selector: {selector}")

        products_found = 0
        self.logger.info(f"[SILPO] Total product cards found: {len(product_cards)}")

        for card in product_cards:
            product = self.extract_product(card, response, category_name)
            if product:
                self.save_product(product)
                yield product
                products_found += 1

        self.logger.info(f"[SILPO] Found {products_found} products on page {page}")

        # Close Playwright page if included
        page_obj = response.meta.get('playwright_page')
        if page_obj:
            import asyncio
            asyncio.create_task(page_obj.close())
            self.logger.debug(f"[SILPO] Scheduled page close for {response.url}")

        # Handle pagination if on first page
        if page == 1:
            yield from self.handle_pagination(response, category_name)

    def extract_product(self, card, response: Response, category_name: str) -> Optional[
        dict[str, Union[Union[str, float, None], Any]]]:
        """Extract product data from product card."""
        # Product name
        name = card.css(f'{self.product_title_selector}::text').get()
        name = self.clean_text(name)

        # Product price
        price_text = card.css(f'{self.product_price_selectors}::text').get()
        price = self.clean_price(price_text)

        if not price:
            return None

        # Product URL
        product_url = card.css(f'{self.product_link_selector}::attr(href)').get()
        if product_url:
            product_url = urljoin(response.url, product_url)

        # Extract image URL
        image_url = card.css('[data-autotestid="img"]::attr(src)').get()
        if not image_url:
            image_url = card.css('img::attr(src)').get()
        if image_url:
            image_url = urljoin(response.url, image_url)

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

    def handle_pagination(self, response: Response, category_name: str):
        """Handle pagination for Silpo categories."""
        # Get pagination block
        pag_block = response.css(self.pagination_selector)

        if pag_block:
            # Look for pagination items
            pagination_items = pag_block.css('.pagination-item.ng-star-inserted, .pagination-item')

            if pagination_items:
                # Extract page numbers
                page_numbers = []
                for item in pagination_items:
                    page_text = item.css('a::text').get()
                    if not page_text:
                        page_text = item.css('::text').get()
                    if page_text and page_text.strip().isdigit():
                        page_numbers.append(int(page_text.strip()))

                if page_numbers:
                    last_page_num = max(page_numbers)
                    self.logger.info(f"Found {last_page_num} total pages for {category_name}")

                    # Generate requests for remaining pages
                    base_url = response.url.split('?')[0]
                    for page_num in range(2, last_page_num + 1):
                        if '?page=' in response.url:
                            page_url = re.sub(r'page=\d+', f'page={page_num}', response.url)
                        else:
                            separator = '&' if '?' in response.url else '?'
                            page_url = f"{response.url}{separator}page={page_num}"

                        yield self.make_request(
                            page_url,
                            callback=self.parse_category,
                            meta={'category_name': category_name, 'page': page_num}
                        )
