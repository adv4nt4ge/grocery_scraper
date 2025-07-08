#!/usr/bin/env python3
"""
Varus spider with store-specific logic and selectors.
"""

from typing import Any, Optional, Union
from urllib.parse import urljoin

from scrapy.http import Response

from .base_spider import BaseGrocerySpider
from ..utils import init_page_with_blocking


class VarusSpider(BaseGrocerySpider):
    """Spider for Varus grocery store."""

    name = 'varus'
    store_name = 'Varus'
    allowed_domains = ['varus.ua']
    start_urls = ['https://varus.ua']

    # Use Playwright for JavaScript rendering
    use_playwright = True

    # Varus-specific selectors - only main categories
    category_selectors = '.a-megamenu-item.a-megamenu-item--main.a-megamenu-item--has-child > a'
    product_card_selector = ['.sf-product-card', '.sf-product-card sf-product-card--out-of-stock-container']
    product_title_selector = '.sf-product-card__title'
    product_price_selectors = ['.sf-price__regular', '.sf-price__special']
    product_link_selector = 'a'
    pagination_selector = '[data-transaction-name="Pagination - Go To Last"]'

    # Breadcrumb settings
    breadcrumb_selector = '.breadcrumbs a'

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
        """Create request with Varus-specific Playwright settings."""
        self.logger.info(f"[VARUS] Creating custom request for: {url}")
        request_meta = meta or {}

        # Add Playwright settings if enabled
        if self.use_playwright:
            request_meta['playwright'] = True
            request_meta['playwright_include_page'] = True
            # Use custom page initialization with resource blocking
            request_meta['playwright_page_init_callback'] = init_page_with_blocking
            # Wait for the Vue.js API call to complete before extracting products
            from scrapy_playwright.page import PageMethod
            request_meta['playwright_page_methods'] = [
                PageMethod('wait_for_selector', '.sf-product-card, .sf-product-card__wrapper', timeout=3000),
            ]

        # Add errback to ensure page cleanup
        return super().make_request(url, callback, request_meta, errback=self.close_page_on_error, **kwargs)
    
    async def close_page_on_error(self, failure):
        """Close Playwright page on request failure."""
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
            self.logger.info(f"[VARUS] Closed page due to error: {failure.value}")

    def parse(self, response: Response):
        """Parse main page and discover categories."""
        # If we have a direct category URL, parse it directly
        if self.start_url and self.category_name:
            self.logger.info(f"Parsing direct category: {self.category_name}")
            # Use our custom request method for the category page
            yield self.make_request(
                self.start_url,
                callback=self.parse_category,
                meta={'category_name': self.category_name, 'page': 1}
            )
            return

        # If discover_categories flag is set, discover categories
        if self.discover_categories_mode:
            self.logger.info("Category discovery mode enabled")
            # Varus needs a custom request with Playwright to load categories
            yield self.make_request(
                self.start_urls[0],
                callback=self.discover_categories_with_js,
                meta={'page': 1}
            )
            return

        # Otherwise, check if we should load from DB or discover
        categories_exist = self.check_categories_in_db()
        
        if categories_exist:
            self.logger.info("Loading categories from database")
            yield from self.parse_all_categories_from_db()
        else:
            self.logger.info("No categories in database, starting category discovery")
            yield from self.discover_categories(response)

    def parse_all_categories_from_db(self):
        """Parse all categories stored in database."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                           SELECT category, category_url
                           FROM categories
                           WHERE store = ?
                             AND category_url IS NOT NULL
                             AND category_url != ''
                           ''', (self.store_name,))
            categories = cursor.fetchall()

            self.logger.info(f"Found {len(categories)} categories in database")

            excluded_count = 0
            for category_name, category_url in categories:
                # Check if URL should be excluded
                if self.is_category_excluded(category_name, None, category_url):
                    excluded_count += 1
                    continue
                    
                self.logger.info(f"Queuing category: {category_name} -> {category_url}")
                yield self.make_request(
                    category_url,
                    callback=self.parse_category,
                    meta={'category_name': category_name, 'page': 1}
                )
            
            if excluded_count > 0:
                self.logger.info(f"[VARUS] Excluded {excluded_count} categories based on URL filters")

        except Exception as e:
            self.logger.error(f"Error loading categories from database: {e}")
            self.logger.info("No categories found in database. Run category discovery first.")
        finally:
            conn.close()

    def discover_categories_with_js(self, response: Response):
        """Discover categories from Varus using JavaScript rendering."""
        self.logger.info("[VARUS] Starting category discovery with JavaScript rendering")
        
        # Clear existing categories first
        self.clear_categories_for_store()
        
        # Extract category elements to get both links and names
        category_elements = response.css(self.category_selectors)
        self.logger.info(f"[VARUS] Found {len(category_elements)} category elements")
        
        saved_count = 0
        skipped_count = 0
        for element in category_elements:
            category_url = element.css('::attr(href)').get()
            category_name = element.css('::text').get()
            
            if category_url and category_url.startswith('/') and category_name:
                full_url = urljoin(response.url, category_url)
                
                # Clean and use the actual category name from the link text
                category_name = self.clean_text(category_name)
                
                # Check if URL should be excluded
                if self.is_category_excluded(category_name, None, full_url):
                    skipped_count += 1
                    continue
                
                # Save to database
                if self.save_category_to_db(category_name, full_url):
                    saved_count += 1
        
        self.logger.info(f"[VARUS] Category discovery complete. Saved {saved_count} categories to database")
        if skipped_count > 0:
            self.logger.info(f"[VARUS] Skipped {skipped_count} categories based on URL filters")
        self.logger.info(f"[VARUS] Run spider again without discover_categories flag to parse products")
        
        # Close Playwright page if included
        page_obj = response.meta.get('playwright_page')
        if page_obj:
            import asyncio
            asyncio.create_task(page_obj.close())
            self.logger.debug(f"[VARUS] Scheduled page close for {response.url}")

    def parse_category(self, response: Response):
        """Parse category page and extract products."""
        category_name = response.meta.get('category_name') or self.category_name or 'Unknown'
        page = response.meta.get('page', 1)

        self.logger.info(f"[VARUS] Parsing category: {category_name} (page {page})")
        self.logger.info(f"[VARUS] Response URL: {response.url}")
        self.logger.info(f"[VARUS] Response status: {response.status}")
        self.logger.info(f"[VARUS] Response size: {len(response.text)} chars")

        # Check if sf-product-card elements are now present after waiting
        if 'sf-product-card' in response.text:
            self.logger.info("[VARUS] sf-product-card found in HTML after waiting")
        else:
            self.logger.warning("[VARUS] sf-product-card still NOT found in HTML after waiting")
            # Log first 1000 chars to debug
            self.logger.debug(f"[VARUS] HTML preview: {response.text[:1000]}")

        # Extract products using multiple CSS selectors
        product_cards = []
        for selector in self.product_card_selector:
            cards = response.css(selector)
            product_cards.extend(cards)
            self.logger.info(f"[VARUS] Found {len(cards)} cards with selector: {selector}")
        
        products_found = 0
        self.logger.info(f"[VARUS] Total product cards found: {len(product_cards)}")

        for card in product_cards:
            product = self.extract_product(card, response, category_name)
            if product:
                self.save_product(product)
                yield product
                products_found += 1

        self.logger.info(f"[VARUS] Found {products_found} products on page {page}")
        
        # Close Playwright page if included
        page_obj = response.meta.get('playwright_page')
        if page_obj:
            import asyncio
            asyncio.create_task(page_obj.close())
            self.logger.debug(f"[VARUS] Scheduled page close for {response.url}")

        # Handle pagination if on first page
        if page == 1:
            yield from self.handle_pagination(response, category_name)

    def extract_product(self, card, response: Response, category_name: str) -> Optional[
        dict[str, Union[Union[str, float, None], Any]]]:
        """Extract product data from product card."""
        # Product name
        name = card.css(f'{self.product_title_selector}::text').get()
        if not name:
            return None
        name = self.clean_text(name)

        # Product price
        price = None
        for price_selector in self.product_price_selectors:
            price_text = card.css(f'{price_selector}::text').get()
            if price_text:
                price = self.clean_price(price_text)
                if price:
                    break

        if not price:
            return None

        # Product URL
        product_url = card.css(f'{self.product_link_selector}::attr(href)').get()
        if product_url:
            product_url = urljoin(response.url, product_url)

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
        }

    def handle_pagination(self, response: Response, category_name: str):
        """Handle pagination for Varus categories."""
        # Find "Go To Last" button to get total pages
        last_page_element = response.css(self.pagination_selector)

        if last_page_element:
            last_page_url = last_page_element.css('::attr(href)').get()
            if last_page_url:
                # Extract page number from URL
                import re
                page_match = re.search(r'[?&]page=(\d+)', last_page_url)
                if page_match:
                    total_pages = int(page_match.group(1))
                    self.logger.info(f"Found {total_pages} total pages for {category_name}")

                    # Generate requests for remaining pages
                    base_url = response.url.split('?')[0]
                    for page_num in range(2, total_pages + 1):  # Scrape all pages
                        page_url = f"{base_url}?page={page_num}"
                        yield self.make_request(
                            page_url,
                            callback=self.parse_category,
                            meta={'category_name': category_name, 'page': page_num}
                        )


# Playwright page initialization function for Varus
async def init_varus_page(page, request):
    """Initialize page to wait for Vue.js API responses."""
    # Set up monitoring for the catalog API response
    await page.evaluate('''
        window.vueApiComplete = false;
        window.apiCallCount = 0;
        
        // Monitor network responses using resource event
        const originalFetch = window.fetch;
        window.fetch = function(...args) {
            const response = originalFetch.apply(this, arguments);
            if (args[0] && args[0].includes && args[0].includes('vue_storefront_catalog_2')) {
                window.apiCallCount++;
                response.then(res => {
                    if (res.status === 200) {
                        // Wait a bit for Vue to process the response
                        setTimeout(() => {
                            window.vueApiComplete = true;
                        }, 2000);
                    }
                }).catch(() => {});
            }
            return response;
        };
        
        // Fallback: Mark as complete after 15 seconds regardless
        setTimeout(() => {
            window.vueApiComplete = true;
        }, 15000);
    ''')
