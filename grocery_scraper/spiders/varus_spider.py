#!/usr/bin/env python3
"""
Varus spider with store-specific logic and selectors.
"""

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

    def get_wait_function(self) -> str:
        """Get Varus-specific JavaScript wait function."""
        return '''() => {
            const hasProducts = document.querySelectorAll('.sf-product-card, .sf-product-card__wrapper').length > 0;
            const hasCategories = document.querySelectorAll('.a-megamenu-item a').length > 0;
            const hasContent = document.body.innerText.length > 100;
            const isEmpty = document.querySelector('.empty-results') !== null;
            return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
        }'''

    def make_request(self, url: str, callback=None, meta=None, **kwargs):
        """Create request with Varus-specific Playwright settings."""
        self.logger.info(f"[VARUS] Creating custom request for: {url}")
        request_meta = meta or {}

        # Add Varus-specific Playwright settings if enabled
        if self.use_playwright:
            request_meta['playwright'] = True
            request_meta['playwright_include_page'] = True
            request_meta['playwright_page_init_callback'] = init_page_with_blocking
            
            # Wait for Vue.js products to load
            from scrapy_playwright.page import PageMethod
            request_meta['playwright_page_methods'] = [
                PageMethod('wait_for_selector', '.sf-product-card, .sf-product-card__wrapper', timeout=3000),
            ]

        return super().make_request(url, callback, request_meta, **kwargs)

    def parse(self, response: Response):
        """Parse main page - uses base class implementation with Varus discovery override."""
        # If discover_categories flag is set, use Varus-specific discovery
        if self.discover_categories_mode:
            self.logger.info("Category discovery mode enabled - using Varus-specific discovery")
            yield self.make_request(
                self.start_urls[0],
                callback=self.discover_categories_with_js,
                meta={'page': 1}
            )
            return
        
        # Otherwise use base class implementation
        yield from super().parse(response)

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
