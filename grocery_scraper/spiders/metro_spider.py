#!/usr/bin/env python3
"""
Metro spider with store-specific logic and selectors.
"""

from .base_spider import BaseGrocerySpider


class MetroSpider(BaseGrocerySpider):
    """Spider for Metro grocery store."""

    name = 'metro'
    store_name = 'Metro'
    allowed_domains = ['metro.zakaz.ua', 'zakaz.ua']
    start_urls = ['https://metro.zakaz.ua/uk/']

    # Use Playwright for JavaScript rendering
    use_playwright = True

    # Metro-specific selectors based on website analysis
    category_selectors = 'a[href*="/uk/categories/"], a[href*="/categories/"]'
    product_card_selector = '[data-testid="product-tile"]'
    product_title_selector = '.ProductTile__title'
    product_price_selectors = ['.Price__value_caption', '.Price__value_mobile', '.Price__value']
    product_link_selector = ''  # ProductTile itself is the link
    pagination_selector = '.Pagination'

    # Breadcrumb settings
    breadcrumb_selector = '[data-marker="Disabled Breadcrumb"]'

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
        """Get Metro-specific JavaScript wait function."""
        return '''() => {
            const hasProducts = document.querySelector('[data-testid="product-tile"]') !== null;
            const hasCategories = document.querySelector('a[href*="/categories/"]') !== null;
            const hasContent = document.body.innerText.length > 100;
            const isEmpty = document.querySelector('.empty-results') !== null;
            return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
        }'''