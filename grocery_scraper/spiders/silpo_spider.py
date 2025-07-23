#!/usr/bin/env python3
"""
Silpo spider with store-specific logic and selectors.
"""

from .base_spider import BaseGrocerySpider


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

    def get_wait_function(self) -> str:
        """Get Silpo-specific JavaScript wait function."""
        return '''() => {
            const hasProducts = document.querySelector('[data-autotestid="shop-silpo-product-card"]') !== null;
            const hasCategories = document.querySelector('[data-autotestid="ssr-menu-categories__link"]') !== null;
            const hasContent = document.body.innerText.length > 100;
            const isEmpty = document.querySelector('.empty-results') !== null;
            return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
        }'''
