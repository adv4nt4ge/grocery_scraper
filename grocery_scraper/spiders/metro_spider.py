import scrapy
from scrapy.http import Request
from scrapy_playwright.page import PageMethod
from .base_spider import BaseGrocerySpider
from urllib.parse import urljoin, quote
import re
from datetime import datetime


class MetroSpider(scrapy.Spider):
    name = 'metro'
    allowed_domains = ['metro.zakaz.ua', 'zakaz.ua']
    base_url = 'https://metro.zakaz.ua'
    
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 15000,
    }
    
    def __init__(self, product_name=None, use_db_categories=False, *args, **kwargs):
        super(MetroSpider, self).__init__(*args, **kwargs)
        self.product_name = product_name
        self.use_db_categories = use_db_categories
    
    async def start(self):
        """New async start method for Scrapy 2.13+"""
        for request in self.start_requests():
            yield request
    
    def start_requests(self):
        # Check if we should use categories from database
        if self.use_db_categories:
            from database import db
            categories = db.get_category_urls('Metro')
            if categories:
                self.logger.info(f"Found {len(categories)} categories in database for Metro")
                for cat in categories:
                    yield Request(
                        cat['category_url'],
                        callback=self.parse_category_page,
                        meta={
                            'category': cat['category'],
                            'subcategory': cat.get('subcategory'),
                            'category_id': cat['id'],
                            'playwright': True,
                            'playwright_page_methods': [
                                PageMethod('wait_for_load_state', 'domcontentloaded'),
                                PageMethod('wait_for_function', 
                                           '''() => { 
                                               const hasMainProducts = document.querySelectorAll("[data-testid='product-tile']").length > 1;
                                               const hasArticles = document.querySelectorAll("article").length > 0;
                                               const hasLoaded = document.readyState === "complete";
                                               return hasMainProducts || (hasArticles && hasLoaded);
                                           }''', 
                                           timeout=8000),
                                PageMethod('wait_for_timeout', 500)
                            ]
                        }
                    )
                return
            else:
                self.logger.warning("No categories found in database for Metro, falling back to discovery")
        
        if self.product_name:
            # Metro/Zakaz working search URL patterns
            search_patterns = [
                f'/uk/search/?q={quote(self.product_name)}',  # Primary search URL
                f'/search?q={quote(self.product_name)}'       # Fallback that redirects to primary
            ]
            
            for pattern in search_patterns:
                url = urljoin(self.base_url, pattern)
                yield Request(
                    url,
                    callback=self.parse_search_results,
                    meta={
                        'playwright': True,
                        'playwright_page_methods': [
                            PageMethod('wait_for_load_state', 'domcontentloaded'),
                            PageMethod('wait_for_function', 
                                       '''() => { 
                                           // Wait for products or ensure page is fully loaded
                                           const hasMainProducts = document.querySelectorAll("[data-testid='product-tile']").length > 1;
                                           const hasArticles = document.querySelectorAll("article").length > 0;
                                           const hasLoaded = document.readyState === "complete";
                                           return hasMainProducts || (hasArticles && hasLoaded);
                                       }''', 
                                       timeout=8000),
                            # Extra wait for dynamic content if needed
                            PageMethod('wait_for_timeout', 500)
                        ]
                    },
                    errback=self.errback
                )
        else:
            # Get all categories - scrape entire site
            yield Request(
                self.base_url,
                callback=self.parse_categories,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_load_state', 'domcontentloaded'),
                        PageMethod('wait_for_function', 
                                   '''() => { 
                                       const hasCategories = document.querySelectorAll('a[href*="/catalog/"], a[href*="/category/"]').length > 5;
                                       const hasLoaded = document.readyState === "complete";
                                       return hasCategories || hasLoaded;
                                   }''', 
                                   timeout=8000),
                        PageMethod('wait_for_timeout', 500)
                    ]
                }
            )
    
    def parse_search_results(self, response):
        """Parse search results page"""
        self.logger.info(f"Parsing search results from {response.url}")
        
        # Print response body length and some content
        self.logger.info(f"Response body length: {len(response.body)}")
        
        # Extract category information from URL or page
        category_info = self.extract_category_info(response)
        
        # Check if page has products loaded
        has_products_container = response.css('[class*="search"], [class*="product"], [class*="catalog"]')
        
        # Try multiple modern selectors for Metro/Zakaz
        selectors_to_try = [
            '[data-testid="product-tile"]',           # Main product grid
            '[class*="ProductTile"]',
            '[class*="product-tile"]', 
            '[class*="ProductCard"]',
            '[class*="product-card"]',
            '[class*="SearchResultItem"]',
            'article[class*="product"]',               # Product articles
            'article',                                 # Generic articles
            '.goods-tile',
            '.product-item'
        ]
        
        products = []
        for selector in selectors_to_try:
            products = response.css(selector)
            if products:
                self.logger.info(f"Found {len(products)} products with selector: {selector}")
                # If we found article elements but they seem to be non-product content, 
                # try to see if there are actual product tiles we missed
                if selector == 'article' and len(products) == 1:
                    # Check if we missed product tiles due to dynamic loading
                    product_tiles = response.css('[data-testid="product-tile"]')
                    if product_tiles:
                        self.logger.info(f"Found {len(product_tiles)} product tiles after re-checking")
                        products = product_tiles
                        selector = '[data-testid="product-tile"]'
                break
        
        if not products:
            self.logger.warning(f"No products found at {response.url}")
            return
        
        for product in products:
            item = ProductItem()
            
            # Extract product name - Metro uses specific patterns
            name_selectors = [
                '[data-testid="product-tile-title"]::text',
                '[class*="Title"]::text',
                '[class*="title"]::text',
                '[class*="name"]::text',
                'h3::text',
                'h4::text',
                'a::text',
                'img::attr(alt)'  # Sometimes name is in img alt
            ]
            
            name = None
            for selector in name_selectors:
                name_candidates = product.css(selector).getall()
                if name_candidates:
                    # Take first non-empty candidate
                    for candidate in name_candidates:
                        if candidate and candidate.strip():
                            name = candidate.strip()
                            break
                    if name:
                        break
            
            if name:
                item['name'] = name
            else:
                continue
            
            # Extract price - Metro has complex pricing
            price_selectors = [
                '[data-testid="product-tile-price"]::text',
                '[class*="Price"]:not([class*="Old"]) span::text',
                '[class*="price"]:not([class*="old"])::text',
                '[class*="cost"]::text',
                '.price::text',
                'span[class*="price"]::text'
            ]
            
            price_text = None
            for selector in price_selectors:
                price_elements = product.css(selector).getall()
                if price_elements:
                    # Join all price parts (sometimes split across spans)
                    price_text = ''.join(price_elements)
                    break
            
            if price_text:
                extracted_price = self.extract_price(price_text)
                if extracted_price:
                    item['price'] = extracted_price
            else:
                # Fallback: search for price patterns in all text
                all_text = ' '.join(product.css('::text').getall())
                extracted_price = self.extract_price(all_text)
                if extracted_price:
                    item['price'] = extracted_price
            
            # Note: original_price and brand fields removed from database
            
            # Extract URL
            product_url = product.css('a::attr(href)').get()
            if product_url:
                item['store_url'] = urljoin(self.base_url, product_url)
            
            # Extract image
            image_selectors = [
                '[data-testid="product-tile-image"] img::attr(src)',
                'img::attr(src)',
                'img::attr(data-src)'
            ]
            
            for selector in image_selectors:
                image_url = product.css(selector).get()
                if image_url:
                    item['image_url'] = urljoin(self.base_url, image_url)
                    break
            
            # Note: brand field removed from database
            
            # Note: availability fields removed from database schema
            
            # Set store info
            item['store'] = 'Metro'
            item['scraped_at'] = datetime.now().isoformat()
            
            # Add category information
            if category_info:
                item['category'] = category_info.get('category')
                item['subcategory'] = category_info.get('subcategory')
            
            if item.get('name') and item.get('price'):
                yield item
        
        # Simple pagination using page numbers
        current_page = response.meta.get('page', 1)
        base_url = response.meta.get('base_url') or response.url.split('?')[0]
        
        # Check if we were redirected to first page (Metro specific behavior)
        # Metro redirects to first page instead of returning 404
        actual_url = response.url.split('?')[0]
        if current_page > 1 and actual_url == base_url and 'page=' not in response.url:
            self.logger.info(f"Pagination ended at page {current_page} - redirected to first page")
            return
        
        # Also check if we got the same products as the first page
        first_page_products = response.meta.get('first_page_products', set())
        current_products = set()
        
        if current_page == 1 and products:
            # Save first page product names for comparison
            for product in products[:5]:  # Check first 5 products
                name = product.css('[data-testid="product-tile-title"]::text').get()
                if name:
                    current_products.add(name.strip())
        
        # Only continue if we found products on this page
        if products:
            # Request next page
            next_page = current_page + 1
            
            # Build next page URL
            if '?' in base_url:
                next_url = f"{base_url}&page={next_page}"
            else:
                next_url = f"{base_url}?page={next_page}"
            
            yield Request(
                next_url,
                callback=self.parse_search_results,
                meta={
                    'page': next_page,
                    'base_url': base_url,
                    'category': response.meta.get('category'),
                    'subcategory': response.meta.get('subcategory'),
                    'first_page_products': current_products if current_page == 1 else first_page_products,
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_load_state', 'domcontentloaded'),
                        PageMethod('wait_for_function', 
                                   '''() => { 
                                       const hasMainProducts = document.querySelectorAll("[data-testid='product-tile']").length > 1;
                                       const hasArticles = document.querySelectorAll("article").length > 0;
                                       const hasLoaded = document.readyState === "complete";
                                       return hasMainProducts || (hasArticles && hasLoaded);
                                   }''', 
                                   timeout=8000),
                        PageMethod('wait_for_timeout', 500)
                    ]
                },
                errback=self.handle_pagination_404
            )
        else:
            self.logger.info(f"No products found on page {current_page}, stopping pagination")
    
    def parse_categories(self, response):
        """Parse main page for category links - comprehensive site scraping"""
        self.logger.info(f"Discovering categories from {response.url}")
        
        # Comprehensive category selectors for Metro/Zakaz
        category_selectors = [
            # Metro-specific navigation
            'a[href*="/uk/categories/"]::attr(href)',
            'a[href*="/categories/"]::attr(href)',
            'a[href*="/uk/catalog/"]::attr(href)',
            'a[href*="/catalog/"]::attr(href)',
            
            # Main navigation patterns
            '[class*="menu"] a[href*="/categories/"]::attr(href)',
            '[class*="nav"] a[href*="/categories/"]::attr(href)',
            '[class*="category"] a::attr(href)',
            
            # Look for links that seem to be categories
            'a[href*="fruits"]::attr(href)',
            'a[href*="vegetables"]::attr(href)', 
            'a[href*="meat"]::attr(href)',
            'a[href*="dairy"]::attr(href)',
            'a[href*="bread"]::attr(href)',
            'a[href*="beverages"]::attr(href)',
            
            # Footer and sitemap
            'footer a[href*="/categories/"]::attr(href)',
            'footer a[href*="/catalog/"]::attr(href)'
        ]
        
        categories = set()  # Use set to avoid duplicates
        
        for selector in category_selectors:
            found_categories = response.css(selector).getall()
            if found_categories:
                categories.update(found_categories)
                self.logger.info(f"Found {len(found_categories)} categories with selector: {selector[:50]}")
        
        # Filter and process categories
        processed_categories = set()
        for category_url in categories:
            if category_url and ('catalog' in category_url or 'categories' in category_url or 
                                'fruits' in category_url or 'vegetables' in category_url or
                                'meat' in category_url or 'dairy' in category_url or
                                'bread' in category_url or 'beverages' in category_url):
                # Clean and normalize URL
                clean_url = category_url.strip()
                if clean_url.startswith('/'):
                    clean_url = urljoin(self.base_url, clean_url)
                
                # Avoid duplicate similar URLs and ensure it's a valid Metro URL
                # Filter out problematic URL patterns that cause 404s
                skip_patterns = [
                    '#', 'javascript:', 'mailto:', 'tel:',
                    'own-brands=', 'filter=', 'sort=', 'brand=',
                    'custom-categories/promotions', 'utm_', 'ref=',
                    '?page=', '&page=', 'redirect='
                ]
                
                if (clean_url not in processed_categories and 
                    'metro.zakaz.ua' in clean_url and
                    not any(skip in clean_url for skip in skip_patterns) and
                    len(clean_url) < 200):  # Avoid extremely long URLs
                    processed_categories.add(clean_url)
                    
                    # Extract category name from URL
                    url_parts = clean_url.split('/')
                    category_name = None
                    if 'catalog' in clean_url or 'categories' in clean_url:
                        for i, part in enumerate(url_parts):
                            if part in ['catalog', 'categories'] and i + 1 < len(url_parts):
                                category_name = url_parts[i + 1].replace('-', ' ').title()
                                break
                    
                    # Yield category item to save to database
                    if category_name:
                        yield {
                            '_type': 'category',
                            'store': 'Metro',
                            'category': category_name,
                            'subcategory': None,
                            'category_url': clean_url
                        }
                    
                    yield Request(
                        clean_url,
                        callback=self.parse_category_page,
                        errback=self.handle_category_error,  # Handle 404s gracefully
                        meta={
                            'playwright': True,
                            'playwright_page_methods': [
                                PageMethod('wait_for_load_state', 'domcontentloaded'),
                                PageMethod('wait_for_function', 
                                           '''() => { 
                                               const hasMainProducts = document.querySelectorAll("[data-testid='product-tile']").length > 1;
                                               const hasSubcategories = document.querySelectorAll('a[href*="/catalog/"]').length > 3;
                                               const hasLoaded = document.readyState === "complete";
                                               return hasMainProducts || hasSubcategories || hasLoaded;
                                           }''', 
                                           timeout=8000),
                                PageMethod('wait_for_timeout', 500)
                            ]
                        }
                    )
        
        self.logger.info(f"Total unique categories found: {len(processed_categories)}")
        
        # Also try to find a sitemap or category listing page
        sitemap_selectors = [
            'a[href*="sitemap"]::attr(href)',
            'a[href*="categories"]::attr(href)',
            'a[href*="catalog"]::attr(href)'
        ]
        
        for selector in sitemap_selectors:
            sitemap_urls = response.css(selector).getall()
            for sitemap_url in sitemap_urls[:3]:  # Limit to avoid too many requests
                if sitemap_url and sitemap_url not in processed_categories:
                    yield Request(
                        urljoin(self.base_url, sitemap_url),
                        callback=self.parse_categories,  # Parse as categories page
                        meta={
                            'playwright': True,
                            'playwright_page_methods': [
                                PageMethod('wait_for_load_state', 'domcontentloaded'),
                                PageMethod('wait_for_timeout', 500)
                            ]
                        }
                    )
    
    def parse_category_page(self, response):
        """Parse products from category page and discover subcategories"""
        self.logger.info(f"Processing category page: {response.url}")
        
        # Parse products from this category page
        yield from self.parse_search_results(response)
        
        # Look for subcategories on this page
        subcategory_selectors = [
            # Subcategory navigation within category
            '[class*="SubCategory"] a[href*="/catalog/"]::attr(href)',
            '[class*="subcategory"] a[href*="/catalog/"]::attr(href)',
            '.category-nav a[href*="/catalog/"]::attr(href)',
            
            # Breadcrumb-style subcategories
            '[class*="breadcrumb"] a[href*="/catalog/"]::attr(href)',
            
            # Generic subcategory links
            'a[href*="/catalog/"][href*="' + response.url.split('/')[-1] + '"]::attr(href)',
            
            # Any catalog links that seem to be deeper in hierarchy
            'a[href*="/catalog/"]::attr(href)'
        ]
        
        found_subcategories = set()
        for selector in subcategory_selectors:
            subcategories = response.css(selector).getall()
            for subcat_url in subcategories:
                if subcat_url and subcat_url not in found_subcategories:
                    clean_url = subcat_url.strip()
                    if clean_url.startswith('/'):
                        clean_url = urljoin(self.base_url, clean_url)
                    
                    # Only process if it's different from current page and seems like a valid category
                    if (clean_url != response.url and 
                        len(clean_url.split('/')) > len(response.url.split('/')) and
                        ('catalog' in clean_url or 'category' in clean_url)):
                        
                        found_subcategories.add(clean_url)
        
        # Yield requests for subcategories (limit to avoid infinite recursion)
        if found_subcategories and len(found_subcategories) <= 50:  # Reasonable limit
            self.logger.info(f"Found {len(found_subcategories)} subcategories in {response.url}")
            for subcat_url in found_subcategories:
                yield Request(
                    subcat_url,
                    callback=self.parse_category_page,
                    meta={
                        'playwright': True,
                        'playwright_page_methods': [
                            PageMethod('wait_for_load_state', 'domcontentloaded'),
                            PageMethod('wait_for_function', 
                                       '''() => { 
                                           const hasMainProducts = document.querySelectorAll("[data-testid='product-tile']").length > 1;
                                           const hasSubcategories = document.querySelectorAll('a[href*="/catalog/"]').length > 3;
                                           const hasLoaded = document.readyState === "complete";
                                           return hasMainProducts || hasSubcategories || hasLoaded;
                                       }''', 
                                       timeout=8000),
                            PageMethod('wait_for_timeout', 500)
                        ]
                    }
                )
    
    def extract_category_info(self, response):
        """Extract category information from UI elements first, then fallback to URL"""
        category_info = {}
        url = response.url
        
        try:
            # PRIORITY 1: Extract category from UI breadcrumbs (better user-friendly names)
            breadcrumb_selectors = [
                '[data-marker="Disabled Breadcrumb"][itemprop="title"]::text',  # Metro-specific breadcrumbs
                '[data-marker="Disabled Breadcrumb"]::text', 
                '[itemprop="title"]::text',
                '[class*="breadcrumb"] a::text',
                '[class*="nav-breadcrumb"] a::text',
                'nav a::text',
                '.category-path a::text'
            ]
            
            for selector in breadcrumb_selectors:
                breadcrumbs = response.css(selector).getall()
                if breadcrumbs:
                    # Process all breadcrumbs to extract category hierarchy
                    valid_crumbs = []
                    for crumb in breadcrumbs:
                        crumb = crumb.strip()
                        if crumb and crumb.lower() not in ['home', 'головна', 'главная', 'metro', 'zakaz', '']:
                            valid_crumbs.append(crumb)
                    
                    if valid_crumbs:
                        category_info['category'] = valid_crumbs[0]  # First valid breadcrumb as main category
                        if len(valid_crumbs) > 1:
                            category_info['subcategory'] = valid_crumbs[1]  # Second as subcategory
                        break
            
            # PRIORITY 2: Extract from page title if no breadcrumbs found
            if not category_info.get('category'):
                page_title = response.css('title::text').get()
                if page_title:
                    # Look for category patterns in title
                    title_clean = page_title.strip()
                    # Common patterns: "Category - Metro" or "Category | Metro" 
                    title_parts = [part.strip() for part in title_clean.replace('|', '-').split('-')]
                    if len(title_parts) > 1:
                        potential_category = title_parts[0]
                        # Filter out generic terms
                        if (potential_category and len(potential_category) < 50 and 
                            potential_category.lower() not in ['metro', 'zakaz', 'search', 'пошук']):
                            category_info['category'] = potential_category
            
            # PRIORITY 3: Fallback to URL structure if UI extraction failed
            if not category_info.get('category'):
                if '/categories/' in url:
                    # URL pattern: /uk/categories/category-name/subcategory-name/
                    url_parts = url.split('/categories/')[-1].split('/')
                    if url_parts and url_parts[0]:
                        # Clean category name from URL
                        category = url_parts[0].replace('-', ' ').replace('_', ' ').title()
                        category_info['category'] = category
                        
                        # Look for subcategory
                        if len(url_parts) > 1 and url_parts[1]:
                            subcategory = url_parts[1].replace('-', ' ').replace('_', ' ').title()
                            category_info['subcategory'] = subcategory
                
                elif '/catalog/' in url:
                    # URL pattern: /uk/catalog/category-name/subcategory-name/
                    url_parts = url.split('/catalog/')[-1].split('/')
                    if url_parts and url_parts[0]:
                        category = url_parts[0].replace('-', ' ').replace('_', ' ').title()
                        category_info['category'] = category
                        
                        if len(url_parts) > 1 and url_parts[1]:
                            subcategory = url_parts[1].replace('-', ' ').replace('_', ' ').title()
                            category_info['subcategory'] = subcategory
                
                elif '/search/' in url and self.product_name:
                    # For search results, use search term as category
                    category_info['category'] = f"Search: {self.product_name}"
            
            # PRIORITY 4: Try to extract main category from page heading/title elements
            if not category_info.get('category'):
                heading_selectors = [
                    'h1::text',
                    '.page-title::text',
                    '.category-title::text',
                    '[class*="title"]:first-of-type::text'
                ]
                
                for selector in heading_selectors:
                    heading = response.css(selector).get()
                    if heading:
                        heading = heading.strip()
                        if (heading and len(heading) < 100 and 
                            heading.lower() not in ['metro', 'zakaz', 'products', 'товари']):
                            category_info['category'] = heading
                            break
            
            return category_info
            
        except Exception as e:
            self.logger.error(f"Error extracting category info: {e}")
            return {}

    def extract_price(self, price_text):
        """Extract numeric price from text - Metro specific handling"""
        if not price_text:
            return None
        
        # Remove currency symbols and normalize
        price_text = re.sub(r'[₴грнUAH\s]', '', price_text.strip())
        price_text = price_text.replace(',', '.')
        
        # Extract numeric value - handle wholesale prices (larger numbers)
        matches = re.findall(r'(\d+\.?\d*)', price_text)
        if matches:
            # If multiple numbers found, take the first reasonable price
            for match in matches:
                price = float(match)
                # Filter out unreasonably large prices (likely wholesale)
                if 0.1 <= price <= 10000:
                    return price
            # If no reasonable price found, return the first one
            return float(matches[0])
        
        return None
    
    def handle_category_error(self, failure):
        """Handle errors for category page requests (404s, etc.)"""
        if hasattr(failure, 'response') and failure.response:
            status = failure.response.status
            if status == 404:
                self.logger.warning(f"Category page not found (404): {failure.request.url}")
            else:
                self.logger.error(f"Category page error ({status}): {failure.request.url}")
        else:
            self.logger.error(f"Category request failed: {failure.request.url} - {failure.value}")
    
    def handle_pagination_404(self, failure):
        """Handle 404 errors during pagination - this means we've reached the end."""
        if hasattr(failure.value, 'response') and failure.value.response:
            response = failure.value.response
            if response.status == 404:
                page = failure.request.meta.get('page', 'unknown')
                category = failure.request.meta.get('category', 'unknown')
                self.logger.info(f"Pagination ended at page {page} for category '{category}' (404 response)")
                return
        
        # For other errors, use default error handling
        self.errback(failure)
    
    def errback(self, failure) -> None:
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(f"Error: {failure.value}")