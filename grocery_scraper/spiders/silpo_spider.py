import scrapy
from scrapy.http import Request
from scrapy_playwright.page import PageMethod
from .base_spider import BaseGrocerySpider
from urllib.parse import urljoin, quote
import re
from datetime import datetime
from typing import Any


class SilpoSpider(scrapy.Spider):
    name = "silpo"
    allowed_domains = ["silpo.ua"]
    base_url = "https://silpo.ua"
    
    
    def __init__(self, product_name=None, start_url=None, *args, **kwargs):
        super(SilpoSpider, self).__init__(*args, **kwargs)
        self.product_name = product_name
        self.start_url = start_url
        self.processed_categories = set()  # Track processed categories to avoid duplicates
        self.requested_categories = set()  # Track requested category URLs
        self.failed_urls = set()  # Track URLs that failed after all retries
    
    
    @staticmethod
    def get_page_methods(fast_mode=False):
        """Get optimized page methods for fast loading"""
        if fast_mode:
            # Minimal page methods for problematic pages and retries
            return [
                PageMethod('wait_for_load_state', 'domcontentloaded'),
                PageMethod('wait_for_timeout', 500),  # Faster wait
                PageMethod('wait_for_function',
                           '''() => {
                               // Quick check if content loaded or page ready
                               const hasProducts = document.querySelector('[data-autotestid="shop-silpo-product-card"]') !== null;
                               const hasCategories = document.querySelector('[data-autotestid="menu-categories__link"]') !== null;
                               const hasContent = document.body.innerText.length > 100;
                               return hasProducts || hasCategories || hasContent || document.readyState === 'complete';
                           }''', timeout=4000),  # Faster timeout
            ]

        return [
            PageMethod('wait_for_load_state', 'domcontentloaded'),
            PageMethod('wait_for_timeout', 300),  # Faster wait
            PageMethod('wait_for_function',
                       '''() => {
                           // Quick check if essential content loaded
                           const hasProducts = document.querySelector('[data-autotestid="shop-silpo-product-card"]') !== null;
                           const hasCategories = document.querySelector('[data-autotestid="menu-categories__link"]') !== null;
                           const hasContent = document.body.innerText.length > 100;
                           const isEmpty = document.querySelector('.empty-results') !== null;
                           return hasProducts || hasCategories || hasContent || isEmpty || document.readyState === 'complete';
                       }''', timeout=5000),  # Balanced timeout
        ]

    async def start(self):
        """New async start method for Scrapy 2.13+"""
        for request in self.start_requests():
            yield request

    def start_requests(self):
        if self.start_url:
            # Direct URL for testing
            yield Request(
                self.start_url,
                callback=self.parse_search_results,
                meta={
                    'playwright': True,
                    'playwright_wait_for_timeout': 12000,  # Optimized timeout
                    'playwright_page_methods': self.get_page_methods()
                },
                errback=self.errback
            )
        elif self.product_name:
            # Try updated search URL patterns based on site structure
            search_patterns = [
                f'/search?q={quote(self.product_name)}',
                f'/ua/search?q={quote(self.product_name)}',
            ]

            for pattern in search_patterns:
                url = urljoin(self.base_url, pattern)
                yield Request(
                    url,
                    callback=self.parse_search_results,
                    meta={
                        'search_url': url,
                        'playwright': True,
                        'playwright_wait_for_timeout': 12000,  # Optimized timeout
                        'playwright_page_methods': self.get_page_methods()
                    },
                    errback=self.errback
                )
        else:
            # Get all categories
            yield Request(
                self.base_url,
                callback=self.parse_categories,
                meta={
                    'playwright': True,
                    'playwright_wait_for_timeout': 20000,  # Fast timeout
                    'playwright_page_methods': [
                        PageMethod('wait_for_load_state', 'domcontentloaded'),
                        PageMethod('wait_for_timeout', 4000),
                        PageMethod('click', '[data-autotestid="shop-silpo-category-menu-button"]'),
                        PageMethod('wait_for_timeout', 1000),
                    ]
                }
            )
    
    def parse_search_results(self, response):
        self.logger.info(f"Parsing search results from {response.url}")

        # Check response status and content type
        if response.status != 200:
            self.logger.warning(f"Non-200 response ({response.status}) from {response.url}")
            return

        # Check if response is text-based
        content_type = response.headers.get('content-type', b'').decode('utf-8')
        if 'text/html' not in content_type:
            self.logger.warning(f"Non-HTML response from {response.url}, content-type: {content_type}")
            return

        # Extract category information
        category_info = self.extract_category_info(response)

        try:
            # Check if we have products - using Silpo specific selectors
            products_container = response.css('.product-card-list')
            if products_container:
                # Look for products within the container
                products = products_container.xpath("//shop-silpo-common-product-card[@data-autotestid='shop-silpo-product-card']")
            else:
                # Fallback: look for products globally
                products = response.xpath("//shop-silpo-common-product-card[@data-autotestid='shop-silpo-product-card']")

            if not products:
                # Try CSS selectors
                products = response.css('[data-autotestid="shop-silpo-product-card"]')

            if not products:
                # Try alternative selectors
                products = response.css('.product-card-list__item, .product-card')

        except (AttributeError, ValueError, TypeError) as e:
            self.logger.error(f"Error parsing CSS selectors from {response.url}: {e}")
            return

        if not products:
            return

        # Track seen products to avoid duplicates
        seen_products = set()

        for product in products:
            item = ProductItem()

            # Extract product name - using Silpo specific selectors
            name = product.css('.product-card__title::text').get()
            if not name:
                # Try aria-label
                aria_label = product.css('::attr(aria-label)').get()
                if aria_label:
                    name_match = re.search(r'товар\s+([^,]+)', aria_label, re.I)
                    if name_match:
                        name = name_match.group(1).strip()

            if not name:
                continue

            item['name'] = name.strip()

            # Create unique identifier for deduplication
            product_url = product.css('.product-card a::attr(href), a::attr(href)').get()

            # Use name + URL as unique key, fallback to just name if no URL
            unique_key = f"{item['name']}|{product_url or ''}"
            if unique_key in seen_products:
                continue
            seen_products.add(unique_key)

            # Extract price - using Silpo specific selectors
            price_text = product.css('.product-card-price__displayPrice::text').get()
            if not price_text:
                # Try aria-label for price
                aria_label = product.css('::attr(aria-label)').get()
                if aria_label:
                    price_match = re.search(r'ціна\s+([\d.,]+)', aria_label, re.I)
                    if price_match:
                        price_text = price_match.group(1)

            if price_text:
                extracted_price = self.extract_price(price_text)
                if extracted_price:
                    item['price'] = extracted_price
                else:
                    pass
            else:
                pass

            # Set URLs
            if product_url:
                item['store_url'] = urljoin(self.base_url, product_url)

            # Save the category URL where this product was found
            item['url'] = response.url

            # Extract image - using Silpo specific selector
            image_url = product.css('[data-autotestid="img"]::attr(src)').get()
            if not image_url:
                image_url = product.css('img::attr(src)').get()

            if image_url:
                item['image_url'] = urljoin(self.base_url, image_url)

            # Set store info
            item['store'] = 'Silpo'
            item['scraped_at'] = datetime.now().isoformat()

            # Add category information
            if category_info:
                item['category'] = category_info.get('category')
                item['subcategory'] = category_info.get('subcategory')

            yield item

        # Simple pagination - get last page number
        current_page = response.meta.get('page_num', 1)

        # Extract base URL without page parameter for tracking
        base_url = response.url.split('?')[0]

        # Get last page number from pagination
        pag_block = response.css('.pagination')

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
                else:
                    last_page_num = None
            else:
                last_page_num = None
        else:
            last_page_num = None

        # If we found a last page number and this is page 1, generate all page URLs
        if last_page_num and current_page == 1 and last_page_num > 1:
            # Check if we already processed this category's pagination
            if base_url in self.processed_categories:
                return

            self.processed_categories.add(base_url)
            self.logger.info(f"Generating {last_page_num - 1} pagination requests for {base_url}")

            # Generate all page URLs from 2 to last_page_num
            for page_num in range(2, last_page_num + 1):
                if '?page=' in response.url:
                    page_url = re.sub(r'page=\d+', f'page={page_num}', response.url)
                else:
                    separator = '&' if '?' in response.url else '?'
                    page_url = f"{response.url}{separator}page={page_num}"

                yield Request(
                    page_url,
                    callback=self.parse_search_results,
                    meta={
                        'playwright': True,
                        'playwright_wait_for_timeout': 12000,  # Optimized timeout
                        'page_num': page_num,
                        'playwright_page_methods': self.get_page_methods()
                    }
                )
    
    def parse_categories(self, response):
        """Parse main page for category links"""
        self.logger.info(f"Parsing categories from {response.url}")

        # Single selector for category discovery - Silpo specific
        categories = response.css('a[data-autotestid="menu-categories__link"]::attr(href)').getall()
        self.logger.info(f"Found {len(categories)} category links with Silpo selector")
        
        if not categories:
            # Try alternative selectors if main selector fails
            alt_selectors = [
                "//a[contains(@class, 'menu')]/@href",
                "//div[contains(@class, 'catalog')]//a/@href",
                "//nav//a/@href"
            ]
            for alt_xpath in alt_selectors:
                alt_categories = response.xpath(alt_xpath).getall()
                if alt_categories:
                    categories.extend(alt_categories)
                    break

        # Deduplicate categories
        categories = list(set(categories))
        self.logger.info(f"Total unique category URLs: {len(categories)}")

        # Filter for actual category URLs
        category_count = 0
        max_categories = 500  # Limit to prevent following too many URLs

        for category_url in categories:
            if not category_url:
                continue

            # Skip certain patterns that are not product categories
            skip_patterns = [
                'search', 'filter', 'sort', 'page=', '#', 'javascript:', 'mailto:', 'tel:',
                'quiz', 'work', 'anti-corruption', 'promotion', 'contact', 'about',
                'delivery', 'payment', 'help', 'news', 'blog', 'career', 'partnership',
                'stores', 'shops', 'locations', 'map', 'branches', 'addresses',
                'login', 'register', 'account', 'profile', 'cart', 'checkout',
                'privacy', 'terms', 'conditions', 'policy', 'faq', 'support',
                'novelty', 'new', 'sale', 'discount', 'action', 'special',
                'business', 'biznes', 'shvydka-dostavka', 'dostavka-dlya', 'optom',
                'wholesale', 'corporate', 'b2b', 'franchise', 'vacancy',
                'gotovi-stravy-i-kulinariia', 'spetsialni-propozytsii'
            ]
            
            # Skip individual product URLs
            product_patterns = [
                '-g', '-kg', '-ml', '-l', '-sm', '-250g', '-500g', '-1kg', '-100ml', '-250ml',
                'product-', 'item-', '-z-', '-dlya-', '-na-', '-vid-', '-ta-', '-i-'
            ]
            
            if any(skip in category_url for skip in skip_patterns):
                continue
                
            # Additional check: skip if URL looks like a specific product
            url_parts = category_url.lower().split('/')
            is_product = False
            for part in url_parts:
                if any(pattern in part for pattern in product_patterns) and len(part) > 20:
                    is_product = True
                    break
            
            if is_product:
                continue

            # Check if it's a relative URL starting with /
            if category_url.startswith('/') and len(category_url) > 1:
                full_url = urljoin(self.base_url, category_url)

                # Skip if it's the same as base URL or language switch
                if full_url.rstrip('/') == self.base_url.rstrip('/'):
                    continue
                if category_url in ['/ua', '/uk', '/ru', '/en']:
                    continue

                # Skip if we already requested this category
                if full_url in self.requested_categories:
                    continue

                self.requested_categories.add(full_url)
                category_count += 1

                # Limit the number of URLs to follow
                if category_count > max_categories:
                    break

                # Check if this is a problematic page that needs fast mode
                problematic_pages = [
                    'ponchiki', 'zamorozhene-tisto', 'cukor', 'toniruyuschie-sredstva-dlya-volos',
                    'optovi-zakupivli', 'suppliers', 'finansova-zvitnist', 'smachna_robota',
                    'experience-of-your-future', 'rasprodazha', 'tovari-dlya-grizuniv',
                    'tovari-dlya-ptahiv', 'tovari-dlya-kotiv', 'tovari-dlya-ryb', 'akvariumistika',
                    'varuscafe', 'vigidni-propozitsiyi', 'kuhonni-priladdya', 'tovari-dlya-domu'
                ]

                use_fast_mode = any(prob in full_url for prob in problematic_pages)
                timeout = 8000 if use_fast_mode else 12000

                yield Request(
                    full_url,
                    callback=self.parse_category_page,
                    meta={
                        'playwright': True,
                        'playwright_wait_for_timeout': timeout,
                        'playwright_page_methods': self.get_page_methods(fast_mode=use_fast_mode)
                    }
                )

        self.logger.info(f"Following {category_count} category URLs")
    
    def parse_category_page(self, response):
        """Parse products from category page"""
        self.logger.info(f"Parsing category page: {response.url}")
        # Reuse the same parsing logic as search results
        yield from self.parse_search_results(response)
    
    
    def extract_category_info(self, response):
        """Extract category information from breadcrumbs or URL"""
        category_info = {}

        try:
            # First try to get category from breadcrumbs - Silpo specific selector
            breadcrumb_selectors = [
                '.breadcrumbs-list__item.breadcrumbs-list__item--active.ng-star-inserted::text',
                '.breadcrumbs a::text',
                '.breadcrumb a::text',
                'nav.breadcrumbs a::text',
                '[class*="breadcrumb"] a::text'
            ]

            breadcrumbs = []
            for selector in breadcrumb_selectors:
                breadcrumbs = response.css(selector).getall()
                if breadcrumbs:
                    break

            if breadcrumbs:
                # Clean and filter breadcrumbs
                valid_crumbs = []
                for crumb in breadcrumbs:
                    crumb = crumb.strip()
                    if crumb and crumb.lower() not in ['home', 'головна', 'silpo', 'сільпо', '']:
                        valid_crumbs.append(crumb)

                if valid_crumbs:
                    # Take the last two as category and subcategory
                    if len(valid_crumbs) >= 2:
                        category_info['category'] = valid_crumbs[-2]
                        category_info['subcategory'] = valid_crumbs[-1]
                    else:
                        category_info['category'] = valid_crumbs[-1]

            # If no breadcrumbs, try to extract from URL
            if not category_info:
                # Clean URL by removing query parameters first
                clean_url = response.url.split('?')[0]
                url_parts = clean_url.split('/')
                # Remove empty parts and domain
                url_parts = [p for p in url_parts[3:] if p]

                if url_parts:
                    # Decode URL encoded parts
                    from urllib.parse import unquote
                    url_parts = [unquote(p) for p in url_parts]

                    # Convert URL slug to readable format
                    if url_parts:
                        category_name = url_parts[-1].replace('-', ' ').title()
                        category_info['category'] = category_name

                        if len(url_parts) > 1:
                            parent_category = url_parts[-2].replace('-', ' ').title()
                            category_info['subcategory'] = category_name
                            category_info['category'] = parent_category

            # Always include the category URL for reference
            category_info['category_url'] = response.url

        except (AttributeError, IndexError, ValueError, KeyError) as e:
            self.logger.error(f"Error extracting category: {e}")

        return category_info
    
    def extract_price(self, price_text):
        """Extract numeric price from text"""
        if not price_text:
            return None

        # Remove thousand separators (spaces) but keep decimal points
        price_text = price_text.replace(' ', '')

        # Find all price patterns (digits with optional decimal point)
        price_patterns = re.findall(r'(\d+(?:\.\d+)?)', price_text)

        if price_patterns:
            # Get the last price (usually the current/discounted price)
            last_price = price_patterns[-1]
            try:
                return float(last_price)
            except ValueError:
                self.logger.warning(f"Could not convert price to float: {last_price}")
                return None

        return None
    
    def errback(self, failure: Any) -> None:
        # Ignore errors for non-essential URLs
        ignore_patterns = ['/novelty', '/new', '/sale', '/discount', '/action', '/special', '/promotion', '/own-tm',
                           '/optovi-zakupivli', '/suppliers', '/finansova-zvitnist', '/smachna_robota',
                           '/experience-of-your-future',
                           '/rasprodazha', '/varuscafe', '/vigidni-propozitsiyi-vid-bankiv', '/tenders', '/advertisers',
                           '/partners', '/business', '/biznes', '/shvydka-dostavka', '/dostavka-dlya', '/optom',
                           '/wholesale', '/corporate', '/b2b', '/franchise', '/career', '/vacancy']
        if any(pattern in failure.request.url for pattern in ignore_patterns):
            return

        # Log the error and add to failed URLs for tracking
        error_type = type(failure.value).__name__
        url = failure.request.url
        
        self.logger.error(f"Request failed: {url}")
        self.logger.error(f"Error: {error_type} - {failure.value}")
        
        # Track failed URLs for final reporting
        self.failed_urls.add(url)
    
    def closed(self):
        """Called when spider is closed - do final retry of failed URLs"""
        if self.failed_urls:
            self.logger.info(f"Spider closed. {len(self.failed_urls)} URLs failed after all retries:")
            for url in self.failed_urls:
                self.logger.info(f"  - {url}")
            
            # You can uncomment this to enable automatic final retry
            # self.logger.info("Starting final retry phase...")
            # for url in self.failed_urls:
            #     yield Request(
            #         url=url,
            #         callback=self.parse_search_results,
            #         meta={
            #             'playwright': True,
            #             'playwright_wait_for_timeout': 60000,  # Maximum timeout
            #             'playwright_page_methods': [
            #                 PageMethod('wait_for_load_state', 'networkidle'),  # Wait for network idle
            #                 PageMethod('wait_for_timeout', 5000),  # Extra wait
            #             ] + self.get_page_methods(fast_mode=True),
            #             'final_retry': True
            #         },
            #         dont_filter=True,
            #         priority=1
            #     )
        else:
            self.logger.info("Spider completed successfully with no failed URLs!")