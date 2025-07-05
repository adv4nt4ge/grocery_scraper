# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import NotConfigured
from playwright.sync_api import sync_playwright
import logging
import asyncio
from urllib.parse import urljoin


class PlaywrightMiddleware:
    """Middleware for handling JavaScript rendering with Playwright"""
    
    def __init__(self, browser_type='chromium', launch_options=None):
        self.browser_type = browser_type
        self.launch_options = launch_options or {'headless': True}
        self.playwright = None
        self.browser = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @classmethod
    def from_crawler(cls, crawler):
        browser_type = crawler.settings.get('PLAYWRIGHT_BROWSER_TYPE', 'chromium')
        launch_options = crawler.settings.get('PLAYWRIGHT_LAUNCH_OPTIONS', {'headless': True})
        
        middleware = cls(browser_type=browser_type, launch_options=launch_options)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware
    
    def spider_opened(self, spider):
        """Initialize Playwright when spider opens"""
        try:
            self.playwright = sync_playwright().start()
            self.browser = getattr(self.playwright, self.browser_type).launch(**self.launch_options)
            spider.logger.info(f"Playwright initialized with {self.browser_type}")
        except Exception as e:
            spider.logger.error(f"Failed to initialize Playwright: {e}")
            raise NotConfigured(f"Playwright initialization failed: {e}")
    
    def spider_closed(self, spider):
        """Clean up Playwright when spider closes"""
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            spider.logger.info("Playwright cleaned up")
        except Exception as e:
            spider.logger.error(f"Error cleaning up Playwright: {e}")
    
    def process_request(self, request, spider):
        """Process request with Playwright if meta['playwright'] is True"""
        if not request.meta.get('playwright'):
            return None
        
        if not self.browser:
            spider.logger.error("Playwright browser not initialized")
            return None
        
        page = None
        try:
            # Create new page for each request
            page = self.browser.new_page()
            
            # Set headers
            headers = {}
            if hasattr(request, 'headers'):
                for key, values in request.headers.items():
                    if values:
                        headers[key.decode()] = values[0].decode()
            
            if headers:
                page.set_extra_http_headers(headers)
            
            # Set user agent
            user_agent = headers.get('User-Agent') or request.meta.get('user_agent')
            if user_agent:
                page.set_extra_http_headers({'User-Agent': user_agent})
            
            # Navigate to URL
            spider.logger.debug(f"Loading page with Playwright: {request.url}")
            response = page.goto(
                request.url,
                wait_until='domcontentloaded',
                timeout=request.meta.get('playwright_timeout', 30000)
            )
            
            # Execute page methods if specified
            page_methods = request.meta.get('playwright_page_methods', [])
            for method_config in page_methods:
                if isinstance(method_config, dict):
                    method_name = method_config.get('method')
                    if method_name == 'wait_for_selector':
                        selector = method_config.get('selector')
                        timeout = method_config.get('timeout', 10000)
                        if selector:
                            try:
                                page.wait_for_selector(selector, timeout=timeout)
                            except Exception as e:
                                spider.logger.warning(f"Selector '{selector}' not found: {e}")
                    elif method_name == 'wait_for_load_state':
                        state = method_config.get('state', 'domcontentloaded')
                        page.wait_for_load_state(state)
                    elif method_name == 'click':
                        selector = method_config.get('selector')
                        if selector:
                            try:
                                page.click(selector)
                                page.wait_for_load_state('domcontentloaded')
                            except Exception as e:
                                spider.logger.warning(f"Could not click '{selector}': {e}")
                    elif method_name == 'scroll':
                        # Scroll to load more content
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1000)
            
            # Get page content
            html_content = page.content()
            
            # Close the page
            page.close()
            
            # Create Scrapy response
            return HtmlResponse(
                url=request.url,
                body=html_content,
                encoding='utf-8',
                request=request
            )
            
        except Exception as e:
            spider.logger.error(f"Playwright error for {request.url}: {e}")
            if page is not None:
                try:
                    page.close()
                except:
                    pass
            return None
    
    def process_response(self, request, response, spider):
        return response
    
    def process_exception(self, request, exception, spider):
        spider.logger.error(f"Exception in PlaywrightMiddleware for {request.url}: {exception}")
        return None


class PlaywrightTimeoutRetryMiddleware:
    """Middleware to handle Playwright timeout errors specifically"""
    
    def __init__(self, max_retry_times=2):
        self.max_retry_times = max_retry_times
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(max_retry_times=crawler.settings.getint('RETRY_TIMES', 2))
    
    def process_exception(self, request, exception, spider):
        # Handle Playwright timeout errors
        exception_name = type(exception).__name__
        if 'TimeoutError' in exception_name and 'playwright' in str(exception).lower():
            retry_times = request.meta.get('playwright_retry_times', 0) + 1
            
            if retry_times <= self.max_retry_times:
                spider.logger.warning(f"Playwright timeout for {request.url}, retry {retry_times}/{self.max_retry_times}")
                
                retryreq = request.copy()
                retryreq.meta['playwright_retry_times'] = retry_times
                # Increase timeout for retry
                retryreq.meta['playwright_wait_for_timeout'] = min(30000, 15000 + (retry_times * 5000))
                retryreq.dont_filter = True
                
                return retryreq
            else:
                spider.logger.error(f"Max Playwright retries exceeded for {request.url}")
        
        return None


class RetryMiddleware:
    """Enhanced retry middleware for grocery stores"""
    
    def __init__(self, max_retry_times=3, retry_http_codes=None):
        self.max_retry_times = max_retry_times
        self.retry_http_codes = retry_http_codes or [403, 500, 502, 503, 504, 408, 429]
    
    @classmethod
    def from_crawler(cls, crawler):
        max_retry_times = crawler.settings.getint('RETRY_TIMES', 3)
        retry_http_codes = crawler.settings.getlist('RETRY_HTTP_CODES', [403, 500, 502, 503, 504, 408, 429])
        return cls(max_retry_times=max_retry_times, retry_http_codes=retry_http_codes)
    
    def process_request(self, request, spider):
        # Add delay for retry requests
        retry_attempt = request.meta.get('retry_attempt', 0)
        if retry_attempt > 0:
            delay = 2 ** (retry_attempt - 1)  # Exponential backoff: 1s, 2s, 4s
            import time
            time.sleep(delay)
            spider.logger.info(f"Retry request after {delay}s delay: {request.url}")
        return None
    
    def process_response(self, request, response, spider):
        if response.status in self.retry_http_codes:
            retry_times = request.meta.get('retry_times', 0) + 1
            
            if retry_times <= self.max_retry_times:
                spider.logger.warning(f"Retrying {request.url} (attempt {retry_times}/{self.max_retry_times}) - Status: {response.status}")
                retryreq = request.copy()
                retryreq.meta['retry_times'] = retry_times
                
                # Add delay for retries
                if response.status == 429:  # Rate limited
                    retryreq.meta['download_delay'] = min(retry_times * 2, 10)
                elif response.status == 403:  # Forbidden
                    retryreq.meta['download_delay'] = retry_times * 3
                
                return retryreq
            else:
                spider.logger.error(f"Max retries exceeded for {request.url}")
        
        return response


class HeadersMiddleware:
    """Middleware for setting appropriate headers for Ukrainian grocery stores"""
    
    def process_request(self, request, spider):
        # Set headers specific to Ukrainian grocery stores
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7,ru;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Store-specific headers
        if 'silpo.ua' in request.url:
            headers.update({
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://silpo.ua',
                'Referer': 'https://silpo.ua/'
            })
        elif 'varus.ua' in request.url:
            headers.update({
                'Origin': 'https://varus.ua',
                'Referer': 'https://varus.ua/'
            })
        elif 'atbmarket.com' in request.url:
            headers.update({
                'Origin': 'https://www.atbmarket.com',
                'Referer': 'https://www.atbmarket.com/'
            })
        elif 'zakaz.ua' in request.url:
            headers.update({
                'Origin': 'https://metro.zakaz.ua',
                'Referer': 'https://metro.zakaz.ua/'
            })
        
        # Update request headers
        for key, value in headers.items():
            request.headers[key] = value
        
        return None


class GroceryScrapySpiderMiddleware:
    """Spider middleware for grocery scraping specific functionality"""
    
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s
    
    def process_spider_input(self, response, spider):
        return None
    
    def process_spider_output(self, response, result, spider):
        # Log statistics about scraped items
        items_count = 0
        requests_count = 0
        
        for item in result:
            if hasattr(item, '__getitem__'):  # Is an item
                items_count += 1
            else:  # Is a request
                requests_count += 1
            yield item
        
        if items_count > 0 or requests_count > 0:
            spider.logger.debug(f"Page {response.url}: {items_count} items, {requests_count} requests")
    
    def process_spider_exception(self, response, exception, spider):
        spider.logger.error(f"Spider exception for {response.url}: {exception}")
        return None
    
    def process_start_requests(self, start_requests, spider):
        for r in start_requests:
            yield r
    
    def spider_opened(self, spider):
        spider.logger.info(f"Spider opened: {spider.name}")