# Scrapy settings for grocery_scraper project

BOT_NAME = 'grocery_scraper'

SPIDER_MODULES = ['grocery_scraper.spiders']
NEWSPIDER_MODULE = 'grocery_scraper.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# Optimized for Playwright with reduced timeouts
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# Reduced for faster processing
DOWNLOAD_DELAY = 0.3
# The download delay setting will honor only one of:
# Increased for better parallelism
CONCURRENT_REQUESTS_PER_DOMAIN = 64
# CONCURRENT_REQUESTS_PER_IP = 16

# Randomize download delay (50-150% of DOWNLOAD_DELAY)
RANDOMIZE_DOWNLOAD_DELAY = True

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'uk-UA,uk;q=0.9,en;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'grocery_scraper.grocery_scraper.middlewares.GroceryScrapySpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'grocery_scraper.middlewares.HeadersMiddleware': 400,
    'grocery_scraper.middlewares.PlaywrightTimeoutRetryMiddleware': 500,
    'grocery_scraper.middlewares.RetryMiddleware': 550,
}

# Download handlers for scrapy-playwright
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'grocery_scraper.pipelines.ValidationPipeline': 100,
    'grocery_scraper.pipelines.CategoryPipeline': 150,  # Process after validation
    'grocery_scraper.pipelines.DeduplicationPipeline': 200,
    'grocery_scraper.pipelines.DatabasePipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay - balanced for sequential processing
AUTOTHROTTLE_START_DELAY = 0.8
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 10.0
# The average number of requests Scrapy should be sending in parallel to
# each remote server - optimized for concurrent pagination
AUTOTHROTTLE_TARGET_CONCURRENCY = 3.0
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = True

# Force spider closure after no activity
# CLOSESPIDER_TIMEOUT = 15  # Close spider after 10 seconds of inactivity

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Playwright settings
PLAYWRIGHT_BROWSER_TYPE = 'chromium'
PLAYWRIGHT_LAUNCH_OPTIONS = {
    'headless': True,
    'args': [
        "--disable-notifications",
        "--disable-features=VizDisplayCompositor",
        "--disable-blink-features=AutomationControlled",
        "--disable-web-security",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-gpu",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-images",
        "--disable-plugins",
        "--disable-audio",
        "--disable-video",
        '--disable-features=TranslateUI',
        '--disable-ipc-flooding-protection',
        '--disable-default-apps',
        '--mute-audio',
        '--no-default-browser-check',
        '--disable-extensions',
        '--disable-component-extensions-with-background-pages',
    ]
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 15000
PLAYWRIGHT_DEFAULT_PAGE_TIMEOUT = 20000
# Increased Playwright limits for better concurrency
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 32
PLAYWRIGHT_MAX_CONTEXTS = 32
# Close contexts while crawling to prevent memory leaks
# Increased for large crawls with 700+ pages
PLAYWRIGHT_CONTEXT_CLOSE_AFTER_USES = 500
PLAYWRIGHT_CONTEXT_KWARGS = {
    'storage_state': None,  # Use default storage state
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'extra_http_headers': {
        'Accept-Encoding': 'gzip, deflate',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    }
}
# Block unnecessary resources
PLAYWRIGHT_CONTEXTS_ARGS = {
    'locale': 'uk-UA',
    'ignore_https_errors': True,
    "bypass_csp": True,
    "viewport": {"width": 1920, "height": 1080},
}
# Resource types to block for speed
PLAYWRIGHT_BLOCK_RESOURCE_TYPES = [
    'image', 'media', 'font',
    'ping', 'prefetch', 'preconnect', 'other',
    'websocket', 'manifest', 'texttrack'
]
# Performance optimizations based on Scrapy docs
# Increased thread pool for better concurrent processing
REACTOR_THREADPOOL_MAXSIZE = 50
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DOWNLOAD_TIMEOUT = 15

# Scheduler optimization for broad crawls
SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'

# Disable unnecessary features for performance
# COOKIES_ENABLED = False  # Overridden below
RETRY_ENABLED = True  # Needed for retry functionality
REDIRECT_ENABLED = False

# Connection pooling
DOWNLOAD_MAXSIZE = 10485760
DOWNLOAD_WARNSIZE = 5242880

# Database settings
DATABASE_URL = 'sqlite:///grocery_data.db'
DATABASE_CONNECTION_SETTINGS = {
    'check_same_thread': False,
    'timeout': 30,
}
# Retry settings
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403, 522, 524]

# Log settings (WARNING for maximum speed, INFO for monitoring)
LOG_LEVEL = 'INFO'  # Balanced monitoring

# Feed export encoding
FEED_EXPORT_ENCODING = 'utf-8'

# Memory optimization
MEMUSAGE_ENABLED = True
MEMUSAGE_LIMIT_MB = 1024
MEMUSAGE_WARNING_MB = 512
MEMUSAGE_CHECK_INTERVAL_SECONDS = 10

# Response processing limits - increased for better throughput
SCRAPER_SLOT_MAX_ACTIVE_SIZE = 10000000

# Queue size limits for better memory management
SCHEDULER_MEMORY_QUEUE_LIMIT = 100000

# Disable stats collection for speed (enable only when debugging)
STATS_CLASS = 'scrapy.statscollectors.MemoryStatsCollector'

# Duplicate filter settings
DUPEFILTER_CLASS = 'scrapy.dupefilters.RFPDupeFilter'
DUPEFILTER_DEBUG = False

# Cookies optimization - enabled for session persistence
COOKIES_ENABLED = True
COOKIES_DEBUG = False
