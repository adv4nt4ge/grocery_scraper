"""
Simple configuration file for the grocery scraper.
"""

# Database
DATABASE_PATH = "/Users/dkovtunov/shared/grocery_data.db"

# Store configurations
STORES = {
    "varus": {
        "name": "Varus",
        "base_url": "https://varus.ua",
        "use_playwright": True,
    },
    "silpo": {
        "name": "Silpo", 
        "base_url": "https://silpo.ua",
        "use_playwright": True,
    },
    "atb": {
        "name": "ATB",
        "base_url": "https://www.atbmarket.com",
        "api_url": "https://www.atbmarket.com/api/v1/products",
        "use_playwright": False,
    },
    "metro": {
        "name": "Metro",
        "base_url": "https://metro.zakaz.ua",
        "use_playwright": True,
    }
}

# Scraping settings
SCRAPING = {
    "timeout": 60000,  # 60 seconds
    "wait_between_requests": 1,  # seconds
    "wait_between_categories": 2,  # seconds
    "max_retries": 3,
    "headless": True,
}

# Resource blocking patterns (for faster loading)
BLOCKED_RESOURCES = [
    # Images and media
    r'\.jpg', r'\.jpeg', r'\.png', r'\.gif', r'\.webp', r'\.svg',
    r'\.mp4', r'\.avi', r'\.mov', r'\.mp3', r'\.wav',
    # Fonts
    r'\.woff', r'\.woff2', r'\.ttf', r'\.eot',
    # Analytics and tracking
    r'google-analytics\.com', r'googletagmanager\.com',
    r'doubleclick\.net', r'facebook\.com', r'yandex\.',
    r'clarity\.ms', r'hotjar\.com', r'segment\.com',
    r'sentry\.io', r'newrelic\.com', r'datadog',
    # Styles (optional - remove if layout breaks)
    r'\.css$',
]

# Browser settings
BROWSER_ARGS = [
    "--disable-images",
    "--disable-javascript-images", 
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]