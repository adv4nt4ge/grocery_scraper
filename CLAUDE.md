# Grocery Price Scraper

A professional web scraper for Ukrainian grocery stores using Scrapy and curl-based solutions.

## Project Instructions

This project scrapes grocery prices from:
- **Varus** (varus.ua) - Scrapy spider
- **Silpo** (silpo.ua) - Scrapy spider  
- **ATB** (atbmarket.com) - Curl-based scraper (bypasses Cloudflare)
- **Metro** (metro.zakaz.ua) - Scrapy spider

## Key Features
- JavaScript rendering with Playwright
- Automatic category discovery with Ukrainian category names
- Category and subcategory extraction
- Database storage with comprehensive schema
- Concurrent scraping
- Error handling and retries
- Cloudflare bypass for ATB

## Commands

### Scrapy Spiders
- `scrapy crawl varus` - Scrape Varus store
- `scrapy crawl silpo` - Scrape Silpo store  
- `scrapy crawl metro` - Scrape Metro store
- `scrapy list` - List available Scrapy spiders

### ATB (Curl-based)
- `python atb_curl_scraper.py` - Run ATB scraper with category discovery
- `python scrape_atb_all.py` - Run ATB scraper on all categories

### Database
Data is saved to shared database `/Users/dkovtunov/shared/grocery_data.db` with:
- Products: name, price, category, subcategory, store, url, image_url
- Categories: Ukrainian category names extracted from breadcrumbs
- Current data: 115K+ products across 4 stores
- **Shared location**: Accessible by both scraper and API projects

## Database Schema
- **ATB**: 8,938 products with Ukrainian categories (Бакалія, Алкоголь, etc.)
- **Metro**: 16,259 products
- **Silpo**: 66,439 products  
- **Varus**: 23,775 products

## Coding Standards and Conventions

### Naming Conventions
- **AVOID using "enhanced" in class or method names** - This prefix doesn't provide meaningful information about functionality
- Use descriptive, specific names that indicate the actual purpose or behavior
- Examples:
  - ❌ `EnhancedDatabase` → ✅ `OptimizedDatabase` or `PooledDatabase`
  - ❌ `enhanced_spider.py` → ✅ `base_spider.py` or `advanced_spider.py`
  - ❌ `enhanced_cache_manager()` → ✅ `cache_manager()` or `smart_cache_manager()`

### General Guidelines
- Use clear, descriptive names that explain what the code does
- Prefer specific technical terms over generic adjectives
- Follow Python naming conventions (snake_case for functions/variables, PascalCase for classes)
- Keep class and method names concise but descriptive
- Never change working locators without checking with you first.
- Varus uses a Vue.js SPA with heavy client-side rendering. The products are NOT in the initial HTML but are dynamically loaded by JavaScript after the Vue application
  initializes.