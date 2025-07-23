#!/usr/bin/env python3
"""
ATB scraper using curl to bypass Cloudflare protection.

⚠️  LIMITATION: ATB uses advanced Cloudflare protection that requires
JavaScript execution and cannot be bypassed with curl alone.

This scraper will detect Cloudflare protection and inform the user
that a Playwright-based scraper is needed for ATB.

For working ATB scraping, use the Scrapy spider with Playwright:
    scrapy crawl atb
"""

import json
import subprocess
import sqlite3
import time
from typing import List, Dict, Optional
from datetime import datetime
import re


class ATBCurlScraper:
    """ATB scraper using curl to bypass Cloudflare protection."""
    
    def __init__(self, db_path: str = "grocery_data.db"):
        self.store_name = "ATB"
        self.base_url = "https://www.atbmarket.com"
        self.api_url = "https://www.atbmarket.com/api/v1/catalog/search"
        self.db_path = db_path
        self.session_cookies = None
        
    def init_database(self):
        """Initialize database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # Create products table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    price REAL,
                    category TEXT,
                    subcategory TEXT,
                    store TEXT NOT NULL,
                    url TEXT,
                    image_url TEXT,
                    scraped_at TEXT,
                    UNIQUE(name, store, url)
                )
            ''')
            
            # Create categories table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT,
                    category_url TEXT,
                    UNIQUE(store, category, subcategory)
                )
            ''')
            
            conn.commit()
            print("✅ Database initialized")
            
        except Exception as e:
            print(f"❌ Database error: {e}")
        finally:
            conn.close()
    
    def get_categories(self) -> List[Dict[str, str]]:
        """Get ATB categories with correct Ukrainian names and URLs."""
        return [
            {"name": "Бакалія", "id": "285", "url": "/catalog/285-bakaliya"},
            {"name": "М'ясо", "id": "maso", "url": "/catalog/maso"},
            {"name": "Молочні продукти та яйця", "id": "molocni", "url": "/catalog/molocni-produkti-ta-ajca"},
            {"name": "Овочі та фрукти", "id": "287", "url": "/catalog/287-ovochi-ta-frukti"},
            {"name": "Хлібобулочні вироби", "id": "325", "url": "/catalog/325-khlibobulochni-virobi"},
            {"name": "Напої безалкогольні", "id": "294", "url": "/catalog/294-napoi-bezalkogol-ni"},
            {"name": "Кондитерські вироби", "id": "kondit", "url": "/catalog/konditerski-virobi"},
            {"name": "Заморожені продукти", "id": "zamoroz", "url": "/catalog/zamorozheni-produkti"},
            {"name": "Консерви", "id": "konservy", "url": "/catalog/konservi"},
            {"name": "Товари для дому", "id": "household", "url": "/catalog/pobutova-khimiya"},
            {"name": "Дитяче харчування", "id": "baby", "url": "/catalog/dityache-kharchuvannya"},
            {"name": "Алкогольні напої", "id": "alcohol", "url": "/catalog/alkogolni-napoi"},
        ]
    
    def make_curl_request(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Make HTTP request using curl to bypass Cloudflare."""
        headers = [
            "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language: en-US,en;q=0.5",
            "Accept-Encoding: gzip, deflate, br",
            "Connection: keep-alive",
            "Upgrade-Insecure-Requests: 1",
        ]
        
        for attempt in range(max_retries):
            try:
                cmd = [
                    "curl", "-L", "-X", "GET",
                    "--compressed",
                    "--cookie-jar", "/tmp/atb_cookies.txt",
                    "--cookie", "/tmp/atb_cookies.txt",
                ]
                
                # Add headers
                for header in headers:
                    cmd.extend(["-H", header])
                
                # Add cookies if we have them
                if self.session_cookies:
                    cmd.extend(["-b", self.session_cookies])
                
                cmd.append(url)
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=35)
                
                if result.returncode == 0 and result.stdout:
                    return result.stdout
                else:
                    print(f"⚠️ Curl error (attempt {attempt + 1}): {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                print(f"⚠️ Request timeout (attempt {attempt + 1})")
            except Exception as e:
                print(f"⚠️ Request error (attempt {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def build_page_url(self, category_url: str, page: int) -> str:
        """Build page URL for ATB pagination."""
        if page == 1:
            return category_url
        
        # ATB uses different pagination patterns, try common ones
        patterns = [
            f"{category_url}?page={page}",
            f"{category_url}&page={page}" if '?' in category_url else f"{category_url}?page={page}",
            f"{category_url}/page/{page}",
            f"{category_url}?p={page}",
        ]
        
        # Return the most likely pattern for ATB
        if '?' in category_url:
            return f"{category_url}&page={page}"
        else:
            return f"{category_url}?page={page}"
    
    def is_empty_page(self, html: str) -> bool:
        """Check if page indicates no more products."""
        empty_indicators = [
            'no products found',
            'немає товарів',
            'пусто',
            'empty-results',
            'no-results',
            '404',
            'page not found',
            'сторінка не знайдена',
        ]
        
        # Check for Cloudflare protection
        cloudflare_indicators = [
            'just a moment',
            'enable javascript and cookies',
            '_cf_chl_opt',
            'cloudflare',
            'challenge-platform',
        ]
        
        html_lower = html.lower()
        
        # If Cloudflare protection detected, it's not exactly "empty" but we can't proceed
        if any(indicator in html_lower for indicator in cloudflare_indicators):
            return False  # Don't treat as empty, treat as blocked
        
        return any(indicator in html_lower for indicator in empty_indicators)
    
    def is_cloudflare_protected(self, html: str) -> bool:
        """Check if page is showing Cloudflare protection."""
        # Only check for actual blocking indicators, not just presence of Cloudflare
        blocking_indicators = [
            'just a moment',
            'enable javascript and cookies',
            '_cf_chl_opt',
            'window._cf_chl_opt',
            'challenge-platform/h/g',  # More specific
        ]
        
        html_lower = html.lower()
        return any(indicator in html_lower for indicator in blocking_indicators)
    
    def establish_session(self) -> bool:
        """Visit main page first to establish session cookies."""
        print("🔐 Establishing session with ATB...")
        
        # Clean up any old cookies
        import os
        cookie_file = "/tmp/atb_cookies.txt"
        if os.path.exists(cookie_file):
            os.remove(cookie_file)
        
        html = self.make_curl_request(self.base_url)
        if html:
            if not self.is_cloudflare_protected(html):
                print("✅ Session established successfully")
                return True
            else:
                print("⚠️ Could not establish session - Cloudflare protection active")
                return False
        else:
            print("❌ No response received")
            return False
    
    def extract_products_from_html(self, html: str, category_name: str) -> List[Dict]:
        """Extract products from ATB HTML page."""
        products = []
        
        try:
            # ATB uses article.catalog-item structure - extract each complete article
            import re
            # Find all complete catalog-item articles
            article_pattern = r'<article[^>]*class="[^"]*catalog-item[^"]*"[^>]*>.*?</article>'
            catalog_items = re.findall(article_pattern, html, re.DOTALL | re.IGNORECASE)
            
            print(f"   🔍 Found {len(catalog_items)} complete catalog items")
            
            for i, item_html in enumerate(catalog_items, 1):
                try:
                    product = self.parse_product_html(item_html, category_name)
                    if product:
                        products.append(product)
                        if i <= 3:  # Debug first few products
                            print(f"   ✅ Extracted product {i}: {product['name'][:50]}... - {product['price']} грн")
                    # elif i == 1:  # Debug disabled for cleaner output
                    #     pass
                except Exception as e:
                    if i <= 2:  # Only show errors for first few
                        print(f"   ❌ DEBUG: Error parsing product {i}: {e}")
            
            # If no structured products found, try to extract from JSON data in HTML
            if not products:
                json_pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
                json_match = re.search(json_pattern, html, re.DOTALL)
                
                if json_match:
                    try:
                        data = json.loads(json_match.group(1))
                        products = self.extract_products_from_json(data, category_name)
                    except json.JSONDecodeError:
                        pass
            
        except Exception as e:
            print(f"⚠️ Error extracting products: {e}")
        
        return products
    
    def parse_product_html(self, html: str, category_name: str) -> Optional[Dict]:
        """Parse individual product from HTML snippet."""
        try:
            # Extract name - ATB specific patterns (more specific)
            name_patterns = [
                r'<h[1-6][^>]*class="[^"]*catalog-item__name[^"]*"[^>]*>([^<]+)</h[1-6]>',
                r'<div[^>]*class="[^"]*catalog-item__name[^"]*"[^>]*>([^<]+)</div>',
                r'<a[^>]*class="[^"]*catalog-item__name[^"]*"[^>]*>([^<]+)</a>',
                r'<img[^>]*class="catalog-item__img"[^>]*alt="([^"]+)"',  # More specific alt
                r'<h[1-6][^>]*>([^<]+)</h[1-6]>',
                r'title="([^"]+)"',
            ]
            
            name = None
            for pattern in name_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    name = match.group(1).strip()
                    break
            
            if not name:
                # Fallback: look for alt in img tag, but exclude currency 
                alt_search = re.search(r'<img[^>]*alt="([^"]*)"', html, re.IGNORECASE)
                if alt_search and alt_search.group(1).strip() and alt_search.group(1).strip() != "Гривня":
                    name = alt_search.group(1).strip()
                else:
                    return None
            
            # Extract price - ATB specific patterns (fixed)
            price_patterns = [
                # Two-part price pattern: 86.<span class="product-price__coin">90</span>
                r'(\d+)\.<span[^>]*class="[^"]*product-price__coin[^"]*"[^>]*>(\d+)</span>',
                # Simpler two-part pattern
                r'(\d+)\.<span[^>]*>(\d+)</span>',
                # Simple patterns
                r'(\d+[.,]\d+)\s*грн',
                r'(\d+)\s*грн',
                r'(\d+[.,]\d+)\s*₴',
                r'(\d+)\s*₴',
            ]
            
            price = None
            for pattern in price_patterns:
                match = re.search(pattern, html)
                if match:
                    if len(match.groups()) == 2:  # Two-part price (main + coin)
                        try:
                            main_part = match.group(1)
                            coin_part = match.group(2)
                            price = float(f"{main_part}.{coin_part}")
                            break
                        except ValueError:
                            continue
                    else:  # Single part price
                        price_str = match.group(1).replace(',', '.')
                        try:
                            price = float(price_str)
                            break
                        except ValueError:
                            continue
            
            if not price:
                return None
            
            # Extract URL
            url_patterns = [
                r'href="([^"]+)"',
                r"href='([^']+)'",
            ]
            
            url = None
            for pattern in url_patterns:
                match = re.search(pattern, html)
                if match:
                    url = match.group(1)
                    if url.startswith('/'):
                        url = self.base_url + url
                    break
            
            # Extract image
            image_patterns = [
                r'src="([^"]*\.(?:jpg|jpeg|png|webp)[^"]*)"',
                r'data-src="([^"]*\.(?:jpg|jpeg|png|webp)[^"]*)"',
            ]
            
            image_url = None
            for pattern in image_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    image_url = match.group(1)
                    if image_url.startswith('/'):
                        image_url = self.base_url + image_url
                    break
            
            return {
                'name': name,
                'price': price,
                'category': category_name,
                'subcategory': None,
                'store': self.store_name,
                'url': url,
                'image_url': image_url,
                'scraped_at': datetime.now().isoformat(),
            }
            
        except Exception as e:
            print(f"⚠️ Error parsing product HTML: {e}")
            return None
    
    def extract_products_from_json(self, data: dict, category_name: str) -> List[Dict]:
        """Extract products from JSON data embedded in page."""
        products = []
        
        try:
            # Navigate through possible JSON structures
            possible_paths = [
                ['products'],
                ['catalog', 'products'],
                ['data', 'products'],
                ['items'],
                ['results'],
            ]
            
            product_data = None
            for path in possible_paths:
                current = data
                for key in path:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        break
                else:
                    if isinstance(current, list):
                        product_data = current
                        break
            
            if product_data:
                for item in product_data:
                    if isinstance(item, dict):
                        name = item.get('name') or item.get('title')
                        price = item.get('price') or item.get('cost')
                        
                        if name and price:
                            try:
                                price = float(price)
                                products.append({
                                    'name': str(name).strip(),
                                    'price': price,
                                    'category': category_name,
                                    'subcategory': None,
                                    'store': self.store_name,
                                    'url': item.get('url') or item.get('link'),
                                    'image_url': item.get('image') or item.get('photo'),
                                    'scraped_at': datetime.now().isoformat(),
                                })
                            except ValueError:
                                continue
        
        except Exception as e:
            print(f"⚠️ Error extracting from JSON: {e}")
        
        return products
    
    def save_products(self, products: List[Dict]) -> int:
        """Save products to database."""
        if not products:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        saved_count = 0
        
        try:
            cursor = conn.cursor()
            
            for product in products:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO products 
                        (name, price, category, subcategory, store, url, image_url, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        product['name'],
                        product['price'],
                        product['category'],
                        product.get('subcategory'),
                        product['store'],
                        product.get('url'),
                        product.get('image_url'),
                        product['scraped_at']
                    ))
                    saved_count += 1
                    
                except sqlite3.Error as e:
                    print(f"⚠️ Database error for product {product.get('name')}: {e}")
            
            conn.commit()
            
        except Exception as e:
            print(f"❌ Error saving products: {e}")
        finally:
            conn.close()
        
        return saved_count
    
    def save_category(self, category_name: str, category_url: str):
        """Save category to database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO categories 
                (store, category, category_url)
                VALUES (?, ?, ?)
            ''', (self.store_name, category_name, category_url))
            conn.commit()
        except Exception as e:
            print(f"⚠️ Error saving category: {e}")
        finally:
            conn.close()
    
    def get_max_pages(self, category_url: str) -> int:
        """Get the maximum number of pages for a category."""
        print(f"   🔍 Getting pagination info...")
        html = self.make_curl_request(category_url)
        if not html:
            return 1
        
        # Look for pagination numbers
        pagination_patterns = [
            r'page=(\d+).*?(?:наступна|next|>)',  # Link to next with page number
            r'href="[^"]*page=(\d+)"[^>]*>(\d+)<',  # Direct page links
            r'page\s*(\d+)\s*of\s*(\d+)',  # "Page X of Y" format
        ]
        
        max_page = 1
        for pattern in pagination_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                try:
                    if isinstance(match, tuple):
                        page_num = max(int(m) for m in match if m.isdigit())
                    else:
                        page_num = int(match)
                    max_page = max(max_page, page_num)
                except ValueError:
                    continue
        
        print(f"   📊 Found maximum page: {max_page}")
        return max_page

    def scrape_category(self, category: Dict[str, str]) -> int:
        """Scrape products from a specific category using concurrent requests."""
        print(f"📁 Scraping category: {category['name']}")
        
        # Establish session first
        if not self.establish_session():
            print(f"❌ Cannot proceed with {category['name']} - session establishment failed")
            return 0
        
        category_url = self.base_url + category['url']
        
        # Save category to database
        self.save_category(category['name'], category_url)
        
        # Get maximum pages first
        max_pages = self.get_max_pages(category_url)
        
        print(f"   🚀 Starting concurrent scraping of {max_pages} pages...")
        
        # Use concurrent requests to scrape all pages
        total_products = 0
        import concurrent.futures
        import threading
        
        def scrape_single_page(page_num):
            """Scrape a single page and return products."""
            page_url = self.build_page_url(category_url, page_num)
            html = self.make_curl_request(page_url)
            
            if not html or self.is_cloudflare_protected(html) or self.is_empty_page(html):
                return []
            
            products = self.extract_products_from_html(html, category['name'])
            if page_num <= 2:  # Debug first 2 pages
                print(f"   📄 Page {page_num}: {len(products)} products (from {html.count('catalog-item')} items)")
            else:
                print(f"   📄 Page {page_num}: {len(products)} products")
            return products
        
        # Use ThreadPoolExecutor for concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all page requests
            future_to_page = {
                executor.submit(scrape_single_page, page): page 
                for page in range(1, max_pages + 1)
            }
            
            # Collect results
            all_products = []
            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    products = future.result()
                    all_products.extend(products)
                except Exception as e:
                    print(f"   ❌ Error scraping page {page_num}: {e}")
        
        # Save all products
        if all_products:
            saved = self.save_products(all_products)
            total_products = saved
            print(f"   ✅ Saved {saved} total products from {max_pages} pages")
        else:
            print(f"   ⚠️ No products extracted from any pages")
        
        print(f"✅ Category '{category['name']}' complete: {total_products} products")
        return total_products
    
    def scrape_all_categories(self):
        """Scrape all ATB categories."""
        print(f"🛒 Starting ATB scraper (curl-based)")
        print(f"📊 Database: {self.db_path}")
        print(f"⚠️  Note: ATB uses Cloudflare protection - may require Playwright")
        
        # Initialize database
        self.init_database()
        
        categories = self.get_categories()
        total_products = 0
        
        for i, category in enumerate(categories, 1):
            print(f"\n[{i}/{len(categories)}] {category['name']}")
            try:
                count = self.scrape_category(category)
                total_products += count
            except Exception as e:
                print(f"❌ Error scraping {category['name']}: {e}")
            
            # Small delay between categories
            time.sleep(2)
        
        print(f"\n🎉 ATB scraping complete!")
        print(f"📊 Total products scraped: {total_products}")
    
    def scrape_category_by_name(self, category_name: str):
        """Scrape a specific category by name."""
        categories = self.get_categories()
        
        # Find matching category
        matching_category = None
        for cat in categories:
            if category_name.lower() in cat['name'].lower():
                matching_category = cat
                break
        
        if not matching_category:
            print(f"❌ Category '{category_name}' not found")
            print(f"Available categories: {', '.join([c['name'] for c in categories])}")
            return
        
        # Initialize database
        self.init_database()
        
        # Scrape the category
        self.scrape_category(matching_category)


def main():
    """Main function to run ATB scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ATB Grocery Store Scraper (Curl-based)')
    parser.add_argument('--category', '-c', 
                       help='Scrape specific category (e.g., "Бакалія")')
    parser.add_argument('--db', '-d', 
                       default='grocery_data.db',
                       help='Database path')
    
    args = parser.parse_args()
    
    scraper = ATBCurlScraper(db_path=args.db)
    
    if args.category:
        scraper.scrape_category_by_name(args.category)
    else:
        scraper.scrape_all_categories()


if __name__ == "__main__":
    main()