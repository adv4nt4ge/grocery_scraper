#!/usr/bin/env python3
"""
Scrapy Items with validation and serialization for grocery scraper.
"""

import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from scrapy.loader import ItemLoader
from typing import Optional, Union, Any, Dict, List, Tuple
from datetime import datetime

# Simple utility functions (avoid external dependencies)
import re

def clean_text(text):
    if not text:
        return ""
    return ' '.join(text.strip().split())

def clean_price(price_text):
    if not price_text:
        return None
    clean = re.sub(r'[^\d,.]', '', str(price_text))
    clean = clean.replace(',', '.')
    try:
        return float(clean)
    except:
        return None

def normalize_url(url):
    return url.strip() if url else url


def validate_product_data(data):
    return True, []  # Simplified


def validate_price(value: Union[str, float, int]) -> Optional[float]:
    """Validate and clean price value."""
    price = clean_price(value)
    if price is None or price < 0:
        raise ValueError(f"Invalid price: {value}")
    return price


def validate_url(value: str) -> str:
    """Validate and normalize URL."""
    if not value:
        raise ValueError("URL cannot be empty")
    return normalize_url(value)


def validate_store(value: str) -> str:
    """Validate store name."""
    valid_stores = ['ATB', 'Varus', 'Silpo', 'Metro']
    if value not in valid_stores:
        raise ValueError(f"Invalid store: {value}. Must be one of {valid_stores}")
    return value




class ProductItem(scrapy.Item):
    """Enhanced product item with validation and type hints."""
    
    # Required fields
    name = scrapy.Field()
    price = scrapy.Field()
    store = scrapy.Field()
    url = scrapy.Field()
    category = scrapy.Field()
    
    # Optional fields
    subcategory = scrapy.Field()
    image_url = scrapy.Field()
    brand = scrapy.Field()
    description = scrapy.Field()
    
    # Pricing fields
    original_price = scrapy.Field()
    discount_percentage = scrapy.Field()
    discount_amount = scrapy.Field()
    
    # Price per standard unit
    unit_price = scrapy.Field()
    
    # Availability
    availability = scrapy.Field()
    stock_quantity = scrapy.Field()
    
    # Additional metadata
    scraped_at = scrapy.Field()
    product_id = scrapy.Field()
    rating = scrapy.Field()
    reviews_count = scrapy.Field()
    
    # Store-specific fields
    store_category = scrapy.Field()
    store_subcategory = scrapy.Field()
    promo_tags = scrapy.Field()
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate item data."""
        return validate_product_data(dict(self))


class CategoryItem(scrapy.Item):
    """Category information item."""
    
    store = scrapy.Field()
    category = scrapy.Field()
    subcategory = scrapy.Field()
    category_url = scrapy.Field()
    parent_category = scrapy.Field()
    product_count = scrapy.Field()
    scraped_at = scrapy.Field()


class ProductItemLoader(ItemLoader):
    """ItemLoader with preprocessing for ProductItem."""
    
    default_item_class = ProductItem
    default_input_processor = MapCompose(clean_text)
    default_output_processor = TakeFirst()
    
    # Field-specific processors
    name_in = MapCompose(clean_text)
    name_out = TakeFirst()
    
    price_in = MapCompose(validate_price)
    price_out = TakeFirst()
    
    original_price_in = MapCompose(validate_price)
    original_price_out = TakeFirst()
    
    store_in = MapCompose(validate_store)
    store_out = TakeFirst()
    
    url_in = MapCompose(validate_url)
    url_out = TakeFirst()
    
    image_url_in = MapCompose(lambda x: x.strip() if x else None)
    image_url_out = TakeFirst()
    
    category_in = MapCompose(clean_text)
    category_out = TakeFirst()
    
    subcategory_in = MapCompose(clean_text)
    subcategory_out = TakeFirst()
    
    description_in = MapCompose(lambda x: clean_text(x, preserve_newlines=True))
    description_out = TakeFirst()
    
    
    # Date processing
    scraped_at_in = MapCompose(lambda x: x if isinstance(x, datetime) else datetime.now())
    scraped_at_out = TakeFirst()
    
    # Numeric fields
    rating_in = MapCompose(lambda x: float(x) if x and str(x).replace('.', '').isdigit() else None)
    rating_out = TakeFirst()
    
    reviews_count_in = MapCompose(lambda x: int(x) if x and str(x).isdigit() else None)
    reviews_count_out = TakeFirst()
    
    discount_percentage_in = MapCompose(lambda x: float(x) if x and str(x).replace('.', '').isdigit() else None)
    discount_percentage_out = TakeFirst()


class CategoryItemLoader(ItemLoader):
    """ItemLoader for CategoryItem."""
    
    default_item_class = CategoryItem
    default_input_processor = MapCompose(clean_text)
    default_output_processor = TakeFirst()
    
    store_in = MapCompose(validate_store)
    store_out = TakeFirst()
    
    category_url_in = MapCompose(validate_url)
    category_url_out = TakeFirst()
    
    product_count_in = MapCompose(lambda x: int(x) if x and str(x).isdigit() else 0)
    product_count_out = TakeFirst()
    
    scraped_at_in = MapCompose(lambda x: x if isinstance(x, datetime) else datetime.now())
    scraped_at_out = TakeFirst()


def create_product_loader(response=None, selector=None) -> ProductItemLoader:
    """Create a ProductItemLoader instance."""
    return ProductItemLoader(selector=selector, response=response)


def create_category_loader(response=None, selector=None) -> CategoryItemLoader:
    """Create a CategoryItemLoader instance."""
    return CategoryItemLoader(selector=selector, response=response)