# Grocery Search API Documentation

## Overview

RESTful API for searching and analyzing grocery products from Ukrainian stores (Varus, Silpo, ATB, Metro) using Elasticsearch.

**Base URL**: `http://localhost:5001`  
**Version**: 2.0.0 (Refactored Architecture)

## Architecture

The API uses a clean, layered architecture:

- **Controllers**: Thin Flask routes that handle HTTP requests/responses
- **Services**: Business logic layer (`ProductSearchService`)
- **Builders**: Query construction (`SearchQueryBuilder`)
- **Parsers**: Input validation and parsing (`FilterParser`)
- **Formatters**: Consistent response formatting (`ResponseFormatter`)
- **Configuration**: Centralized settings (`SearchConfig`)

## Endpoints

### 1. Search Products

Search for products with advanced filtering, sorting, and pagination.

**Endpoint**: `GET /api/search`

**Parameters**:
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `q` | string | "" | Search query (supports Ukrainian text) |
| `page` | integer | 1 | Page number (1-based) |
| `per_page` | integer | 20 | Results per page (max: 1000) |
| `sort` | string | "relevance" | Sort method: `relevance`, `price_asc`, `price_desc`, `name`, `rating`, `newest` |
| `stores` | string | - | Comma-separated store names: `ATB,Varus,Silpo,Metro` |
| `categories` | string | - | Comma-separated category names |
| `brands` | string | - | Comma-separated brand names |
| `price_min` | float | - | Minimum price filter |
| `price_max` | float | - | Maximum price filter |
| `has_discount` | boolean | - | Filter discounted products (`true`/`false`) |
| `in_stock` | boolean | - | Filter in-stock products (`true`/`false`) |
| `min_rating` | float | - | Minimum rating filter (0-5) |

**Example Requests**:
```
# Basic search
GET /api/search?q=молоко

# Search with pagination
GET /api/search?q=молоко&page=2&per_page=50

# Search with filters
GET /api/search?q=молоко&stores=ATB,Varus&price_max=100&sort=price_asc

# Get 100 products from specific stores
GET /api/search?q=сир&stores=Silpo,Metro&per_page=100
```

**Response**:
```json
{
  "products": [
    {
      "name": "Молоко Галичина 2.5%",
      "price": 42.99,
      "store": "ATB",
      "category": "Молочні продукти",
      "subcategory": "Молоко",
      "url": "https://...",
      "image_url": "https://...",
      "brand": "Галичина",
      "original_price": 52.99,
      "discount_percentage": 18.9,
      "has_discount": true,
      "in_stock": true,
      "rating": 4.5,
      "scraped_at": "2025-07-25T10:30:00",
      "_score": 8.5
    }
  ],
  "pagination": {
    "current_page": 1,
    "per_page": 20,
    "total_results": 245,
    "total_pages": 13,
    "has_next": true,
    "has_prev": false
  },
  "facets": {
    "stores": [
      {"key": "ATB", "count": 89},
      {"key": "Varus", "count": 76}
    ],
    "categories": [
      {"key": "Молочні продукти", "count": 145}
    ],
    "price_ranges": [
      {"key": "0-50", "count": 120},
      {"key": "50-100", "count": 95}
    ]
  },
  "query_info": {
    "query": "молоко",
    "filters": {"stores": ["ATB", "Varus"]},
    "sort_by": "price_asc",
    "took": 15
  }
}
```

### 2. Autocomplete Suggestions

Get search suggestions based on partial input.

**Endpoint**: `GET /api/suggestions`

**Parameters**:
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `q` | string | required | Partial search query |
| `size` | integer | 10 | Number of suggestions (max: 20) |

**Example Request**:
```
GET /api/suggestions?q=мол&size=5
```

**Response**:
```json
{
  "suggestions": [
    "молоко",
    "молоко Галичина",
    "молоко Яготинське",
    "молочний шоколад",
    "молоко безлактозне"
  ]
}
```

### 3. Get Product by ID

Retrieve detailed information about a specific product.

**Endpoint**: `GET /api/product/<product_id>`

**Example Request**:
```
GET /api/product/ATB_12345
```

**Response**:
```json
{
  "product": {
    "name": "Молоко Галичина 2.5%",
    "price": 42.99,
    "store": "ATB",
    "category": "Молочні продукти",
    "url": "https://...",
    "description": "Молоко пастеризоване...",
    "nutrition_info": {...},
    "scraped_at": "2025-07-25T10:30:00"
  }
}
```

### 4. Compare Prices

Compare prices for similar products across different stores.

**Endpoint**: `GET /api/compare`

**Parameters**:
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | required | Product name to compare |

**Example Request**:
```
GET /api/compare?name=Молоко%202.5%25
```

**Response**:
```json
{
  "products": [
    {
      "name": "Молоко Галичина 2.5%",
      "price": 42.99,
      "store": "ATB",
      "_score": 9.8
    },
    {
      "name": "Молоко Яготинське 2.5%",
      "price": 44.50,
      "store": "Silpo",
      "_score": 9.5
    },
    {
      "name": "Молоко Простоквашино 2.5%",
      "price": 46.99,
      "store": "Varus",
      "_score": 9.2
    }
  ]
}
```

### 5. Index Statistics

Get statistics about the product index.

**Endpoint**: `GET /api/stats`

**Example Request**:
```
GET /api/stats
```

**Response**:
```json
{
  "total_products": 115423,
  "stores": [
    {"name": "Silpo", "count": 66439},
    {"name": "Varus", "count": 23775},
    {"name": "Metro", "count": 16259},
    {"name": "ATB", "count": 8938}
  ],
  "categories": [
    {"name": "Бакалія", "count": 12543},
    {"name": "Молочні продукти", "count": 8932}
  ],
  "index_name": "grocery_products",
  "timestamp": "2025-07-25T14:30:00"
}
```

### 6. Health Check

Check API and Elasticsearch connectivity.

**Endpoint**: `GET /api/health`

**Example Request**:
```
GET /api/health
```

**Response**:
```json
{
  "status": "healthy",
  "elasticsearch": "connected",
  "timestamp": "2025-07-25T14:30:00"
}
```

## Search Features

### 1. Search Relevance

The search uses multiple strategies to ensure accurate results:

- **Exact phrase matching** (highest priority)
- **Exact word matching** (no stemming)
- **Word-based matching** with AND operator
- **Fuzzy matching** only for queries longer than 5 characters
- **Multi-field search** across name, brand, category, description

### 2. Ukrainian Language Support

- Full support for Ukrainian text
- Handles both Ukrainian and Russian product names
- Smart stemming and synonym handling
- Stop words filtering

### 3. Sorting Options

- **relevance** (default): By search score
- **price_asc**: Lowest price first
- **price_desc**: Highest price first
- **name**: Alphabetical by product name
- **rating**: Highest rated first
- **newest**: Most recently added

## Error Handling

All endpoints return appropriate HTTP status codes:

- **200**: Success
- **400**: Bad Request (invalid parameters)
- **404**: Not Found (product not found)
- **500**: Internal Server Error

Error response format:
```json
{
  "error": "Error message description"
}
```

## Rate Limiting

Currently no rate limiting is implemented. For production use, consider adding rate limiting middleware.

## Configuration

The API uses centralized configuration for all settings:

```python
class SearchConfig:
    # Boost scores for relevance
    EXACT_PHRASE_BOOST = 10
    EXACT_TERM_BOOST = 8
    EXACT_WORD_BOOST = 7
    WORD_MATCH_BOOST = 4
    FUZZY_MATCH_BOOST = 2
    
    # Pagination limits
    DEFAULT_PAGE_SIZE = 20
    MAX_PAGE_SIZE = 1000
    
    # Fuzzy matching rules
    MIN_QUERY_LENGTH_FOR_FUZZY = 5
```

## Examples

### Find cheapest milk across all stores
```
GET /api/search?q=молоко&sort=price_asc&per_page=10
```

### Get all discounted products from ATB
```
GET /api/search?stores=ATB&has_discount=true&per_page=100
```

### Search for specific brand products
```
GET /api/search?q=Галичина&brands=Галичина&per_page=50
```

### Get products in specific price range
```
GET /api/search?q=сир&price_min=50&price_max=200&sort=price_asc
```

### Compare yogurt prices
```
GET /api/compare?name=Йогурт%20полуниця
```

## Running the API

### With the refactored version:
```bash
python search_api_refactored.py
```

### Environment variables:
- `PORT`: API port (default: 5001)
- `ELASTICSEARCH_HOST`: Elasticsearch URL (default: http://localhost:9200)
- `ELASTICSEARCH_API_KEY`: API key for Elasticsearch

## Testing

The refactored architecture makes testing much easier:

```python
# Test query builder
query = SearchQueryBuilder()
    .add_search_query("молоко")
    .add_filters({"stores": ["ATB"]})
    .build()

# Test service layer
service = ProductSearchService(mock_es)
results = service.search(SearchRequest(query="test"))
```