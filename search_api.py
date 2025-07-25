#!/usr/bin/env python3
"""
Refactored Flask-based REST API for searching grocery products using Elasticsearch.
Demonstrates better separation of concerns and maintainability.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import os

# Configuration Classes
class SearchConfig:
    """Configuration for search functionality."""
    # Boost scores for different match types
    EXACT_PHRASE_BOOST = 10
    EXACT_TERM_BOOST = 8
    EXACT_WORD_BOOST = 7
    WORD_MATCH_BOOST = 4
    FUZZY_MATCH_BOOST = 2
    
    # Fuzzy matching settings
    MIN_QUERY_LENGTH_FOR_FUZZY = 5
    FUZZY_DISTANCE = "1"
    
    # Pagination
    DEFAULT_PAGE_SIZE = 20
    MAX_PAGE_SIZE = 1000
    
    # Price ranges for facets
    PRICE_RANGES = [
        {"key": "0-50", "to": 50},
        {"key": "50-100", "from": 50, "to": 100},
        {"key": "100-200", "from": 100, "to": 200},
        {"key": "200-500", "from": 200, "to": 500},
        {"key": "500+", "from": 500}
    ]
    
    # Aggregation sizes
    STORE_AGG_SIZE = 10
    CATEGORY_AGG_SIZE = 15
    BRAND_AGG_SIZE = 15


class SortOption(Enum):
    """Available sort options."""
    RELEVANCE = "relevance"
    PRICE_ASC = "price_asc"
    PRICE_DESC = "price_desc"
    NAME = "name"
    RATING = "rating"
    NEWEST = "newest"


@dataclass
class SearchRequest:
    """Validated search request parameters."""
    query: str = ""
    page: int = 1
    per_page: int = SearchConfig.DEFAULT_PAGE_SIZE
    sort_by: SortOption = SortOption.RELEVANCE
    filters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.filters is None:
            self.filters = {}


class FilterParser:
    """Parses and validates filter parameters from request."""
    
    @staticmethod
    def parse_filters(args: Dict[str, str]) -> Dict[str, Any]:
        """Parse filter parameters from request arguments."""
        filters = {}
        
        # List filters
        for field in ['stores', 'categories', 'brands']:
            if args.get(field):
                filters[field] = args[field].split(',')
        
        # Numeric filters
        for field in ['price_min', 'price_max', 'min_rating']:
            if args.get(field):
                try:
                    filters[field] = float(args[field])
                except ValueError:
                    pass  # Skip invalid values
        
        # Boolean filters
        for field in ['has_discount', 'in_stock']:
            if args.get(field):
                filters[field] = args[field].lower() == 'true'
        
        return filters


class SearchQueryBuilder:
    """Builds Elasticsearch queries with a fluent interface."""
    
    def __init__(self, config: SearchConfig = SearchConfig()):
        self.config = config
        self.query: Dict[str, Any] = {
            "query": {"bool": {"must": [], "should": [], "filter": []}},
            "highlight": SearchQueryBuilder._get_highlight_config(),
            "aggs": self._get_aggregations_config()
        }
    
    def add_search_query(self, query_text: str) -> 'SearchQueryBuilder':
        """Add search query with relevance scoring."""
        if not query_text or not query_text.strip():
            self.query["query"]["bool"]["must"].append({"match_all": {}})
            return self
        
        should_clauses = [
            self._exact_phrase_clause(query_text),
            self._exact_term_clause(query_text),
            self._exact_word_clause(query_text),
            self._word_match_clause(query_text),
            SearchQueryBuilder._multi_field_clause(query_text)
        ]
        
        # Add fuzzy matching for longer queries
        if self._should_use_fuzzy(query_text):
            should_clauses.append(self._fuzzy_match_clause(query_text))
        
        self.query["query"]["bool"]["should"] = should_clauses
        self.query["query"]["bool"]["minimum_should_match"] = 1
        
        return self
    
    def add_filters(self, filters: Dict[str, Any]) -> 'SearchQueryBuilder':
        """Add filters to the query."""
        if not filters:
            return self
        
        for field in ['stores', 'categories', 'brands', 'has_discount', 'in_stock']:
            if field in filters:
                if field == 'stores':
                    self._add_terms_filter(filters[field], "store")
                elif field == 'categories':
                    self._add_terms_filter(filters[field], "category.raw")
                elif field == 'brands':
                    self._add_terms_filter(filters[field], "brand.raw")
                elif field == 'has_discount':
                    self._add_term_filter(filters[field], "has_discount")
                elif field == 'in_stock':
                    self._add_term_filter(filters[field], "in_stock")
        
        # Handle price range
        if 'price_min' in filters or 'price_max' in filters:
            self._add_price_filter(filters.get('price_min'), filters.get('price_max'))
        
        # Handle rating filter
        if 'min_rating' in filters:
            self._add_range_filter("rating", gte=filters['min_rating'])
        
        return self
    
    def add_sorting(self, sort_by: SortOption) -> 'SearchQueryBuilder':
        """Add sorting configuration."""
        sort_configs = {
            SortOption.RELEVANCE: [{"_score": {"order": "desc"}}],
            SortOption.PRICE_ASC: [{"price": {"order": "asc"}}, {"_score": {"order": "desc"}}],
            SortOption.PRICE_DESC: [{"price": {"order": "desc"}}, {"_score": {"order": "desc"}}],
            SortOption.NAME: [{"name.raw": {"order": "asc"}}, {"_score": {"order": "desc"}}],
            SortOption.RATING: [{"rating": {"order": "desc", "missing": "_last"}}, {"_score": {"order": "desc"}}],
            SortOption.NEWEST: [{"scraped_at": {"order": "desc"}}, {"_score": {"order": "desc"}}]
        }
        
        self.query["sort"] = sort_configs.get(sort_by, sort_configs[SortOption.RELEVANCE])
        return self
    
    def build(self) -> Dict[str, Any]:
        """Build the final query."""
        return self.query
    
    # Private helper methods
    def _exact_phrase_clause(self, query: str) -> Dict[str, Any]:
        return {
            "match_phrase": {
                "name": {
                    "query": query,
                    "boost": self.config.EXACT_PHRASE_BOOST
                }
            }
        }
    
    def _exact_term_clause(self, query: str) -> Dict[str, Any]:
        return {
            "term": {
                "name.raw": {
                    "value": query,
                    "boost": self.config.EXACT_TERM_BOOST
                }
            }
        }
    
    def _exact_word_clause(self, query: str) -> Dict[str, Any]:
        return {
            "match": {
                "name.exact": {
                    "query": query,
                    "operator": "and",
                    "boost": self.config.EXACT_WORD_BOOST
                }
            }
        }
    
    def _word_match_clause(self, query: str) -> Dict[str, Any]:
        return {
            "match": {
                "name": {
                    "query": query,
                    "operator": "and",
                    "boost": self.config.WORD_MATCH_BOOST
                }
            }
        }
    
    @staticmethod
    def _multi_field_clause(query: str) -> Dict[str, Any]:
        return {
            "multi_match": {
                "query": query,
                "fields": ["brand^2", "category^1.5", "subcategory^1.2", "description"],
                "type": "best_fields",
                "operator": "and",
                "fuzziness": "0"
            }
        }
    
    def _fuzzy_match_clause(self, query: str) -> Dict[str, Any]:
        return {
            "match": {
                "name": {
                    "query": query,
                    "fuzziness": self.config.FUZZY_DISTANCE,
                    "boost": self.config.FUZZY_MATCH_BOOST
                }
            }
        }
    
    def _should_use_fuzzy(self, query: str) -> bool:
        """Determine if fuzzy matching should be used."""
        if len(query) <= self.config.MIN_QUERY_LENGTH_FOR_FUZZY:
            return False
        
        # Exclude specific problematic queries
        excluded_terms = ['творог', 'молок', 'масл']
        return not any(term in query.lower() for term in excluded_terms)
    
    def _add_terms_filter(self, values: List[str], field: str):
        """Add terms filter."""
        if isinstance(values, str):
            values = [values]
        self.query["query"]["bool"]["filter"].append({
            "terms": {field: values}
        })
    
    def _add_term_filter(self, value: Any, field: str):
        """Add term filter."""
        self.query["query"]["bool"]["filter"].append({
            "term": {field: value}
        })
    
    def _add_range_filter(self, field: str, gte=None, lte=None):
        """Add range filter."""
        range_filter = {"range": {field: {}}}
        if gte is not None:
            range_filter["range"][field]["gte"] = gte
        if lte is not None:
            range_filter["range"][field]["lte"] = lte
        self.query["query"]["bool"]["filter"].append(range_filter)
    
    def _add_price_filter(self, min_price: Optional[float], max_price: Optional[float]):
        """Add price range filter."""
        self._add_range_filter("price", gte=min_price, lte=max_price)
    
    @staticmethod
    def _get_highlight_config() -> Dict[str, Any]:
        """Get highlight configuration."""
        return {
            "fields": {
                "name": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                "description": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]}
            }
        }
    
    def _get_aggregations_config(self) -> Dict[str, Any]:
        """Get aggregations configuration."""
        return {
            "stores": {"terms": {"field": "store", "size": self.config.STORE_AGG_SIZE}},
            "categories": {"terms": {"field": "category.raw", "size": self.config.CATEGORY_AGG_SIZE}},
            "brands": {"terms": {"field": "brand.raw", "size": self.config.BRAND_AGG_SIZE}},
            "price_ranges": {
                "range": {
                    "field": "price",
                    "ranges": self.config.PRICE_RANGES
                }
            },
            "discounts": {"terms": {"field": "has_discount", "size": 2}}
        }


class ResponseFormatter:
    """Formats API responses consistently."""
    
    @staticmethod
    def format_search_response(
        products: List[Dict[str, Any]], 
        pagination: Dict[str, Any],
        facets: Dict[str, Any],
        query_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format search response."""
        return {
            'products': products,
            'pagination': pagination,
            'facets': facets,
            'query_info': query_info
        }
    
    @staticmethod
    def format_error_response(error: str, status_code: int) -> Tuple[Dict[str, Any], int]:
        """Format error response."""
        return {'error': error}, status_code
    
    @staticmethod
    def extract_facets(aggregations: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract facet information from Elasticsearch aggregations."""
        facets = {}
        
        # Extract term aggregations
        for field in ['stores', 'categories', 'brands', 'discounts']:
            if field in aggregations:
                facets[field] = [
                    {'key': bucket['key'], 'count': bucket['doc_count']}
                    for bucket in aggregations[field]['buckets']
                ]
        
        # Extract price ranges
        if 'price_ranges' in aggregations:
            facets['price_ranges'] = [
                {'key': bucket['key'], 'count': bucket['doc_count']}
                for bucket in aggregations['price_ranges']['buckets']
                if bucket['doc_count'] > 0
            ]
        
        return facets


class ProductSearchService:
    """Service layer for product search operations."""
    
    def __init__(self, es_manager, config: SearchConfig = SearchConfig()):
        self.es_manager = es_manager
        self.config = config
        self.query_builder = SearchQueryBuilder(config)
        self.formatter = ResponseFormatter()
        self.logger = logging.getLogger(__name__)
    
    def search(self, search_request: SearchRequest) -> Dict[str, Any]:
        """Execute product search."""
        try:
            # Build query
            query = (SearchQueryBuilder(self.config)
                    .add_search_query(search_request.query)
                    .add_filters(search_request.filters)
                    .add_sorting(search_request.sort_by)
                    .build())
            
            # Calculate pagination
            from_index = (search_request.page - 1) * search_request.per_page
            
            # Execute search
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query,
                size=search_request.per_page,
                from_=from_index
            )
            
            # Process results
            products = ProductSearchService._process_products(response['hits']['hits'])
            pagination = ProductSearchService._build_pagination(
                response['hits']['total']['value'],
                search_request.page,
                search_request.per_page
            )
            facets = self.formatter.extract_facets(response.get('aggregations', {}))
            query_info = {
                'query': search_request.query,
                'filters': search_request.filters,
                'sort_by': search_request.sort_by.value,
                'took': response.get('took', 0)
            }
            
            return self.formatter.format_search_response(
                products, pagination, facets, query_info
            )
            
        except Exception as search_error:
            self.logger.error(f"Search error: {search_error}")
            return self.formatter.format_search_response(
                [], 
                self._empty_pagination(), 
                {}, 
                {'error': str(search_error)}
            )
    
    def get_suggestions(self, query: str, size: int = 10) -> List[str]:
        """Get autocomplete suggestions for search queries."""
        try:
            suggest_query = {
                "suggest": {
                    "product_suggest": {
                        "prefix": query,
                        "completion": {
                            "field": "suggest",
                            "size": size
                        }
                    }
                }
            }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=suggest_query
            )
            
            suggestions = []
            for option in response['suggest']['product_suggest'][0]['options']:
                suggestions.append(option['text'])
            
            return suggestions
            
        except Exception as e:
            self.logger.error(f"Suggestion error: {e}")
            return []
    
    def get_product_by_id(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific product by ID."""
        try:
            response = self.es_manager.es.get(
                index=self.es_manager.index_name,
                id=product_id
            )
            return response['_source']
            
        except Exception as e:
            self.logger.error(f"Get product error: {e}")
            return None
    
    def compare_prices(self, product_name: str) -> List[Dict[str, Any]]:
        """Compare prices for similar products across stores."""
        try:
            # Build comparison query
            query = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match_phrase": {
                                    "name": {
                                        "query": product_name,
                                        "boost": 10
                                    }
                                }
                            },
                            {
                                "match": {
                                    "name": {
                                        "query": product_name,
                                        "operator": "and",
                                        "boost": 5
                                    }
                                }
                            },
                            {
                                "match": {
                                    "name": {
                                        "query": product_name,
                                        "fuzziness": "1",
                                        "boost": 2
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                "sort": [{"price": {"order": "asc"}}],
                "size": 2000
            }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            products = []
            for hit in response['hits']['hits']:
                product = hit['_source']
                product['_score'] = hit['_score']
                products.append(product)
            
            return products
            
        except Exception as e:
            self.logger.error(f"Price comparison error: {e}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        try:
            stats = self.es_manager.es.count(index=self.es_manager.index_name)
            
            # Get store and category breakdown
            agg_query = {
                "size": 0,
                "aggs": {
                    "stores": {"terms": {"field": "store", "size": 10}},
                    "categories": {"terms": {"field": "category.raw", "size": 10}}
                }
            }
            
            agg_response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=agg_query
            )
            
            return {
                'total_products': stats['count'],
                'stores': [
                    {'name': bucket['key'], 'count': bucket['doc_count']}
                    for bucket in agg_response['aggregations']['stores']['buckets']
                ],
                'categories': [
                    {'name': bucket['key'], 'count': bucket['doc_count']}
                    for bucket in agg_response['aggregations']['categories']['buckets']
                ],
                'index_name': self.es_manager.index_name,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Stats error: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """Check Elasticsearch connection health."""
        try:
            es_status = self.es_manager.es.ping()
            
            return {
                'status': 'healthy' if es_status else 'unhealthy',
                'elasticsearch': 'connected' if es_status else 'disconnected',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception:
            raise
    
    @staticmethod
    def _process_products(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process Elasticsearch hits into product list."""
        products = []
        for hit in hits:
            product = hit['_source']
            product['_score'] = hit['_score']
            if 'highlight' in hit:
                product['_highlight'] = hit['highlight']
            products.append(product)
        return products
    
    @staticmethod
    def _build_pagination(total: int, page: int, per_page: int) -> Dict[str, Any]:
        """Build pagination metadata."""
        total_pages = (total + per_page - 1) // per_page
        return {
            'current_page': page,
            'per_page': per_page,
            'total_results': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    
    def _empty_pagination(self) -> Dict[str, Any]:
        """Return empty pagination object."""
        return {
            'current_page': 1,
            'per_page': self.config.DEFAULT_PAGE_SIZE,
            'total_results': 0,
            'total_pages': 0,
            'has_next': False,
            'has_prev': False
        }


# Flask Application with thin controllers
def create_app(es_manager):
    """Create Flask application with dependency injection."""
    app = Flask(__name__)
    CORS(app)
    
    # Configure Flask
    app.config['JSON_AS_ASCII'] = False
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize services
    search_service = ProductSearchService(es_manager)
    filter_parser = FilterParser()
    formatter = ResponseFormatter()
    
    @app.route('/api/search', methods=['GET'])
    def search_products():
        """Search products endpoint."""
        try:
            search_request = SearchRequest(
                query=request.args.get('q', ''),
                page=max(1, int(request.args.get('page', 1))),
                per_page=min(
                    int(request.args.get('per_page', SearchConfig.DEFAULT_PAGE_SIZE)), 
                    SearchConfig.MAX_PAGE_SIZE
                ),
                sort_by=SortOption(request.args.get('sort', 'relevance')),
                filters=filter_parser.parse_filters(request.args)
            )
        except (ValueError, KeyError) as invalidError:
            return formatter.format_error_response(f"Invalid parameters: {invalidError}", 400)
        
        results = search_service.search(search_request)
        return jsonify(results)
    
    @app.route('/api/suggestions', methods=['GET'])
    def get_suggestions():
        """Get autocomplete suggestions."""
        query = request.args.get('q', '')
        size = min(int(request.args.get('size', 10)), 20)
        
        suggestions = search_service.get_suggestions(query, size)
        return jsonify({'suggestions': suggestions})
    
    @app.route('/api/product/<product_id>', methods=['GET'])
    def get_product(product_id):
        """Get specific product by ID."""
        product = search_service.get_product_by_id(product_id)
        
        if product:
            return jsonify({'product': product})
        else:
            return formatter.format_error_response('Product not found', 404)
    
    @app.route('/api/compare', methods=['GET'])
    def compare_prices():
        """Compare prices for similar products."""
        product_name = request.args.get('name', '')
        
        if not product_name:
            return formatter.format_error_response('Product name is required', 400)
        
        products = search_service.compare_prices(product_name)
        return jsonify({'products': products})
    
    @app.route('/api/stats', methods=['GET'])
    def get_stats():
        """Get index statistics."""
        try:
            stats = search_service.get_stats()
            return jsonify(stats)
        except Exception as exception:
            return formatter.format_error_response(str(exception), 500)
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        try:
            health = search_service.health_check()
            return jsonify(health)
        except Exception as exception:
            return jsonify({
                'status': 'unhealthy',
                'error': str(exception),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/', methods=['GET'])
    def index():
        """API documentation endpoint."""
        endpoints = {
            'search': '/api/search?q=query&stores=ATB,Varus&page=1&per_page=200&sort=price_asc',
            'suggestions': '/api/suggestions?q=query&size=10',
            'product': '/api/product/<product_id>',
            'compare': '/api/compare?name=product_name',
            'stats': '/api/stats',
            'health': '/api/health'
        }
        
        return jsonify({
            'message': 'Grocery Product Search API',
            'version': '2.0.0',
            'endpoints': endpoints,
            'documentation': 'See API_DOCUMENTATION.md for detailed usage'
        })
    
    return app


# Main entry point
if __name__ == '__main__':
    import sys
    sys.path.append(os.path.dirname(__file__))
    
    from elasticsearch_config import es_manager
    
    # Check Elasticsearch connection on startup
    logger = logging.getLogger(__name__)
    try:
        if es_manager.es.ping():
            logger.info("Connected to Elasticsearch - API ready")
        else:
            logger.warning("Elasticsearch not available - some features may not work")
    except Exception as error:
        logger.error(f"Elasticsearch connection error: {error}")
    
    # Create and run app
    app = create_app(es_manager)
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)