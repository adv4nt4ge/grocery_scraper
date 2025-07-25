#!/usr/bin/env python3
"""
Elasticsearch configuration and index mapping for grocery products.
Provides configuration management, connection handling, and index operations.
"""

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError
from typing import Dict, Any, Optional, List, Tuple, Union
import os
import logging
import time
from dataclasses import dataclass, field
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class ElasticsearchConfigError(Exception):
    """Base exception for Elasticsearch configuration errors."""
    pass


class ElasticsearchConnectionError(ElasticsearchConfigError):
    """Elasticsearch connection-related errors."""
    pass


class ElasticsearchIndexError(ElasticsearchConfigError):
    """Elasticsearch index-related errors."""
    pass


@dataclass
class ElasticsearchConfig:
    """Configuration settings for Elasticsearch connection and behavior."""
    
    # Connection settings
    hosts: List[str] = field(default_factory=lambda: [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")])
    api_key: Optional[str] = os.getenv("ELASTICSEARCH_API_KEY")
    username: Optional[str] = os.getenv("ELASTICSEARCH_USERNAME")
    password: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD")
    
    # Timeout and retry settings
    request_timeout: int = int(os.getenv("ELASTICSEARCH_TIMEOUT", "30"))
    max_retries: int = int(os.getenv("ELASTICSEARCH_MAX_RETRIES", "3"))
    retry_on_timeout: bool = True
    
    # SSL settings
    verify_certs: bool = os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() == "true"
    ssl_show_warn: bool = os.getenv("ELASTICSEARCH_SSL_SHOW_WARN", "false").lower() == "true"
    
    # Performance settings
    bulk_chunk_size: int = int(os.getenv("ELASTICSEARCH_BULK_CHUNK_SIZE", "500"))
    bulk_timeout: str = os.getenv("ELASTICSEARCH_BULK_TIMEOUT", "60s")
    
    # Index settings
    default_index_name: str = os.getenv("ELASTICSEARCH_INDEX_NAME", "grocery_products")
    
    def to_client_config(self) -> Dict[str, Any]:
        """Convert to Elasticsearch client configuration."""
        config = {
            "hosts": self.hosts,
            "request_timeout": self.request_timeout,
            "max_retries": self.max_retries,
            "retry_on_timeout": self.retry_on_timeout,
            "verify_certs": self.verify_certs,
            "ssl_show_warn": self.ssl_show_warn,
        }
        
        # Add authentication if provided
        if self.api_key:
            config["api_key"] = self.api_key
        elif self.username and self.password:
            config["http_auth"] = (self.username, self.password)
        
        return config
    
    def validate(self) -> None:
        """Validate configuration settings."""
        if not self.hosts:
            raise ElasticsearchConfigError("At least one Elasticsearch host must be specified")
        
        if self.request_timeout <= 0:
            raise ElasticsearchConfigError("Request timeout must be positive")
        
        if self.max_retries < 0:
            raise ElasticsearchConfigError("Max retries cannot be negative")
        
        if self.bulk_chunk_size <= 0:
            raise ElasticsearchConfigError("Bulk chunk size must be positive")


# Default configuration instance
DEFAULT_CONFIG = ElasticsearchConfig()

# Index settings for optimal grocery product search
# Note: Ukrainian language support requires the analysis-ukrainian plugin:
# docker exec -it <container_name> elasticsearch-plugin install analysis-ukrainian
# If plugin is not available, the stemmer will fall back to "russian" language
INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "product_name_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop_ukrainian",
                        "stop_russian", 
                        "synonym_filter",
                        "stemmer_ukrainian"
                    ]
                },
                "exact_word_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                },
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "autocomplete_filter"
                    ]
                }
            },
            "filter": {
                "stop_ukrainian": {
                    "type": "stop",
                    "stopwords": ["і", "та", "або", "для", "від", "на", "в", "з", "до", "по"]
                },
                "stop_russian": {
                    "type": "stop", 
                    "stopwords": ["и", "или", "для", "от", "на", "в", "с", "до", "по"]
                },
                "synonym_filter": {
                    "type": "synonym",
                    "synonyms": [
                        "молоко,молочко",
                        "хліб,хлеб,булка",
                        "м'ясо,мясо",
                        "овочі,овощи",
                        "фрукти,фрукты"
                    ]
                },
                "stemmer_ukrainian": {
                    "type": "stemmer",
                    "language": "ukrainian"  # Requires analysis-ukrainian plugin
                },
                "autocomplete_filter": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 20
                }
            }
        }
    },
    "mappings": {
        "properties": {
            # Core product fields
            "name": {
                "type": "text",
                "analyzer": "product_name_analyzer",
                "fields": {
                    "raw": {"type": "keyword"},
                    "exact": {
                        "type": "text",
                        "analyzer": "exact_word_analyzer"
                    },
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    }
                }
            },
            "price": {
                "type": "float",
                "index": True
            },
            "store": {
                "type": "keyword",
                "index": True
            },
            "category": {
                "type": "text",
                "analyzer": "product_name_analyzer",
                "fields": {
                    "raw": {"type": "keyword"}
                }
            },
            "subcategory": {
                "type": "text", 
                "analyzer": "product_name_analyzer",
                "fields": {
                    "raw": {"type": "keyword"}
                }
            },
            "url": {
                "type": "keyword",
                "index": False
            },
            
            # Enhanced pricing fields
            "original_price": {
                "type": "float"
            },
            "discount_percentage": {
                "type": "float"
            },
            "discount_amount": {
                "type": "float"
            },
            "unit_price": {
                "type": "float"
            },
            "has_discount": {
                "type": "boolean"
            },
            
            # Product metadata
            "brand": {
                "type": "text",
                "analyzer": "product_name_analyzer",
                "fields": {
                    "raw": {"type": "keyword"}
                }
            },
            "description": {
                "type": "text",
                "analyzer": "product_name_analyzer"
            },
            "image_url": {
                "type": "keyword",
                "index": False
            },
            "product_id": {
                "type": "keyword"
            },
            
            # Ratings and reviews
            "rating": {
                "type": "float"
            },
            "reviews_count": {
                "type": "integer"
            },
            
            # Availability
            "availability": {
                "type": "keyword"
            },
            "stock_quantity": {
                "type": "integer"
            },
            "in_stock": {
                "type": "boolean"
            },
            
            # Promo and tags
            "promo_tags": {
                "type": "text",
                "analyzer": "product_name_analyzer",
                "fields": {
                    "raw": {"type": "keyword"}
                }
            },
            
            # Store-specific fields
            "store_category": {
                "type": "keyword"
            },
            "store_subcategory": {
                "type": "keyword"
            },
            
            # Timestamps
            "scraped_at": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd'T'HH:mm:ss.SSSSSS||epoch_millis"
            },
            "created_at": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd'T'HH:mm:ss.SSSSSS||epoch_millis"
            },
            "updated_at": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd'T'HH:mm:ss.SSSSSS||epoch_millis"
            },
            
            # Search optimization fields
            "search_text": {
                "type": "text",
                "analyzer": "product_name_analyzer"
            },
            "suggest": {
                "type": "completion",
                "analyzer": "product_name_analyzer"
            },
            
            # Price comparison fields
            "price_rank": {
                "type": "integer"
            },
            "price_percentile": {
                "type": "float"
            },
            
            # Location (if needed for local store search)
            "location": {
                "type": "geo_point"
            }
        }
    }
}

class ElasticsearchManager:
    """Manager class for Elasticsearch operations with improved error handling and connection management."""
    
    def __init__(self, 
                 index_name: Optional[str] = None, 
                 config: Optional[ElasticsearchConfig] = None):
        """Initialize Elasticsearch manager.
        
        Args:
            index_name: Name of the Elasticsearch index to use
            config: Elasticsearch configuration settings
        """
        self.config = config or DEFAULT_CONFIG
        self.config.validate()
        
        self.index_name = index_name or self.config.default_index_name
        self.es: Optional[Elasticsearch] = None
        self._connected = False
        
        # Initialize connection
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to Elasticsearch with retry logic."""
        max_attempts = self.config.max_retries + 1
        
        for attempt in range(max_attempts):
            try:
                self.es = Elasticsearch(**self.config.to_client_config())
                
                if self.es.ping():
                    self._connected = True
                    logger.info(f"Connected to Elasticsearch on attempt {attempt + 1}")
                    return
                else:
                    logger.warning(f"Elasticsearch ping failed on attempt {attempt + 1}")
                    
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                
                if attempt < max_attempts - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying connection in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise ElasticsearchConnectionError(f"Failed to connect to Elasticsearch after {max_attempts} attempts") from e
    
    @contextmanager
    def _ensure_connection(self):
        """Context manager to ensure Elasticsearch connection is available."""
        if not self._connected or not self.es:
            self._connect()
        
        try:
            yield self.es
        except (ConnectionError, TransportError) as e:
            logger.warning(f"Connection issue detected, attempting reconnection: {e}")
            self._connected = False
            self._connect()
            yield self.es
    
    def create_index(self, delete_existing: bool = False) -> bool:
        """Create the grocery products index with proper error handling.
        
        Args:
            delete_existing: Whether to delete existing index before creating
            
        Returns:
            True if index was created or already exists, False on error
            
        Raises:
            ElasticsearchIndexError: If index operations fail
        """
        try:
            with self._ensure_connection() as es:
                # Delete existing index if requested
                if delete_existing and es.indices.exists(index=self.index_name):
                    es.indices.delete(index=self.index_name)
                    logger.info(f"Deleted existing index: {self.index_name}")
                
                # Create index if it doesn't exist
                if not es.indices.exists(index=self.index_name):
                    es.indices.create(
                        index=self.index_name,
                        body=INDEX_SETTINGS
                    )
                    logger.info(f"Created index: {self.index_name}")
                    
                    # Wait for index to be ready
                    es.cluster.health(
                        index=self.index_name,
                        wait_for_status="yellow",
                        timeout="30s"
                    )
                    return True
                else:
                    logger.info(f"Index {self.index_name} already exists")
                    return True
                    
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise ElasticsearchIndexError(f"Failed to create index {self.index_name}: {e}") from e
    
    def index_product(self, product_data: Dict[str, Any]) -> bool:
        """Index a single product."""
        try:
            # Prepare document for indexing
            doc = self._prepare_document(product_data)
            
            result = self.es.index(
                index=self.index_name,
                id=product_data.get('product_id'),
                body=doc
            )
            
            return result['result'] in ['created', 'updated']
            
        except Exception as e:
            logger.error(f"Error indexing product: {e}")
            return False
    
    def bulk_index_products(self, products: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
        """Bulk index multiple products with improved error handling and reporting.
        
        Args:
            products: List of product dictionaries to index
            
        Returns:
            Tuple of (success_count, failed_documents)
        """
        if not products:
            logger.warning("No products provided for bulk indexing")
            return 0, []
        
        from elasticsearch.helpers import bulk, BulkIndexError
        
        def generate_docs():
            for product in products:
                try:
                    doc = self._prepare_document(product)
                    yield {
                        "_index": self.index_name,
                        "_id": product.get('product_id'),
                        "_source": doc
                    }
                except Exception as e:
                    logger.error(f"Error preparing document for product {product.get('name', 'Unknown')}: {e}")
                    continue
        
        try:
            with self._ensure_connection() as es:
                success, failed = bulk(
                    es,
                    generate_docs(),
                    chunk_size=self.config.bulk_chunk_size,
                    timeout=self.config.bulk_timeout,
                    max_retries=self.config.max_retries,
                    initial_backoff=2,
                    max_backoff=600
                )
                
                # Log results
                if failed:
                    logger.error(f"Bulk indexing: {len(failed)} of {len(products)} documents failed")
                    
                    # Log sample of failures for debugging
                    for i, error in enumerate(failed[:3]):
                        logger.error(f"Failed document {i+1}: {error}")
                    
                    if len(failed) > 3:
                        logger.error(f"... and {len(failed) - 3} more failures")
                else:
                    logger.info(f"Successfully bulk indexed {success} products")
                    
                return success, failed
                
        except BulkIndexError as e:
            logger.error(f"Bulk indexing error: {len(e.errors)} documents failed")
            return len(products) - len(e.errors), e.errors
        except Exception as e:
            logger.error(f"Unexpected bulk indexing error: {e}")
            return 0, []
    
    def _prepare_document(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare product data for Elasticsearch indexing."""
        doc = product_data.copy()
        
        # Add computed fields
        doc['has_discount'] = bool(doc.get('discount_percentage', 0) > 0)
        doc['in_stock'] = doc.get('availability') != 'out_of_stock'
        
        # Create search text combining relevant fields
        search_fields = [
            doc.get('name', ''),
            doc.get('brand', ''),
            doc.get('category', ''),
            doc.get('subcategory', ''),
            doc.get('description', '')
        ]
        doc['search_text'] = ' '.join(filter(None, search_fields))
        
        # Create autocomplete suggestions
        name = doc.get('name', '')
        if name:
            doc['suggest'] = {
                'input': [name],
                'weight': 1
            }
        
        # Add timestamp
        doc['updated_at'] = doc.get('scraped_at')
        
        return doc
    
    def search_products(self, query: str, filters: Dict[str, Any] = None, 
                       size: int = 20, from_: int = 0) -> Dict[str, Any]:
        """Search products with advanced filtering."""
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "name^3",
                                    "brand^2", 
                                    "category^1.5",
                                    "search_text"
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    ],
                    "filter": []
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"price": {"order": "asc"}}
            ],
            "size": size,
            "from": from_,
            "highlight": {
                "fields": {
                    "name": {},
                    "description": {}
                }
            }
        }
        
        # Add filters
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    body["query"]["bool"]["filter"].append({
                        "terms": {field: value}
                    })
                else:
                    body["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })
        
        try:
            return self.es.search(index=self.index_name, body=body)
        except Exception as e:
            logger.error(f"Search error: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def check_ukrainian_plugin(self) -> bool:
        """Check if the analysis-ukrainian plugin is installed.
        
        Returns:
            True if Ukrainian plugin is available, False otherwise
        """
        try:
            with self._ensure_connection() as es:
                plugins = es.cat.plugins(format='json')
                for plugin in plugins:
                    if plugin.get('component') == 'analysis-ukrainian':
                        return True
                return False
        except Exception as e:
            logger.warning(f"Could not check for Ukrainian plugin: {e}")
            return False
    
    def create_index_with_fallback(self, delete_existing: bool = False) -> bool:
        """Create index with Ukrainian plugin detection and fallback to Russian.
        
        Args:
            delete_existing: Whether to delete existing index before creating
            
        Returns:
            True if index was created successfully
        """
        # Check if Ukrainian plugin is available
        has_ukrainian_plugin = self.check_ukrainian_plugin()
        
        if not has_ukrainian_plugin:
            logger.warning("analysis-ukrainian plugin not found, falling back to Russian stemmer")
            # Create a modified INDEX_SETTINGS with Russian fallback
            import copy
            fallback_settings = copy.deepcopy(INDEX_SETTINGS)
            fallback_settings["settings"]["analysis"]["filter"]["stemmer_ukrainian"]["language"] = "russian"
            
            try:
                with self._ensure_connection() as es:
                    if delete_existing and es.indices.exists(index=self.index_name):
                        es.indices.delete(index=self.index_name)
                        logger.info(f"Deleted existing index: {self.index_name}")
                    
                    if not es.indices.exists(index=self.index_name):
                        es.indices.create(
                            index=self.index_name,
                            body=fallback_settings
                        )
                        logger.info(f"Created index with Russian stemmer fallback: {self.index_name}")
                        
                        # Wait for index to be ready
                        es.cluster.health(
                            index=self.index_name,
                            wait_for_status="yellow",
                            timeout="30s"
                        )
                        return True
                    else:
                        logger.info(f"Index {self.index_name} already exists")
                        return True
                        
            except Exception as e:
                logger.error(f"Error creating index with fallback: {e}")
                raise ElasticsearchIndexError(f"Failed to create index {self.index_name}: {e}") from e
        else:
            logger.info("analysis-ukrainian plugin detected, using native Ukrainian stemmer")
            return self.create_index(delete_existing=delete_existing)

    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check of Elasticsearch connection and index."""
        health_status = {
            "connected": False,
            "cluster_health": None,
            "index_exists": False,
            "index_health": None,
            "document_count": 0,
            "errors": []
        }
        
        try:
            with self._ensure_connection() as es:
                # Test connection
                if es.ping():
                    health_status["connected"] = True
                    
                    # Get cluster health
                    cluster_health = es.cluster.health()
                    health_status["cluster_health"] = cluster_health.get("status")
                    
                    # Check index
                    if es.indices.exists(index=self.index_name):
                        health_status["index_exists"] = True
                        
                        # Get index stats
                        index_stats = es.indices.stats(index=self.index_name)
                        health_status["document_count"] = index_stats["indices"][self.index_name]["total"]["docs"]["count"]
                        
                        # Get index health
                        index_health = es.cluster.health(index=self.index_name)
                        health_status["index_health"] = index_health.get("status")
                    
                else:
                    health_status["errors"].append("Elasticsearch ping failed")
                    
        except Exception as e:
            health_status["errors"].append(f"Health check failed: {e}")
            logger.error(f"Health check error: {e}")
        
        return health_status
    
    def get_suggestions(self, query: str, size: int = 10) -> List[str]:
        """Get autocomplete suggestions for a query.
        
        Args:
            query: Partial query string
            size: Number of suggestions to return
            
        Returns:
            List of suggestion strings
        """
        try:
            with self._ensure_connection() as es:
                body = {
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
                
                result = es.search(index=self.index_name, body=body)
                suggestions = []
                
                for option in result["suggest"]["product_suggest"][0]["options"]:
                    suggestions.append(option["text"])
                
                return suggestions
                
        except Exception as e:
            logger.error(f"Error getting suggestions for '{query}': {e}")
            return []


# Global instance with default configuration
es_manager = ElasticsearchManager()