#!/usr/bin/env python3
"""
Advanced analytics and price comparison utilities for grocery data using Elasticsearch.
"""
import logging
import sys
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

# Add project root to path
sys.path.append(os.path.dirname(__file__))

from elasticsearch_config import es_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GroceryAnalytics:
    """Advanced analytics for grocery price data."""
    
    def __init__(self):
        self.es_manager = es_manager
    
    def get_price_trends(self, days: int = 30) -> Dict[str, Any]:
        """Analyze price trends over the specified number of days."""
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            query = {
                "size": 0,
                "query": {
                    "range": {
                        "scraped_at": {
                            "gte": start_date.isoformat(),
                            "lte": end_date.isoformat()
                        }
                    }
                },
                "aggs": {
                    "price_over_time": {
                        "date_histogram": {
                            "field": "scraped_at",
                            "calendar_interval": "1d"
                        },
                        "aggs": {
                            "avg_price": {"avg": {"field": "price"}},
                            "min_price": {"min": {"field": "price"}},
                            "max_price": {"max": {"field": "price"}}
                        }
                    },
                    "store_trends": {
                        "terms": {"field": "store", "size": 10},
                        "aggs": {
                            "avg_price": {"avg": {"field": "price"}},
                            "total_products": {"value_count": {"field": "product_id"}}
                        }
                    }
                }
            }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            # Process results
            time_trends = []
            for bucket in response['aggregations']['price_over_time']['buckets']:
                time_trends.append({
                    'date': bucket['key_as_string'][:10],  # YYYY-MM-DD format
                    'avg_price': round(bucket['avg_price']['value'] or 0, 2),
                    'min_price': round(bucket['min_price']['value'] or 0, 2),
                    'max_price': round(bucket['max_price']['value'] or 0, 2),
                    'product_count': bucket['doc_count']
                })
            
            store_trends = []
            for bucket in response['aggregations']['store_trends']['buckets']:
                store_trends.append({
                    'store': bucket['key'],
                    'avg_price': round(bucket['avg_price']['value'] or 0, 2),
                    'total_products': bucket['total_products']['value'],
                    'market_share': round((bucket['doc_count'] / response['hits']['total']['value']) * 100, 2)
                })
            
            return {
                'period': f'{days} days',
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'time_trends': time_trends,
                'store_trends': store_trends,
                'total_products_analyzed': response['hits']['total']['value']
            }
            
        except Exception as e:
            logger.error(f"Price trends analysis error: {e}")
            return {'error': str(e)}
    
    def compare_store_prices(self, category: Optional[str] = None) -> Dict[str, Any]:
        """Compare average prices across stores, optionally filtered by category."""
        try:
            query = {
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "store_comparison": {
                        "terms": {"field": "store", "size": 10},
                        "aggs": {
                            "avg_price": {"avg": {"field": "price"}},
                            "median_price": {"percentiles": {"field": "price", "percents": [50]}},
                            "price_stats": {"stats": {"field": "price"}},
                            "discount_rate": {
                                "avg": {"field": "discount_percentage"}
                            },
                            "products_with_discounts": {
                                "filter": {"term": {"has_discount": True}}
                            }
                        }
                    }
                }
            }
            
            # Add category filter if specified
            if category:
                query["query"] = {
                    "term": {"category.raw": category}
                }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            store_comparisons = []
            for bucket in response['aggregations']['store_comparison']['buckets']:
                stats = bucket['price_stats']
                
                store_comparisons.append({
                    'store': bucket['key'],
                    'product_count': bucket['doc_count'],
                    'avg_price': round(bucket['avg_price']['value'] or 0, 2),
                    'median_price': round(bucket['median_price']['values']['50.0'] or 0, 2),
                    'min_price': round(stats['min'] or 0, 2),
                    'max_price': round(stats['max'] or 0, 2),
                    'avg_discount_rate': round(bucket['discount_rate']['value'] or 0, 2),
                    'products_with_discounts': bucket['products_with_discounts']['doc_count'],
                    'discount_percentage': round(
                        (bucket['products_with_discounts']['doc_count'] / bucket['doc_count']) * 100, 2
                    ) if bucket['doc_count'] > 0 else 0
                })
            
            # Sort by average price
            store_comparisons.sort(key=lambda x: x['avg_price'])
            
            return {
                'category': category or 'All categories',
                'store_comparisons': store_comparisons,
                'cheapest_store': store_comparisons[0]['store'] if store_comparisons else None,
                'most_expensive_store': store_comparisons[-1]['store'] if store_comparisons else None,
                'total_products': sum(s['product_count'] for s in store_comparisons)
            }
            
        except Exception as e:
            logger.error(f"Store price comparison error: {e}")
            return {'error': str(e)}
    
    def find_best_deals(self, min_discount: float = 20.0, limit: int = 50) -> List[Dict[str, Any]]:
        """Find products with the best discounts."""
        try:
            query = {
                "size": limit,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"has_discount": True}},
                            {"range": {"discount_percentage": {"gte": min_discount}}}
                        ]
                    }
                },
                "sort": [
                    {"discount_percentage": {"order": "desc"}},
                    {"price": {"order": "asc"}}
                ]
            }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            deals = []
            for hit in response['hits']['hits']:
                product = hit['_source']
                
                # Calculate savings
                original_price = product.get('original_price', 0)
                current_price = product.get('price', 0)
                savings = original_price - current_price if original_price and current_price else 0
                
                deals.append({
                    'name': product.get('name'),
                    'store': product.get('store'),
                    'category': product.get('category'),
                    'current_price': current_price,
                    'original_price': original_price,
                    'discount_percentage': product.get('discount_percentage', 0),
                    'savings': round(savings, 2),
                    'url': product.get('url'),
                    'scraped_at': product.get('scraped_at')
                })
            
            return deals
            
        except Exception as e:
            logger.error(f"Best deals analysis error: {e}")
            return []
    
    def analyze_category_pricing(self, store: Optional[str] = None) -> Dict[str, Any]:
        """Analyze pricing patterns by category, optionally filtered by store."""
        try:
            query = {
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "categories": {
                        "terms": {"field": "category.raw", "size": 20},
                        "aggs": {
                            "price_stats": {"stats": {"field": "price"}},
                            "avg_discount": {"avg": {"field": "discount_percentage"}},
                            "store_breakdown": {
                                "terms": {"field": "store", "size": 10},
                                "aggs": {
                                    "avg_price": {"avg": {"field": "price"}}
                                }
                            }
                        }
                    }
                }
            }
            
            # Add store filter if specified
            if store:
                query["query"] = {
                    "term": {"store": store}
                }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            categories = []
            for bucket in response['aggregations']['categories']['buckets']:
                stats = bucket['price_stats']
                
                # Get store breakdown
                store_prices = []
                for store_bucket in bucket['store_breakdown']['buckets']:
                    store_prices.append({
                        'store': store_bucket['key'],
                        'avg_price': round(store_bucket['avg_price']['value'] or 0, 2),
                        'product_count': store_bucket['doc_count']
                    })
                
                categories.append({
                    'category': bucket['key'],
                    'product_count': bucket['doc_count'],
                    'avg_price': round(stats['avg'] or 0, 2),
                    'min_price': round(stats['min'] or 0, 2),
                    'max_price': round(stats['max'] or 0, 2),
                    'price_std': round(((stats['sum_of_squares'] or 0) - 
                                     (stats['sum'] or 0) ** 2 / (stats['count'] or 1)) ** 0.5, 2),
                    'avg_discount': round(bucket['avg_discount']['value'] or 0, 2),
                    'store_prices': sorted(store_prices, key=lambda x: x['avg_price'])
                })
            
            # Sort by average price descending
            categories.sort(key=lambda x: x['avg_price'], reverse=True)
            
            return {
                'store_filter': store or 'All stores',
                'categories': categories,
                'most_expensive_category': categories[0]['category'] if categories else None,
                'cheapest_category': categories[-1]['category'] if categories else None
            }
            
        except Exception as e:
            logger.error(f"Category pricing analysis error: {e}")
            return {'error': str(e)}
    
    def find_price_outliers(self, category: str, threshold_multiplier: float = 2.0) -> List[Dict[str, Any]]:
        """Find products with prices significantly different from category average."""
        try:
            # First, get category statistics
            stats_query = {
                "size": 0,
                "query": {"term": {"category.raw": category}},
                "aggs": {
                    "price_stats": {"stats": {"field": "price"}}
                }
            }
            
            stats_response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=stats_query
            )
            
            stats = stats_response['aggregations']['price_stats']
            avg_price = stats['avg'] or 0
            
            if avg_price == 0:
                return []
            
            # Calculate outlier thresholds
            upper_threshold = avg_price * threshold_multiplier
            lower_threshold = avg_price / threshold_multiplier
            
            # Find outliers
            outlier_query = {
                "size": 100,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"category.raw": category}}
                        ],
                        "should": [
                            {"range": {"price": {"gte": upper_threshold}}},
                            {"range": {"price": {"lte": lower_threshold}}}
                        ],
                        "minimum_should_match": 1
                    }
                },
                "sort": [{"price": {"order": "desc"}}]
            }
            
            outlier_response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=outlier_query
            )
            
            outliers = []
            for hit in outlier_response['hits']['hits']:
                product = hit['_source']
                price = product.get('price', 0)
                
                # Determine outlier type
                outlier_type = 'expensive' if price >= upper_threshold else 'cheap'
                deviation = ((price - avg_price) / avg_price) * 100
                
                outliers.append({
                    'name': product.get('name'),
                    'store': product.get('store'),
                    'price': price,
                    'category_avg_price': round(avg_price, 2),
                    'deviation_percent': round(deviation, 2),
                    'outlier_type': outlier_type,
                    'url': product.get('url')
                })
            
            return outliers
            
        except Exception as e:
            logger.error(f"Price outlier analysis error: {e}")
            return []
    
    def get_market_insights(self) -> Dict[str, Any]:
        """Get overall market insights and statistics."""
        try:
            query = {
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "overall_stats": {"stats": {"field": "price"}},
                    "store_count": {"cardinality": {"field": "store"}},
                    "category_count": {"cardinality": {"field": "category.raw"}},
                    "brand_count": {"cardinality": {"field": "brand.raw"}},
                    "discount_stats": {
                        "filter": {"term": {"has_discount": True}},
                        "aggs": {
                            "avg_discount": {"avg": {"field": "discount_percentage"}},
                            "max_discount": {"max": {"field": "discount_percentage"}}
                        }
                    },
                    "price_distribution": {
                        "histogram": {
                            "field": "price",
                            "interval": 50
                        }
                    },
                    "top_categories": {
                        "terms": {"field": "category.raw", "size": 10}
                    },
                    "top_brands": {
                        "terms": {"field": "brand.raw", "size": 10}
                    }
                }
            }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            aggs = response['aggregations']
            overall_stats = aggs['overall_stats']
            discount_stats = aggs['discount_stats']
            
            # Calculate additional metrics
            total_products = response['hits']['total']['value']
            products_with_discounts = discount_stats['doc_count']
            discount_rate = (products_with_discounts / total_products) * 100 if total_products > 0 else 0
            
            return {
                'total_products': total_products,
                'unique_stores': aggs['store_count']['value'],
                'unique_categories': aggs['category_count']['value'],
                'unique_brands': aggs['brand_count']['value'],
                'price_statistics': {
                    'avg_price': round(overall_stats['avg'] or 0, 2),
                    'min_price': round(overall_stats['min'] or 0, 2),
                    'max_price': round(overall_stats['max'] or 0, 2),
                    'total_value': round(overall_stats['sum'] or 0, 2)
                },
                'discount_statistics': {
                    'products_with_discounts': products_with_discounts,
                    'discount_rate_percent': round(discount_rate, 2),
                    'avg_discount_percent': round(discount_stats['avg_discount']['value'] or 0, 2),
                    'max_discount_percent': round(discount_stats['max_discount']['value'] or 0, 2)
                },
                'top_categories': [
                    {'name': bucket['key'], 'count': bucket['doc_count']}
                    for bucket in aggs['top_categories']['buckets']
                ],
                'top_brands': [
                    {'name': bucket['key'], 'count': bucket['doc_count']}
                    for bucket in aggs['top_brands']['buckets']
                ],
                'price_distribution': [
                    {'range': f"₴{bucket['key']}-{bucket['key'] + 50}", 'count': bucket['doc_count']}
                    for bucket in aggs['price_distribution']['buckets']
                    if bucket['doc_count'] > 0
                ]
            }
            
        except Exception as e:
            logger.error(f"Market insights error: {e}")
            return {'error': str(e)}


class PriceComparison:
    """Price comparison utilities."""
    
    def __init__(self):
        self.es_manager = es_manager
        self.analytics = GroceryAnalytics()
    
    def compare_product_prices(self, product_name: str, fuzzy: bool = True) -> Dict[str, Any]:
        """Compare prices for a specific product across stores."""
        try:
            # Build search query
            if fuzzy:
                query = {
                    "query": {
                        "multi_match": {
                            "query": product_name,
                            "fields": ["name^3", "search_text"],
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                            "minimum_should_match": "75%"
                        }
                    },
                    "sort": [{"price": {"order": "asc"}}],
                    "size": 50
                }
            else:
                query = {
                    "query": {
                        "bool": {
                            "should": [
                                {"match_phrase": {"name": product_name}},
                                {"wildcard": {"name": f"*{product_name.lower()}*"}}
                            ]
                        }
                    },
                    "sort": [{"price": {"order": "asc"}}],
                    "size": 50
                }
            
            response = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=query
            )
            
            products = []
            store_prices = defaultdict(list)
            
            for hit in response['hits']['hits']:
                product = hit['_source']
                store = product.get('store')
                price = product.get('price', 0)
                
                products.append({
                    'name': product.get('name'),
                    'store': store,
                    'price': price,
                    'original_price': product.get('original_price'),
                    'discount_percentage': product.get('discount_percentage', 0),
                    'url': product.get('url'),
                    'similarity_score': hit['_score']
                })
                
                store_prices[store].append(price)
            
            # Calculate store statistics
            store_stats = {}
            for store, prices in store_prices.items():
                store_stats[store] = {
                    'min_price': min(prices),
                    'max_price': max(prices),
                    'avg_price': statistics.mean(prices),
                    'median_price': statistics.median(prices),
                    'product_count': len(prices)
                }
            
            # Find best deals
            cheapest = min(products, key=lambda x: x['price']) if products else None
            best_discount = max(products, key=lambda x: x['discount_percentage']) if products else None
            
            return {
                'query': product_name,
                'total_matches': len(products),
                'products': products,
                'store_statistics': store_stats,
                'cheapest_option': cheapest,
                'best_discount': best_discount if best_discount and best_discount['discount_percentage'] > 0 else None,
                'price_range': {
                    'min': min(p['price'] for p in products) if products else 0,
                    'max': max(p['price'] for p in products) if products else 0
                }
            }
            
        except Exception as e:
            logger.error(f"Product price comparison error: {e}")
            return {'error': str(e)}
    
    def create_shopping_list_comparison(self, product_names: List[str]) -> Dict[str, Any]:
        """Compare total cost of a shopping list across different stores."""
        try:
            store_totals = defaultdict(lambda: {'total': 0, 'products': [], 'missing_products': []})
            all_comparisons = {}
            
            for product_name in product_names:
                comparison = self.compare_product_prices(product_name)
                all_comparisons[product_name] = comparison
                
                if 'error' in comparison:
                    continue
                
                # Find cheapest option for each store
                store_cheapest = defaultdict(lambda: {'price': float('inf'), 'product': None})
                
                for product in comparison.get('products', []):
                    store = product['store']
                    if product['price'] < store_cheapest[store]['price']:
                        store_cheapest[store] = {'price': product['price'], 'product': product}
                
                # Add to store totals
                for store, data in store_cheapest.items():
                    if data['product']:
                        store_totals[store]['total'] += data['price']
                        store_totals[store]['products'].append({
                            'name': product_name,
                            'price': data['price'],
                            'product_details': data['product']
                        })
                    else:
                        store_totals[store]['missing_products'].append(product_name)
            
            # Convert to regular dict and sort by total
            store_totals = dict(store_totals)
            sorted_stores = sorted(
                store_totals.items(),
                key=lambda x: x[1]['total']
            )
            
            return {
                'shopping_list': product_names,
                'store_comparisons': {
                    store: {
                        'total_cost': round(data['total'], 2),
                        'products_found': len(data['products']),
                        'products_missing': len(data['missing_products']),
                        'products': data['products'],
                        'missing_products': data['missing_products']
                    }
                    for store, data in sorted_stores
                },
                'cheapest_store': sorted_stores[0][0] if sorted_stores else None,
                'most_expensive_store': sorted_stores[-1][0] if sorted_stores else None,
                'potential_savings': round(
                    sorted_stores[-1][1]['total'] - sorted_stores[0][1]['total'], 2
                ) if len(sorted_stores) >= 2 else 0,
                'detailed_comparisons': all_comparisons
            }
            
        except Exception as e:
            logger.error(f"Shopping list comparison error: {e}")
            return {'error': str(e)}


# CLI interface
def main():
    """Command line interface for analytics."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Grocery analytics and price comparison")
    parser.add_argument("--action", 
                       choices=["trends", "compare-stores", "deals", "categories", "outliers", "insights", "compare-product", "shopping-list"],
                       required=True, help="Analysis action to perform")
    parser.add_argument("--days", type=int, default=30, help="Days for trend analysis")
    parser.add_argument("--category", help="Category filter")
    parser.add_argument("--store", help="Store filter")
    parser.add_argument("--product", help="Product name for comparison")
    parser.add_argument("--products", nargs="+", help="Product names for shopping list")
    parser.add_argument("--min-discount", type=float, default=20.0, help="Minimum discount percentage")
    
    args = parser.parse_args()
    
    analytics = GroceryAnalytics()
    comparison = PriceComparison()
    
    if args.action == "trends":
        result = analytics.get_price_trends(args.days)
        print(f"Price Trends Analysis ({args.days} days):")
        print(f"Analyzed {result.get('total_products_analyzed', 0)} products")
        print("\nStore Trends:")
        for store in result.get('store_trends', []):
            print(f"  {store['store']}: ₴{store['avg_price']} avg, {store['total_products']} products")
    
    elif args.action == "compare-stores":
        result = analytics.compare_store_prices(args.category)
        print(f"Store Price Comparison - {result.get('category', 'All categories')}:")
        for store in result.get('store_comparisons', []):
            print(f"  {store['store']}: ₴{store['avg_price']} avg, {store['product_count']} products, {store['discount_percentage']}% with discounts")
    
    elif args.action == "deals":
        result = analytics.find_best_deals(args.min_discount)
        print(f"Best Deals (min {args.min_discount}% discount):")
        for deal in result[:10]:
            print(f"  {deal['name'][:50]} - {deal['store']}: ₴{deal['current_price']} ({deal['discount_percentage']}% off)")
    
    elif args.action == "categories":
        result = analytics.analyze_category_pricing(args.store)
        print(f"Category Pricing Analysis - {result.get('store_filter', 'All stores')}:")
        for cat in result.get('categories', [])[:10]:
            print(f"  {cat['category']}: ₴{cat['avg_price']} avg, {cat['product_count']} products")
    
    elif args.action == "insights":
        result = analytics.get_market_insights()
        print("Market Insights:")
        print(f"  Total products: {result.get('total_products', 0)}")
        print(f"  Stores: {result.get('unique_stores', 0)}")
        print(f"  Categories: {result.get('unique_categories', 0)}")
        print(f"  Average price: ₴{result.get('price_statistics', {}).get('avg_price', 0)}")
        print(f"  Discount rate: {result.get('discount_statistics', {}).get('discount_rate_percent', 0)}%")
    
    elif args.action == "compare-product":
        if not args.product:
            print("Error: --product required for product comparison")
            return
        result = comparison.compare_product_prices(args.product)
        print(f"Price Comparison for '{args.product}':")
        print(f"Found {result.get('total_matches', 0)} matches")
        if result.get('cheapest_option'):
            cheapest = result['cheapest_option']
            print(f"Cheapest: {cheapest['name']} at {cheapest['store']} - ₴{cheapest['price']}")
    
    elif args.action == "shopping-list":
        if not args.products:
            print("Error: --products required for shopping list comparison")
            return
        result = comparison.create_shopping_list_comparison(args.products)
        print("Shopping List Comparison:")
        for store, data in result.get('store_comparisons', {}).items():
            print(f"  {store}: ₴{data['total_cost']} ({data['products_found']}/{len(args.products)} products found)")


if __name__ == "__main__":
    main()