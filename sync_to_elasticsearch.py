#!/usr/bin/env python3
"""
Data synchronization utility to sync grocery products from SQLite to Elasticsearch.
"""

import argparse
import hashlib
import logging
import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Iterator, Tuple

# Add project root to path
sys.path.append(os.path.dirname(__file__))

from config import DATABASE_PATH
from elasticsearch_config import es_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SyncError(Exception):
    """Base exception for sync operations."""
    pass


class DatabaseError(SyncError):
    """Database-related errors."""
    pass


class ElasticsearchError(SyncError):
    """Elasticsearch-related errors."""
    pass


class SQLiteToElasticsearchSync:
    """Synchronize data from SQLite to Elasticsearch with optimized batch processing."""
    
    def __init__(self, db_path: str = DATABASE_PATH, batch_size: int = 1000):
        self.db_path = db_path
        self.es_manager = es_manager
        self.default_batch_size = batch_size
        
    def sync_all_products(self, batch_size: int = None, recreate_index: bool = False) -> Dict[str, int]:
        """Sync all products from SQLite to Elasticsearch."""
        batch_size = batch_size or self.default_batch_size
        logger.info("Starting full product synchronization...")
        
        try:
            # Create/recreate index
            if recreate_index:
                logger.info("Recreating Elasticsearch index...")
                if not self.es_manager.create_index(delete_existing=True):
                    raise ElasticsearchError("Failed to create Elasticsearch index")
            else:
                self.es_manager.create_index(delete_existing=False)
            
            # Get total count
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as total FROM products")
                total_count = cursor.fetchone()['total']
            
            logger.info(f"Found {total_count} products to sync")
            
            stats = {"success": 0, "failed": 0, "total": total_count}
            batch_num = 0
            
            # Process batches using iterator
            query = "SELECT * FROM products ORDER BY scraped_at DESC"
            for batch in self._get_products_iterator(query, batch_size=batch_size):
                batch_num += 1
                logger.info(f"Processing batch {batch_num} ({len(batch)} products)")
                
                success, failed = self._process_products_batch(batch)
                stats["success"] += success
                stats["failed"] += failed
            
            logger.info(f"Sync complete: {stats['success']} successful, {stats['failed']} failed out of {stats['total']} total")
            return stats
            
        except (DatabaseError, ElasticsearchError) as e:
            logger.error(f"Sync error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected sync error: {e}")
            raise SyncError(f"Sync failed: {e}") from e
    
    @contextmanager
    def _get_db_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise DatabaseError(f"Database connection failed: {e}") from e
        finally:
            if conn:
                conn.close()
    
    def _process_products_batch(self, products: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process a batch of products and return success/failure counts."""
        if not products:
            return 0, 0
        
        es_docs = [self._convert_to_es_document(product) for product in products]
        success, failed = self.es_manager.bulk_index_products(es_docs)
        return success, len(failed)
    
    def _get_products_iterator(self, query: str, params: tuple = (), batch_size: int = None) -> Iterator[List[Dict[str, Any]]]:
        """Get an iterator that yields batches of products from the database."""
        batch_size = batch_size or self.default_batch_size
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            batch = []
            for row in cursor:
                batch.append(dict(row))
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            
            if batch:
                yield batch
    
    def sync_recent_products(self, hours: int = 24, batch_size: int = None) -> Dict[str, int]:
        """Sync products updated in the last N hours."""
        batch_size = batch_size or self.default_batch_size
        logger.info(f"Syncing products updated in last {hours} hours...")
        
        try:
            # Create index if it doesn't exist
            self.es_manager.create_index(delete_existing=False)
            
            # Calculate cutoff time using timedelta for better accuracy
            cutoff_time = datetime.now() - timedelta(hours=hours)
            cutoff_str = cutoff_time.isoformat()
            
            # Get recent products count
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) as total FROM products 
                    WHERE scraped_at >= ? OR scraped_at IS NULL
                """, (cutoff_str,))
                total_count = cursor.fetchone()['total']
            
            logger.info(f"Found {total_count} recent products to sync")
            
            stats = {"success": 0, "failed": 0, "total": total_count}
            batch_num = 0
            
            # Process batches using iterator
            query = """
                SELECT * FROM products 
                WHERE scraped_at >= ? OR scraped_at IS NULL
                ORDER BY scraped_at DESC
            """
            for batch in self._get_products_iterator(query, (cutoff_str,), batch_size):
                batch_num += 1
                logger.info(f"Processing recent batch {batch_num} ({len(batch)} products)")
                
                success, failed = self._process_products_batch(batch)
                stats["success"] += success
                stats["failed"] += failed
            
            logger.info(f"Recent sync complete: {stats['success']} successful, {stats['failed']} failed")
            return stats
            
        except (DatabaseError, ElasticsearchError) as e:
            logger.error(f"Recent sync error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected recent sync error: {e}")
            raise SyncError(f"Recent sync failed: {e}") from e
    
    def sync_store_products(self, store_name: str, batch_size: int = None) -> Dict[str, int]:
        """Sync products from a specific store."""
        batch_size = batch_size or self.default_batch_size
        logger.info(f"Syncing products from {store_name}...")
        
        try:
            # Create index if it doesn't exist
            self.es_manager.create_index(delete_existing=False)
            
            # Get store products count
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as total FROM products WHERE store = ?", (store_name,))
                total_count = cursor.fetchone()['total']
            
            logger.info(f"Found {total_count} products from {store_name}")
            
            stats = {"success": 0, "failed": 0, "total": total_count}
            batch_num = 0
            
            # Process batches using iterator
            query = "SELECT * FROM products WHERE store = ? ORDER BY scraped_at DESC"
            for batch in self._get_products_iterator(query, (store_name,), batch_size):
                batch_num += 1
                logger.info(f"Processing store batch {batch_num} ({len(batch)} products)")
                
                success, failed = self._process_products_batch(batch)
                stats["success"] += success
                stats["failed"] += failed
            
            logger.info(f"Store sync complete: {stats['success']} successful, {stats['failed']} failed")
            return stats
            
        except (DatabaseError, ElasticsearchError) as e:
            logger.error(f"Store sync error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected store sync error: {e}")
            raise SyncError(f"Store sync failed: {e}") from e
    
    
    @staticmethod
    def _convert_to_es_document(sqlite_row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert SQLite row to Elasticsearch document with proper type conversion.
        
        Args:
            sqlite_row: Dictionary containing SQLite row data
            
        Returns:
            Dictionary formatted for Elasticsearch indexing
        """
        # Generate product_id if not present
        if not sqlite_row.get('product_id'):
            store = sqlite_row.get('store', '')
            url = sqlite_row.get('url', '')
            if store and url:
                sqlite_row['product_id'] = hashlib.md5(f"{store}:{url}".encode()).hexdigest()
        
        # Helper function for safe numeric conversion
        def safe_float(value: Any) -> Optional[float]:
            if value is None or value == '':
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        def safe_int(value: Any) -> Optional[int]:
            if value is None or value == '':
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Map SQLite fields to Elasticsearch document
        doc = {
            # Core fields
            'product_id': sqlite_row.get('product_id'),
            'name': sqlite_row.get('name'),
            'price': safe_float(sqlite_row.get('price')) or 0.0,
            'store': sqlite_row.get('store'),
            'category': sqlite_row.get('category'),
            'subcategory': sqlite_row.get('subcategory'),
            'url': sqlite_row.get('url'),
            
            # Extended fields (may not exist in all records)
            'brand': sqlite_row.get('brand'),
            'description': sqlite_row.get('description'),
            'image_url': sqlite_row.get('image_url'),
            'original_price': safe_float(sqlite_row.get('original_price')),
            'discount_percentage': safe_float(sqlite_row.get('discount_percentage')) or 0.0,
            'discount_amount': safe_float(sqlite_row.get('discount_amount')),
            'unit_price': safe_float(sqlite_row.get('unit_price')),
            'rating': safe_float(sqlite_row.get('rating')),
            'reviews_count': safe_int(sqlite_row.get('reviews_count')) or 0,
            'availability': sqlite_row.get('availability', 'unknown'),
            'stock_quantity': safe_int(sqlite_row.get('stock_quantity')),
            'promo_tags': sqlite_row.get('promo_tags'),
            'store_category': sqlite_row.get('store_category'),
            'store_subcategory': sqlite_row.get('store_subcategory'),
            
            # Timestamps
            'scraped_at': sqlite_row.get('scraped_at'),
            'created_at': sqlite_row.get('scraped_at'),  # Use scraped_at as created_at
        }
        
        # Remove None values to keep the document clean
        return {k: v for k, v in doc.items() if v is not None}
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get synchronization status and statistics."""
        try:
            # SQLite stats
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) as total FROM products")
                sqlite_total = cursor.fetchone()[0]
                
                cursor.execute("SELECT store, COUNT(*) as count FROM products GROUP BY store")
                sqlite_by_store = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Elasticsearch stats
            es_stats = self.es_manager.es.count(index=self.es_manager.index_name)
            es_total = es_stats.get('count', 0)
            
            # Store breakdown in Elasticsearch
            agg_query = {
                "size": 0,
                "aggs": {
                    "stores": {
                        "terms": {
                            "field": "store",
                            "size": 10
                        }
                    }
                }
            }
            
            es_agg_result = self.es_manager.es.search(
                index=self.es_manager.index_name,
                body=agg_query
            )
            
            es_by_store = {}
            if 'aggregations' in es_agg_result:
                for bucket in es_agg_result['aggregations']['stores']['buckets']:
                    es_by_store[bucket['key']] = bucket['doc_count']
            
            return {
                "sqlite": {
                    "total": sqlite_total,
                    "by_store": sqlite_by_store
                },
                "elasticsearch": {
                    "total": es_total,
                    "by_store": es_by_store
                },
                "sync_status": "synchronized" if sqlite_total == es_total else "out_of_sync",
                "difference": sqlite_total - es_total
            }
            
        except (DatabaseError, ElasticsearchError) as e:
            logger.error(f"Error getting sync status: {e}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error getting sync status: {e}")
            return {"error": f"Unexpected error: {e}"}


def main() -> None:
    """Command line interface for synchronization with comprehensive error handling."""
    parser = argparse.ArgumentParser(
        description="Sync grocery products to Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode=all --recreate-index     # Full sync with index recreation
  %(prog)s --mode=recent --hours=24        # Sync last 24 hours (default)
  %(prog)s --mode=store --store=Varus      # Sync specific store
  %(prog)s --mode=status                   # Check sync status
"""
    )
    parser.add_argument(
        "--mode", 
        choices=["all", "recent", "store", "status"], 
        default="recent", 
        help="Sync mode (default: %(default)s)"
    )
    parser.add_argument(
        "--store", 
        help="Store name for store-specific sync"
    )
    parser.add_argument(
        "--hours", 
        type=int, 
        default=24, 
        help="Hours for recent sync (default: %(default)s)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=1000,
        help="Batch size for processing (default: %(default)s)"
    )
    parser.add_argument(
        "--recreate-index", 
        action="store_true",
        help="Recreate Elasticsearch index (warning: deletes existing data)"
    )
    
    args = parser.parse_args()
    
    try:
        sync = SQLiteToElasticsearchSync(batch_size=args.batch_size)
        
        if args.mode == "all":
            print("Syncing all products...")
            result = sync.sync_all_products(
                batch_size=args.batch_size,
                recreate_index=args.recreate_index
            )
            print(f"Result: {result}")
            
        elif args.mode == "recent":
            print(f"Syncing products from last {args.hours} hours...")
            result = sync.sync_recent_products(
                hours=args.hours,
                batch_size=args.batch_size
            )
            print(f"Result: {result}")
            
        elif args.mode == "store":
            if not args.store:
                parser.error("--store parameter required for store mode")
            print(f"Syncing products from {args.store}...")
            result = sync.sync_store_products(
                store_name=args.store,
                batch_size=args.batch_size
            )
            print(f"Result: {result}")
            
        elif args.mode == "status":
            print("Getting sync status...")
            status = sync.get_sync_status()
            
            if "error" in status:
                print(f"Error getting status: {status['error']}")
                sys.exit(1)
            
            print("Sync Status:")
            print(f"  SQLite total: {status.get('sqlite', {}).get('total', 'N/A')}")
            print(f"  Elasticsearch total: {status.get('elasticsearch', {}).get('total', 'N/A')}")
            print(f"  Status: {status.get('sync_status', 'unknown')}")
            print(f"  Difference: {status.get('difference', 'N/A')}")
            
            print("\nStore breakdown (SQLite):")
            for store, count in status.get('sqlite', {}).get('by_store', {}).items():
                print(f"    {store}: {count:,}")
                
            print("\nStore breakdown (Elasticsearch):")
            for store, count in status.get('elasticsearch', {}).get('by_store', {}).items():
                print(f"    {store}: {count:,}")
    
    except SyncError as e:
        logger.error(f"Sync operation failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Sync operation cancelled by user")
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()