import pymongo
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError
from bson import ObjectId
from typing import Dict, Any, List, Optional, Union
from contextlib import contextmanager
import threading
from urllib.parse import quote_plus
from scripts.utils.logger import log

class MongoClient:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MongoDB client with connection pooling support
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        self._client = None
        self._database = None
        self._lock = threading.Lock()
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize the MongoDB connection"""
        try:
            # Build connection string
            connection_string = self._build_connection_string()
            
            # Connection options
            options = {
                'maxPoolSize': self.config.get('max_pool_size', 100),
                'minPoolSize': self.config.get('min_pool_size', 0),
                'connectTimeoutMS': self.config.get('connect_timeout_ms', 20000),
                'serverSelectionTimeoutMS': self.config.get('server_selection_timeout_ms', 30000),
                'heartbeatFrequencyMS': self.config.get('heartbeat_frequency_ms', 10000)
            }
            
            # Add optional timeout settings only if they have valid values
            max_idle_time = self.config.get('max_idle_time_ms', 0)
            if max_idle_time > 0:
                options['maxIdleTimeMS'] = max_idle_time
                
            socket_timeout = self.config.get('socket_timeout_ms', 0)
            if socket_timeout > 0:
                options['socketTimeoutMS'] = socket_timeout
            
            # SSL options
            if self.config.get('ssl', False):
                options['ssl'] = True
                options['ssl_cert_reqs'] = getattr(pymongo.ssl, self.config.get('ssl_cert_reqs', 'CERT_NONE'))
            
            self._client = PyMongoClient(connection_string, **options)
            self._database = self._client[self.config['database']]
            
            # Test connection
            self._client.admin.command('ping')
            log.info(f"MongoDB connection initialized successfully to database: {self.config['database']}")
            
        except Exception as e:
            log.error(f"Failed to initialize MongoDB connection: {e}")
            raise
    
    def _build_connection_string(self) -> str:
        """Build MongoDB connection string"""
        # Authentication part with URL encoding
        if self.config.get('username') and self.config.get('password'):
            # URL encode username and password to handle special characters
            encoded_username = quote_plus(self.config['username'])
            encoded_password = quote_plus(self.config['password'])
            auth_part = f"{encoded_username}:{encoded_password}@"
        else:
            auth_part = ""
        
        # Base URL
        url = f"mongodb://{auth_part}{self.config['host']}:{self.config['port']}/{self.config['database']}"
        
        # Query parameters
        params = []
        if self.config.get('auth_source') and self.config.get('username'):
            params.append(f"authSource={self.config['auth_source']}")
        if self.config.get('replica_set'):
            params.append(f"replicaSet={self.config['replica_set']}")
        if self.config.get('ssl', False):
            params.append("ssl=true")
        
        if params:
            url += "?" + "&".join(params)
        
        return url
    
    @contextmanager
    def get_collection(self, collection_name: str):
        """Get a collection with error handling"""
        try:
            collection = self._database[collection_name]
            yield collection
        except Exception as e:
            log.error(f"Error accessing collection {collection_name}: {e}")
            raise
    
    def insert_data(self, collection_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]], 
                document_id: Optional[str] = None) -> bool:
        """
        Insert data into a collection
        
        Args:
            collection_name: Name of the collection
            data: Document or list of documents to insert
            document_id: Optional custom document ID. If not provided, MongoDB will generate one automatically
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_collection(collection_name) as collection:
                if isinstance(data, list):
                    # For list of documents, document_id is ignored as it would apply to all documents
                    result = collection.insert_many(data)
                    log.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
                else:
                    # For single document, set custom ID if provided
                    if document_id:
                        data['_id'] = ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id
                    result = collection.insert_one(data)
                    log.info(f"Inserted document with ID {result.inserted_id} into {collection_name}")
                return True
        except Exception as e:
            log.error(f"Failed to insert data into {collection_name}: {e}")
            return False
    
    def update_data(self, collection_name: str, filter_dict: Dict[str, Any], 
                   update_dict: Dict[str, Any], upsert: bool = False, 
                   update_many: bool = False) -> bool:
        """
        Update data in a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria for documents to update
            update_dict: Update operations (should include operators like $set, $inc, etc.)
            upsert: Whether to insert if no document matches
            update_many: Whether to update multiple documents
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_collection(collection_name) as collection:
                if update_many:
                    result = collection.update_many(filter_dict, update_dict, upsert=upsert)
                    log.info(f"Updated {result.modified_count} documents in {collection_name}")
                else:
                    result = collection.update_one(filter_dict, update_dict, upsert=upsert)
                    log.info(f"Updated {result.modified_count} document in {collection_name}")
                return result.modified_count > 0 or (upsert and result.upserted_id is not None)
        except Exception as e:
            log.error(f"Failed to update data in {collection_name}: {e}")
            return False
    
    def delete_data(self, collection_name: str, filter_dict: Dict[str, Any], 
                   delete_many: bool = False) -> bool:
        """
        Delete data from a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria for documents to delete
            delete_many: Whether to delete multiple documents
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_collection(collection_name) as collection:
                if delete_many:
                    result = collection.delete_many(filter_dict)
                    log.info(f"Deleted {result.deleted_count} documents from {collection_name}")
                else:
                    result = collection.delete_one(filter_dict)
                    log.info(f"Deleted {result.deleted_count} document from {collection_name}")
                return result.deleted_count > 0
        except Exception as e:
            log.error(f"Failed to delete data from {collection_name}: {e}")
            return False
    
    def find_one(self, collection_name: str, filter_dict: Dict[str, Any] = None, 
                projection: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Find a single document in a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria
            projection: Fields to include/exclude
            
        Returns:
            Dict or None: Document as dictionary or None if not found
        """
        try:
            with self.get_collection(collection_name) as collection:
                result = collection.find_one(filter_dict or {}, projection)
                if result:
                    # Convert ObjectId to string for JSON serialization
                    if '_id' in result:
                        result['_id'] = str(result['_id'])
                return result
        except Exception as e:
            log.error(f"Failed to find document in {collection_name}: {e}")
            return None
    
    def find_many(self, collection_name: str, filter_dict: Dict[str, Any] = None,
                 projection: Dict[str, Any] = None, sort: List[tuple] = None,
                 limit: Optional[int] = None, skip: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Find multiple documents in a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria
            projection: Fields to include/exclude
            sort: Sort criteria as list of (field, direction) tuples
            limit: Maximum number of documents to return
            skip: Number of documents to skip
            
        Returns:
            List[Dict]: List of documents as dictionaries
        """
        try:
            with self.get_collection(collection_name) as collection:
                cursor = collection.find(filter_dict or {}, projection)
                
                if sort:
                    cursor = cursor.sort(sort)
                if skip:
                    cursor = cursor.skip(skip)
                if limit:
                    cursor = cursor.limit(limit)
                
                results = list(cursor)
                # Convert ObjectId to string for JSON serialization
                for result in results:
                    if '_id' in result:
                        result['_id'] = str(result['_id'])
                
                return results
        except Exception as e:
            log.error(f"Failed to find documents in {collection_name}: {e}")
            return []
    
    def count_documents(self, collection_name: str, filter_dict: Dict[str, Any] = None) -> int:
        """
        Count documents in a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria
            
        Returns:
            int: Number of documents matching the filter
        """
        try:
            with self.get_collection(collection_name) as collection:
                return collection.count_documents(filter_dict or {})
        except Exception as e:
            log.error(f"Failed to count documents in {collection_name}: {e}")
            return 0
    
    def exists(self, collection_name: str, filter_dict: Dict[str, Any]) -> bool:
        """
        Check if a document exists in a collection
        
        Args:
            collection_name: Name of the collection
            filter_dict: Filter criteria
            
        Returns:
            bool: True if document exists, False otherwise
        """
        try:
            with self.get_collection(collection_name) as collection:
                return collection.count_documents(filter_dict, limit=1) > 0
        except Exception as e:
            log.error(f"Failed to check existence in {collection_name}: {e}")
            return False
    
    def aggregate(self, collection_name: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute an aggregation pipeline
        
        Args:
            collection_name: Name of the collection
            pipeline: Aggregation pipeline stages
            
        Returns:
            List[Dict]: Aggregation results
        """
        try:
            with self.get_collection(collection_name) as collection:
                results = list(collection.aggregate(pipeline))
                # Convert ObjectId to string for JSON serialization
                for result in results:
                    if '_id' in result:
                        result['_id'] = str(result['_id'])
                return results
        except Exception as e:
            log.error(f"Failed to execute aggregation on {collection_name}: {e}")
            return []
    
    def create_index(self, collection_name: str, keys: Union[str, List[tuple]], 
                    unique: bool = False, background: bool = True) -> bool:
        """
        Create an index on a collection
        
        Args:
            collection_name: Name of the collection
            keys: Index specification
            unique: Whether the index should enforce uniqueness
            background: Whether to build the index in the background
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_collection(collection_name) as collection:
                collection.create_index(keys, unique=unique, background=background)
                log.info(f"Created index on {collection_name}")
                return True
        except Exception as e:
            log.error(f"Failed to create index on {collection_name}: {e}")
            return False
    
    def drop_collection(self, collection_name: str) -> bool:
        """
        Drop a collection
        
        Args:
            collection_name: Name of the collection to drop
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self._database.drop_collection(collection_name)
            log.info(f"Dropped collection {collection_name}")
            return True
        except Exception as e:
            log.error(f"Failed to drop collection {collection_name}: {e}")
            return False
    
    def get_collection_names(self) -> List[str]:
        """
        Get list of collection names in the database
        
        Returns:
            List[str]: List of collection names
        """
        try:
            return self._database.list_collection_names()
        except Exception as e:
            log.error(f"Failed to get collection names: {e}")
            return []
    
    def close(self):
        """Close the MongoDB connection"""
        if self._client:
            self._client.close()
            log.info("MongoDB connection closed")