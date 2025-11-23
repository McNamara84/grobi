"""
SumarioPMD Database Client for GFZ Data Services.

This module provides database access to the SUMARIOPMD database at GFZ,
specifically for managing author metadata (Creators) in the resource/resourceagent/role tables.

CRITICAL: Only updates Creators (role='Creator'), never Contributors or other roles!
"""

import logging
from typing import Optional, List, Dict, Tuple, Any
from contextlib import contextmanager

from mysql.connector import Error as MySQLError
from mysql.connector.pooling import MySQLConnectionPool

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception for database operations."""
    pass


class ConnectionError(DatabaseError):
    """Exception raised when database connection fails."""
    pass


class TransactionError(DatabaseError):
    """Exception raised when database transaction fails."""
    pass


class SumarioPMDClient:
    """
    Client for SUMARIOPMD database operations.
    
    Manages author metadata in the three-table system:
    - resource: DOI storage (identifier field)
    - resourceagent: Person data (firstname, lastname, identifier=ORCID)
    - role: Maps resourceagent to role types (Creator, ContactPerson, etc.)
    
    CRITICAL: Only modifies records where role.role = 'Creator'!
    """
    
    def __init__(self, host: str, database: str, username: str, password: str, pool_size: int = 5):
        """
        Initialize database client with connection pooling.
        
        Args:
            host: Database host (e.g., rz-mysql3.gfz-potsdam.de)
            database: Database name (sumario-pmd)
            username: Database username
            password: Database password
            pool_size: Connection pool size (default: 5)
            
        Raises:
            ConnectionError: If connection pool creation fails
        """
        self.host = host
        self.database = database
        self.username = username
        
        # Add .gfz-potsdam.de suffix if not present
        if not self.host.endswith('.gfz-potsdam.de') and not self.host.startswith('localhost'):
            self.host = f"{self.host}.gfz-potsdam.de"
        
        try:
            self.pool = MySQLConnectionPool(
                pool_name="sumariopmd_pool",
                pool_size=pool_size,
                pool_reset_session=True,
                host=self.host,
                database=self.database,
                user=self.username,
                password=password,
                connect_timeout=10,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                auth_plugin='mysql_native_password'
            )
            logger.info(f"Connection pool created for {self.host}/{self.database}")
        except MySQLError as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise ConnectionError(f"Failed to connect to database: {e}") from e
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting pooled database connection.
        
        Yields:
            mysql.connector.connection.MySQLConnection: Database connection
            
        Raises:
            ConnectionError: If connection cannot be obtained
        """
        connection = None
        try:
            connection = self.pool.get_connection()
            yield connection
        except MySQLError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise ConnectionError(f"Database connection failed: {e}") from e
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test database connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                cursor.close()
                message = f"✓ Connected to MySQL {version}"
                logger.info(message)
                return True, message
        except Exception as e:
            message = f"✗ Connection failed: {str(e)}"
            logger.error(message)
            return False, message
    
    def get_resource_id_for_doi(self, doi: str) -> Optional[int]:
        """
        Get resource_id for a given DOI.
        
        Args:
            doi: DOI string (e.g., "10.5880/gfz_orbit/rso/gnss_g_v02")
            
        Returns:
            Resource ID (int) or None if not found
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT id 
            FROM resource 
            WHERE identifier = %s
            LIMIT 1
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (doi,))
                result = cursor.fetchone()
                cursor.close()
                
                if result:
                    resource_id = result[0]
                    logger.debug(f"Found resource_id {resource_id} for DOI {doi}")
                    return resource_id
                else:
                    logger.warning(f"No resource found for DOI {doi}")
                    return None
                    
        except MySQLError as e:
            logger.error(f"Database error fetching resource_id for {doi}: {e}")
            raise DatabaseError(f"Failed to fetch resource_id: {e}") from e
    
    def fetch_creators_for_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """
        Fetch all Creators for a resource.
        
        CRITICAL: Only fetches records where role.role = 'Creator'!
        
        Args:
            resource_id: Resource ID from resource table
            
        Returns:
            List of creator dictionaries with keys:
            - order: int (order in author list)
            - firstname: str
            - lastname: str
            - name: str (full "Lastname, Firstname" format)
            - orcid: str (ID only, without URL prefix)
            - identifiertype: str (usually "ORCID")
            - nametype: str (usually "Personal")
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT 
                ra.order,
                ra.firstname,
                ra.lastname,
                ra.name,
                ra.identifier AS orcid,
                ra.identifiertype,
                ra.nametype
            FROM resourceagent ra
            INNER JOIN role r 
                ON r.resourceagent_resource_id = ra.resource_id 
                AND r.resourceagent_order = ra.order
            WHERE ra.resource_id = %s 
                AND r.role = 'Creator'
            ORDER BY ra.order ASC
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, (resource_id,))
                creators = cursor.fetchall()
                cursor.close()
                
                logger.info(f"Fetched {len(creators)} creators for resource_id {resource_id}")
                return creators
                
        except MySQLError as e:
            logger.error(f"Database error fetching creators for resource_id {resource_id}: {e}")
            raise DatabaseError(f"Failed to fetch creators: {e}") from e
    
    def update_creators_transactional(
        self, 
        resource_id: int, 
        creators: List[Dict[str, str]]
    ) -> Tuple[bool, str, List[str]]:
        """
        Update creators for a resource in a transaction with ROLLBACK support.
        
        CRITICAL: Only updates records where role.role = 'Creator'!
        
        Strategy:
        1. BEGIN TRANSACTION
        2. Delete existing Creator entries (resourceagent + role)
        3. Insert new Creator entries
        4. COMMIT if successful, ROLLBACK on any error
        
        Args:
            resource_id: Resource ID from resource table
            creators: List of creator dicts with keys:
                - firstname: str
                - lastname: str
                - orcid: str (full URL or ID only - will be normalized to ID only)
                
        Returns:
            Tuple of (success: bool, message: str, errors: List[str])
            
        Raises:
            TransactionError: If transaction fails and cannot be rolled back
        """
        errors = []
        connection = None
        
        try:
            connection = self.pool.get_connection()
            cursor = connection.cursor()
            
            # Start transaction
            connection.start_transaction()
            logger.debug(f"Started transaction for resource_id {resource_id}")
            
            # Step 1: Delete existing Creator entries
            # First delete from role table (foreign key constraint)
            delete_role_query = """
                DELETE FROM role 
                WHERE resourceagent_resource_id = %s 
                    AND role = 'Creator'
            """
            cursor.execute(delete_role_query, (resource_id,))
            deleted_roles = cursor.rowcount
            logger.debug(f"Deleted {deleted_roles} role entries")
            
            # Then delete from resourceagent table
            # Get the orders first to delete exact matches
            select_orders_query = """
                SELECT DISTINCT ra.order 
                FROM resourceagent ra
                INNER JOIN role r 
                    ON r.resourceagent_resource_id = ra.resource_id 
                    AND r.resourceagent_order = ra.order
                WHERE ra.resource_id = %s 
                    AND r.role = 'Creator'
            """
            cursor.execute(select_orders_query, (resource_id,))
            creator_orders = [row[0] for row in cursor.fetchall()]
            
            if creator_orders:
                # Delete resourceagent entries for these specific orders
                delete_ra_query = """
                    DELETE FROM resourceagent 
                    WHERE resource_id = %s 
                        AND `order` IN ({})
                """.format(','.join(['%s'] * len(creator_orders)))
                cursor.execute(delete_ra_query, (resource_id, *creator_orders))
                deleted_agents = cursor.rowcount
                logger.debug(f"Deleted {deleted_agents} resourceagent entries")
            
            # Step 2: Insert new Creator entries
            insert_ra_query = """
                INSERT INTO resourceagent 
                (resource_id, `order`, name, firstname, lastname, identifier, identifiertype, nametype)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            insert_role_query = """
                INSERT INTO role 
                (role, resourceagent_resource_id, resourceagent_order)
                VALUES (%s, %s, %s)
            """
            
            for order, creator in enumerate(creators, start=1):
                firstname = creator.get('firstname', '').strip()
                lastname = creator.get('lastname', '').strip()
                orcid_raw = creator.get('orcid', '').strip()
                
                # Validate required fields
                if not lastname:
                    errors.append(f"Creator {order}: Missing lastname")
                    continue
                
                # Normalize ORCID: Remove URL prefix if present
                # DataCite: https://orcid.org/0000-0001-5401-6794
                # Database: 0000-0001-5401-6794
                orcid_id = orcid_raw
                if orcid_raw.startswith('https://orcid.org/'):
                    orcid_id = orcid_raw.replace('https://orcid.org/', '')
                elif orcid_raw.startswith('http://orcid.org/'):
                    orcid_id = orcid_raw.replace('http://orcid.org/', '')
                
                # Build full name in "Lastname, Firstname" format
                if firstname:
                    name = f"{lastname}, {firstname}"
                else:
                    name = lastname
                
                # Insert into resourceagent
                ra_values = (
                    resource_id,
                    order,
                    name,
                    firstname,
                    lastname,
                    orcid_id if orcid_id else None,
                    'ORCID' if orcid_id else None,
                    'Personal'
                )
                cursor.execute(insert_ra_query, ra_values)
                
                # Insert into role
                role_values = ('Creator', resource_id, order)
                cursor.execute(insert_role_query, role_values)
                
                logger.debug(f"Inserted creator {order}: {name} (ORCID: {orcid_id or 'N/A'})")
            
            # Commit transaction
            connection.commit()
            message = f"✓ Successfully updated {len(creators)} creators for resource_id {resource_id}"
            logger.info(message)
            
            cursor.close()
            return True, message, errors
            
        except MySQLError as e:
            # Rollback on error
            if connection:
                try:
                    connection.rollback()
                    logger.warning(f"Transaction rolled back for resource_id {resource_id}")
                except MySQLError as rollback_error:
                    logger.error(f"CRITICAL: Rollback failed: {rollback_error}")
                    raise TransactionError(f"Transaction failed AND rollback failed: {rollback_error}") from e
            
            error_msg = f"✗ Database transaction failed: {str(e)}"
            logger.error(error_msg)
            errors.append(str(e))
            return False, error_msg, errors
            
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def close_pool(self):
        """Close all connections in the pool."""
        try:
            # Note: mysql-connector-python doesn't have a direct pool.close() method
            # Connections are closed when they're returned to the pool and garbage collected
            logger.info("Connection pool cleanup requested")
        except Exception as e:
            logger.error(f"Error during pool cleanup: {e}")
