"""
SumarioPMD Database Client for GFZ Data Services - PyMySQL Version.

This module provides database access to the SUMARIOPMD database at GFZ,
specifically for managing author metadata (Creators) in the resource/resourceagent/role tables.

Uses PyMySQL (pure Python) instead of mysql-connector-python for perfect Nuitka compatibility.

CRITICAL: Only updates Creators (role='Creator'), never Contributors or other roles!
"""

import logging
from typing import Optional, List, Dict, Tuple, Any
from contextlib import contextmanager

# PyMySQL: Pure Python MySQL client - works perfectly in frozen apps!
import pymysql
from pymysql.cursors import DictCursor

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
        Initialize database client with connection parameters.
        
        Args:
            host: Database host (e.g., rz-mysql3.gfz-potsdam.de)
            database: Database name (sumario-pmd)
            username: Database username
            password: Database password
            pool_size: Unused (kept for API compatibility)
            
        Raises:
            ConnectionError: If test connection fails
        """
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        
        # Add .gfz-potsdam.de suffix if not present
        if not self.host.endswith('.gfz-potsdam.de') and not self.host.startswith('localhost'):
            self.host = f"{self.host}.gfz-potsdam.de"
        
        logger.info(f"SumarioPMDClient initialized for {self.host}/{self.database} using PyMySQL")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting database connection.
        
        PyMySQL creates connections on demand (no pooling), which is perfect for frozen apps.
        
        Yields:
            connection: PyMySQL connection
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        connection = None
        try:
            # PyMySQL connection - pure Python, works great in frozen apps!
            # No SSL/auth_plugin parameters needed - PyMySQL handles everything automatically
            connection = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                connect_timeout=10,
                charset='utf8mb4',
                cursorclass=DictCursor  # Return results as dictionaries
            )
        except pymysql.Error as e:
            # Only catch connection errors here, not query errors during transactions
            logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Database connection failed: {e}") from e
        
        try:
            yield connection
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test database connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT VERSION()")
                    result = cursor.fetchone()
                    version = result['VERSION()']  # DictCursor returns dict
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
                with conn.cursor() as cursor:
                    cursor.execute(query, (doi,))
                    result = cursor.fetchone()
                    
                    if result:
                        resource_id = result['id']
                        logger.debug(f"Found resource_id {resource_id} for DOI {doi}")
                        return resource_id
                    else:
                        logger.warning(f"No resource found for DOI {doi}")
                        return None
                        
        except pymysql.Error as e:
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
                ra.order AS `order`,
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
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id,))
                    creators = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(creators)} creators for resource_id {resource_id}")
                    return creators
                    
        except pymysql.Error as e:
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
            with self.get_connection() as connection:
                # Start transaction
                connection.begin()
                logger.debug(f"Started transaction for resource_id {resource_id}")
                
                with connection.cursor() as cursor:
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
                    creator_orders = [row['order'] for row in cursor.fetchall()]
                    
                    if creator_orders:
                        # Delete resourceagent entries for these specific orders
                        placeholders = ','.join(['%s'] * len(creator_orders))
                        delete_ra_query = f"""
                            DELETE FROM resourceagent 
                            WHERE resource_id = %s 
                                AND `order` IN ({placeholders})
                        """
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
                
                return True, message, errors
                
        except pymysql.Error as e:
            # Rollback on error
            if connection:
                try:
                    connection.rollback()
                    logger.warning(f"Transaction rolled back for resource_id {resource_id}")
                except pymysql.Error as rollback_error:
                    logger.error(f"CRITICAL: Rollback failed: {rollback_error}")
                    raise TransactionError(f"Transaction failed AND rollback failed: {rollback_error}") from e
            
            error_msg = f"✗ Database transaction failed: {str(e)}"
            logger.error(error_msg)
            errors.append(str(e))
            return False, error_msg, errors
    
    # =========================================================================
    # Publisher Methods
    # =========================================================================
    
    def get_publisher_for_doi(self, doi: str) -> Optional[str]:
        """
        Get the publisher name for a given DOI.
        
        Args:
            doi: DOI string (e.g., "10.5880/GFZ.1.1.2021.001")
            
        Returns:
            Publisher name (str) or None if not found
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT publisher 
            FROM resource 
            WHERE identifier = %s
            LIMIT 1
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (doi,))
                    result = cursor.fetchone()
                    
                    if result:
                        publisher = result['publisher']
                        logger.debug(f"Found publisher '{publisher}' for DOI {doi}")
                        return publisher
                    else:
                        logger.warning(f"No resource found for DOI {doi}")
                        return None
                        
        except pymysql.Error as e:
            logger.error(f"Database error fetching publisher for {doi}: {e}")
            raise DatabaseError(f"Failed to fetch publisher: {e}") from e
    
    def update_publisher(self, doi: str, publisher_name: str) -> Tuple[bool, str]:
        """
        Update the publisher name for a DOI in the resource table.
        
        Note: The database only stores the publisher name (varchar(255)),
        not the extended publisher metadata (identifier, scheme, etc.)
        which are only stored in DataCite.
        
        Args:
            doi: DOI string
            publisher_name: New publisher name
            
        Returns:
            Tuple of (success: bool, message: str)
            - (False, message) for validation errors (empty publisher name)
            - (False, message) if DOI not found in database
            - (True, message) on successful update
            - (False, message) if update affected no rows
            
        Raises:
            DatabaseError: Only raised for actual database errors during query execution
                          (e.g., connection lost, SQL syntax error, constraint violation)
        """
        if not publisher_name:
            return False, "Publisher-Name darf nicht leer sein"
        
        # First check if DOI exists
        resource_id = self.get_resource_id_for_doi(doi)
        if resource_id is None:
            return False, f"DOI {doi} nicht in der Datenbank gefunden"
        
        query = """
            UPDATE resource 
            SET publisher = %s, updated_at = NOW()
            WHERE identifier = %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (publisher_name, doi))
                    conn.commit()
                    
                    if cursor.rowcount > 0:
                        message = f"✓ Publisher für DOI {doi} aktualisiert: {publisher_name}"
                        logger.info(message)
                        return True, message
                    else:
                        message = f"✗ Keine Änderung für DOI {doi}"
                        logger.warning(message)
                        return False, message
                        
        except pymysql.Error as e:
            error_msg = f"✗ Database update failed for {doi}: {str(e)}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def close_pool(self):
        """No-op for PyMySQL (connections are created on demand)."""
        logger.info("Connection cleanup requested (PyMySQL creates connections on demand)")

