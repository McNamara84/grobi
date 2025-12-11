"""
SumarioPMD Database Client for GFZ Data Services - PyMySQL Version.

This module provides database access to the SUMARIOPMD database at GFZ,
specifically for managing author metadata (Creators) and contributor metadata
in the resource/resourceagent/role/contactinfo tables.

Uses PyMySQL (pure Python) instead of mysql-connector-python for perfect Nuitka compatibility.

Table Structure:
- resource: DOI storage (identifier field)
- resourceagent: Person data (firstname, lastname, identifier=ORCID)
- role: Maps resourceagent to role types (Creator, ContactPerson, etc.)
- contactinfo: Email/Website/Position for ContactPerson contributors
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
    
    Manages author and contributor metadata in the four-table system:
    - resource: DOI storage (identifier field)
    - resourceagent: Person data (firstname, lastname, identifier=ORCID)
    - role: Maps resourceagent to role types (Creator, ContactPerson, etc.)
    - contactinfo: Email/Website/Position for ContactPerson contributors
    
    Supports both Creator updates (authors) and Contributor updates.
    """
    
    # Valid contributor types (non-Creator roles)
    VALID_CONTRIBUTOR_TYPES = [
        "ContactPerson", "DataCollector", "DataCurator", "DataManager",
        "Distributor", "Editor", "HostingInstitution", "Producer",
        "ProjectLeader", "ProjectManager", "ProjectMember",
        "RegistrationAgency", "RegistrationAuthority", "RelatedPerson",
        "Researcher", "ResearchGroup", "RightsHolder", "Sponsor",
        "Supervisor", "Translator", "WorkPackageLeader", "Other",
        "pointOfContact"  # GFZ-internal type
    ]
    
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

    # =========================================================================
    # Contributor Methods (non-Creator roles)
    # =========================================================================
    
    def fetch_all_contactinfo_for_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """
        Fetch all ContactInfo data for a resource, regardless of role.
        
        This retrieves contactinfo for ALL resourceagents (Creators and Contributors alike).
        ContactInfo is linked to the resourceagent, not to a specific role.
        
        Args:
            resource_id: Resource ID from resource table
            
        Returns:
            List of dictionaries with keys:
            - firstname: str
            - lastname: str
            - name: str (full "Lastname, Firstname" format)
            - email: str or None
            - website: str or None
            - position: str or None
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT 
                ra.firstname,
                ra.lastname,
                ra.name,
                ci.email,
                ci.website,
                ci.position
            FROM resourceagent ra
            INNER JOIN contactinfo ci 
                ON ci.resourceagent_resource_id = ra.resource_id 
                AND ci.resourceagent_order = ra.order
            WHERE ra.resource_id = %s
                AND (ci.email IS NOT NULL OR ci.website IS NOT NULL OR ci.position IS NOT NULL)
            ORDER BY ra.order ASC
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id,))
                    results = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(results)} contactinfo entries for resource_id {resource_id}")
                    return results
                    
        except pymysql.Error as e:
            logger.error(f"Database error fetching contactinfo for resource_id {resource_id}: {e}")
            raise DatabaseError(f"Failed to fetch contactinfo: {e}") from e
    
    def fetch_contributors_for_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """
        Fetch all Contributors (non-Creator roles) for a resource.
        
        Contributors can have multiple roles, which are returned as comma-separated list.
        Also fetches contactinfo (email, website, position) if available.
        
        Args:
            resource_id: Resource ID from resource table
            
        Returns:
            List of contributor dictionaries with keys:
            - order: int (order in contributor list)
            - firstname: str
            - lastname: str
            - name: str (full "Lastname, Firstname" format)
            - orcid: str (ID only, without URL prefix)
            - identifiertype: str (usually "ORCID")
            - nametype: str (usually "Personal")
            - roles: str (comma-separated list of roles)
            - email: str or None (from contactinfo)
            - website: str or None (from contactinfo)
            - position: str or None (from contactinfo)
            
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
                ra.nametype,
                GROUP_CONCAT(DISTINCT r.role ORDER BY r.role SEPARATOR ', ') AS roles,
                ci.email,
                ci.website,
                ci.position
            FROM resourceagent ra
            INNER JOIN role r 
                ON r.resourceagent_resource_id = ra.resource_id 
                AND r.resourceagent_order = ra.order
            LEFT JOIN contactinfo ci 
                ON ci.resourceagent_resource_id = ra.resource_id 
                AND ci.resourceagent_order = ra.order
            WHERE ra.resource_id = %s 
                AND r.role != 'Creator'
            GROUP BY ra.resource_id, ra.order, ra.firstname, ra.lastname, 
                     ra.name, ra.identifier, ra.identifiertype, ra.nametype,
                     ci.email, ci.website, ci.position
            ORDER BY ra.order ASC
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id,))
                    contributors = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(contributors)} contributors for resource_id {resource_id}")
                    return contributors
                    
        except pymysql.Error as e:
            logger.error(f"Database error fetching contributors for resource_id {resource_id}: {e}")
            raise DatabaseError(f"Failed to fetch contributors: {e}") from e
    
    def fetch_contactinfo_for_contributor(
        self, 
        resource_id: int, 
        order: int
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch email/website/position for a specific contributor.
        
        Args:
            resource_id: Resource ID from resource table
            order: Order of the contributor in resourceagent
            
        Returns:
            Dictionary with email, website, position or None if not found
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT email, website, position
            FROM contactinfo
            WHERE resourceagent_resource_id = %s 
                AND resourceagent_order = %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id, order))
                    result = cursor.fetchone()
                    
                    if result:
                        logger.debug(f"Found contactinfo for resource_id {resource_id}, order {order}")
                        return result
                    else:
                        logger.debug(f"No contactinfo for resource_id {resource_id}, order {order}")
                        return None
                        
        except pymysql.Error as e:
            logger.error(f"Database error fetching contactinfo: {e}")
            raise DatabaseError(f"Failed to fetch contactinfo: {e}") from e
    
    def update_contributors_transactional(
        self, 
        resource_id: int, 
        contributors: List[Dict[str, Any]]
    ) -> Tuple[bool, str, List[str]]:
        """
        Update contributors for a resource in a transaction with ROLLBACK support.
        
        This updates non-Creator roles only. Creator entries are not touched.
        
        Strategy:
        1. BEGIN TRANSACTION
        2. Delete existing non-Creator entries (role + contactinfo + resourceagent)
        3. Insert new contributor entries into resourceagent
        4. Insert role entries (multiple per contributor if needed)
        5. Insert contactinfo entries (only for ContactPerson roles)
        6. COMMIT if successful, ROLLBACK on any error
        
        Args:
            resource_id: Resource ID from resource table
            contributors: List of contributor dicts with keys:
                - firstname: str
                - lastname: str
                - orcid: str (full URL or ID only - will be normalized to ID only)
                - nametype: str (default: "Personal")
                - contributorTypes: str (comma-separated list of roles)
                - email: str (optional, for ContactPerson)
                - website: str (optional, for ContactPerson)
                - position: str (optional, for ContactPerson)
                
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
                logger.debug(f"Started transaction for contributors, resource_id {resource_id}")
                
                with connection.cursor() as cursor:
                    # Step 1: Get existing non-Creator orders to delete
                    select_orders_query = """
                        SELECT DISTINCT ra.order 
                        FROM resourceagent ra
                        INNER JOIN role r 
                            ON r.resourceagent_resource_id = ra.resource_id 
                            AND r.resourceagent_order = ra.order
                        WHERE ra.resource_id = %s 
                            AND r.role != 'Creator'
                    """
                    cursor.execute(select_orders_query, (resource_id,))
                    contributor_orders = [row['order'] for row in cursor.fetchall()]
                    
                    if contributor_orders:
                        # Step 2a: Delete from contactinfo first (no FK constraint, but clean up)
                        placeholders = ','.join(['%s'] * len(contributor_orders))
                        delete_ci_query = f"""
                            DELETE FROM contactinfo 
                            WHERE resourceagent_resource_id = %s 
                                AND resourceagent_order IN ({placeholders})
                        """
                        cursor.execute(delete_ci_query, (resource_id, *contributor_orders))
                        deleted_ci = cursor.rowcount
                        logger.debug(f"Deleted {deleted_ci} contactinfo entries")
                        
                        # Step 2b: Delete from role table (only non-Creator)
                        delete_role_query = f"""
                            DELETE FROM role 
                            WHERE resourceagent_resource_id = %s 
                                AND role != 'Creator'
                                AND resourceagent_order IN ({placeholders})
                        """
                        cursor.execute(delete_role_query, (resource_id, *contributor_orders))
                        deleted_roles = cursor.rowcount
                        logger.debug(f"Deleted {deleted_roles} role entries")
                        
                        # Step 2c: Delete from resourceagent
                        # But only if the order has NO remaining roles (could still have Creator)
                        for order in contributor_orders:
                            # Check if this order still has roles
                            check_query = """
                                SELECT COUNT(*) as cnt FROM role 
                                WHERE resourceagent_resource_id = %s AND resourceagent_order = %s
                            """
                            cursor.execute(check_query, (resource_id, order))
                            remaining = cursor.fetchone()['cnt']
                            
                            if remaining == 0:
                                delete_ra_query = """
                                    DELETE FROM resourceagent 
                                    WHERE resource_id = %s AND `order` = %s
                                """
                                cursor.execute(delete_ra_query, (resource_id, order))
                                logger.debug(f"Deleted resourceagent order {order}")
                    
                    # Step 3: Determine next order number
                    # Get max order from existing resourceagent entries
                    max_order_query = """
                        SELECT COALESCE(MAX(`order`), 0) as max_order 
                        FROM resourceagent 
                        WHERE resource_id = %s
                    """
                    cursor.execute(max_order_query, (resource_id,))
                    max_order = cursor.fetchone()['max_order']
                    next_order = max_order + 1
                    
                    # Step 4: Insert new contributor entries
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
                    
                    insert_ci_query = """
                        INSERT INTO contactinfo 
                        (resourceagent_resource_id, resourceagent_order, email, website, position)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    for contributor in contributors:
                        firstname = contributor.get('firstname', '').strip()
                        lastname = contributor.get('lastname', '').strip()
                        orcid_raw = contributor.get('orcid', '').strip()
                        nametype = contributor.get('nametype', 'Personal').strip()
                        contributor_types = contributor.get('contributorTypes', '').strip()
                        email = contributor.get('email', '').strip() or None
                        website = contributor.get('website', '').strip() or None
                        position = contributor.get('position', '').strip() or None
                        
                        # Validate: need at least lastname or name
                        name = contributor.get('name', '').strip()
                        if not lastname and not name:
                            errors.append(f"Contributor: Missing lastname/name")
                            continue
                        
                        # Normalize ORCID: Remove URL prefix if present
                        orcid_id = orcid_raw
                        if orcid_raw.startswith('https://orcid.org/'):
                            orcid_id = orcid_raw.replace('https://orcid.org/', '')
                        elif orcid_raw.startswith('http://orcid.org/'):
                            orcid_id = orcid_raw.replace('http://orcid.org/', '')
                        
                        # Build full name in "Lastname, Firstname" format
                        if not name:
                            if firstname:
                                name = f"{lastname}, {firstname}"
                            else:
                                name = lastname
                        
                        # Parse contributor types
                        types_list = [t.strip() for t in contributor_types.split(',') if t.strip()]
                        if not types_list:
                            types_list = ['Other']
                        
                        # Validate types
                        valid_types = []
                        for t in types_list:
                            if t in self.VALID_CONTRIBUTOR_TYPES:
                                valid_types.append(t)
                            else:
                                logger.warning(f"Unknown contributor type: {t}, using 'Other'")
                                if 'Other' not in valid_types:
                                    valid_types.append('Other')
                        
                        if not valid_types:
                            valid_types = ['Other']
                        
                        # Determine identifier type
                        identifier_type = None
                        if orcid_id:
                            if orcid_id.startswith('https://ror.org/') or 'ror.org' in orcid_id:
                                identifier_type = 'ROR'
                            else:
                                identifier_type = 'ORCID'
                        
                        # Insert into resourceagent
                        ra_values = (
                            resource_id,
                            next_order,
                            name,
                            firstname if nametype == 'Personal' else None,
                            lastname if nametype == 'Personal' else None,
                            orcid_id if orcid_id else None,
                            identifier_type,
                            nametype
                        )
                        cursor.execute(insert_ra_query, ra_values)
                        
                        # Insert roles (one entry per type)
                        for role_type in valid_types:
                            role_values = (role_type, resource_id, next_order)
                            cursor.execute(insert_role_query, role_values)
                        
                        # Insert contactinfo if this is a ContactPerson and has email/website
                        if 'ContactPerson' in valid_types and (email or website or position):
                            ci_values = (resource_id, next_order, email, website, position)
                            cursor.execute(insert_ci_query, ci_values)
                            logger.debug(f"Inserted contactinfo for order {next_order}")
                        
                        logger.debug(f"Inserted contributor {next_order}: {name} (Roles: {', '.join(valid_types)})")
                        next_order += 1
                
                # Commit transaction
                connection.commit()
                message = f"✓ Successfully updated {len(contributors)} contributors for resource_id {resource_id}"
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
    
    def upsert_contactinfo(
        self, 
        resource_id: int, 
        order: int, 
        email: Optional[str], 
        website: Optional[str], 
        position: Optional[str]
    ) -> Tuple[bool, str]:
        """
        Insert or update contactinfo for a contributor.
        
        Args:
            resource_id: Resource ID
            order: Order of the contributor
            email: Email address (can be None)
            website: Website URL (can be None)
            position: Position/title (can be None)
            
        Returns:
            Tuple of (success: bool, message: str)
            
        Raises:
            DatabaseError: If query fails
        """
        # Check if entry exists
        check_query = """
            SELECT 1 FROM contactinfo 
            WHERE resourceagent_resource_id = %s AND resourceagent_order = %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(check_query, (resource_id, order))
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        # Update existing
                        update_query = """
                            UPDATE contactinfo 
                            SET email = %s, website = %s, position = %s
                            WHERE resourceagent_resource_id = %s AND resourceagent_order = %s
                        """
                        cursor.execute(update_query, (email, website, position, resource_id, order))
                    else:
                        # Insert new
                        insert_query = """
                            INSERT INTO contactinfo 
                            (resourceagent_resource_id, resourceagent_order, email, website, position)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(insert_query, (resource_id, order, email, website, position))
                    
                    conn.commit()
                    
                    action = "updated" if exists else "inserted"
                    message = f"✓ ContactInfo {action} for resource_id {resource_id}, order {order}"
                    logger.info(message)
                    return True, message
                    
        except pymysql.Error as e:
            error_msg = f"✗ Failed to upsert contactinfo: {str(e)}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e
    
    def get_contributor_roles_for_resource(self, resource_id: int) -> Dict[int, List[str]]:
        """
        Get all contributor roles grouped by order for a resource.
        
        Args:
            resource_id: Resource ID
            
        Returns:
            Dictionary mapping order to list of role names
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT resourceagent_order, role
            FROM role
            WHERE resourceagent_resource_id = %s AND role != 'Creator'
            ORDER BY resourceagent_order, role
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id,))
                    results = cursor.fetchall()
                    
                    roles_by_order: Dict[int, List[str]] = {}
                    for row in results:
                        order = row['resourceagent_order']
                        role = row['role']
                        if order not in roles_by_order:
                            roles_by_order[order] = []
                        roles_by_order[order].append(role)
                    
                    return roles_by_order
                    
        except pymysql.Error as e:
            logger.error(f"Database error fetching contributor roles: {e}")
            raise DatabaseError(f"Failed to fetch contributor roles: {e}") from e
    
    # =========================================================================
    # File/Download-URL Methods
    # =========================================================================
    
    def fetch_download_urls_for_resource(self, resource_id: int) -> List[Dict[str, Any]]:
        """
        Fetch download URLs from file table for a specific resource.
        
        Args:
            resource_id: Resource ID from resource table
            
        Returns:
            List of file dictionaries with keys:
            - filename: str
            - location: str (download URL)
            - format: str (file format)
            - size: int (file size in bytes)
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT 
                filename,
                location,
                format,
                size
            FROM file
            WHERE resource_id = %s
            ORDER BY filename ASC
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (resource_id,))
                    files = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(files)} files for resource_id {resource_id}")
                    return files
                    
        except pymysql.Error as e:
            logger.error(f"Database error fetching files for resource_id {resource_id}: {e}")
            raise DatabaseError(f"Failed to fetch files: {e}") from e
    
    def fetch_all_dois_with_downloads(self) -> List[Tuple[str, str, str, str, int]]:
        """
        Fetch all DOIs with their download URLs from file table.
        
        Returns one row per file, so a DOI with multiple files will appear multiple times.
        
        Returns:
            List of tuples containing:
            (DOI, Filename, Download_URL, Format, Size_Bytes)
            
        Raises:
            DatabaseError: If query fails
        """
        query = """
            SELECT 
                r.identifier AS doi,
                f.filename,
                f.location AS download_url,
                f.format,
                f.size
            FROM resource r
            INNER JOIN file f ON f.resource_id = r.id
            ORDER BY r.identifier ASC, f.filename ASC
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    # Convert to list of tuples
                    dois_files = [
                        (
                            row['doi'],
                            row['filename'],
                            row['download_url'],
                            row['format'],
                            row['size']
                        )
                        for row in results
                    ]
                    
                    logger.info(f"Fetched {len(dois_files)} file entries from database")
                    return dois_files
                    
        except pymysql.Error as e:
            logger.error(f"Database error fetching DOIs with downloads: {e}")
            raise DatabaseError(f"Failed to fetch DOIs with downloads: {e}") from e
