"""
Rating and Review system for OER resources.
Uses PostgreSQL for persistent storage (shares DSpace database).
Links reviews to DSpace eperson table.
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection settings (use DSpace PostgreSQL)
DB_HOST = os.getenv("POSTGRES_HOST", "dspace-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "dspace")
DB_USER = os.getenv("POSTGRES_USER", "dspace")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dspace")


@dataclass
class Review:
    """Review model."""
    id: int
    resource_id: str
    eperson_id: str  # UUID from eperson table
    email: str
    display_name: str
    rating: int  # 1-5 stars
    comment: str
    created_at: str
    updated_at: str
    helpful_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ReviewStore:
    """PostgreSQL-based review storage linked to DSpace eperson."""
    
    def __init__(self):
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with context manager."""
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database schema if not exists."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Create reviews table linked to eperson
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS oer_reviews (
                        id SERIAL PRIMARY KEY,
                        resource_id VARCHAR(255) NOT NULL,
                        eperson_id UUID NOT NULL REFERENCES eperson(uuid) ON DELETE CASCADE,
                        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                        comment TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        helpful_count INTEGER DEFAULT 0,
                        CONSTRAINT unique_user_resource UNIQUE (resource_id, eperson_id)
                    )
                """)
                
                # Create indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_oer_reviews_resource 
                    ON oer_reviews(resource_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_oer_reviews_eperson 
                    ON oer_reviews(eperson_id)
                """)
                
                # Create helpful tracking table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS oer_review_helpful (
                        id SERIAL PRIMARY KEY,
                        review_id INTEGER NOT NULL REFERENCES oer_reviews(id) ON DELETE CASCADE,
                        eperson_id UUID NOT NULL REFERENCES eperson(uuid) ON DELETE CASCADE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT unique_helpful UNIQUE (review_id, eperson_id)
                    )
                """)
                
                conn.commit()
                print("Review tables initialized successfully")
    
    def get_eperson_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get eperson info by email. Names are stored in metadatavalue table."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        e.uuid, 
                        e.email,
                        fn.text_value as firstname,
                        ln.text_value as lastname,
                        COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM eperson e
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE e.email = %s
                """, (email,))
                return cur.fetchone()
    
    def get_eperson_by_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Get eperson info by UUID. Names are stored in metadatavalue table."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        e.uuid, 
                        e.email,
                        fn.text_value as firstname,
                        ln.text_value as lastname,
                        COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM eperson e
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE e.uuid = %s
                """, (uuid,))
                return cur.fetchone()
    
    def add_review(
        self,
        resource_id: str,
        eperson_id: str,  # UUID
        rating: int,
        comment: str = ""
    ) -> Dict[str, Any]:
        """Add or update a review. Returns the review with user info."""
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Verify eperson exists
                cur.execute("SELECT uuid FROM eperson WHERE uuid = %s", (eperson_id,))
                if not cur.fetchone():
                    raise ValueError(f"User with ID {eperson_id} not found")
                
                # Upsert: insert or update on conflict
                cur.execute("""
                    INSERT INTO oer_reviews (resource_id, eperson_id, rating, comment)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (resource_id, eperson_id) 
                    DO UPDATE SET 
                        rating = EXCLUDED.rating,
                        comment = EXCLUDED.comment,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id, resource_id, eperson_id, rating, comment, 
                              created_at, updated_at, helpful_count
                """, (resource_id, eperson_id, rating, comment))
                
                result = cur.fetchone()
                conn.commit()
                
                # Get user info (names from metadatavalue)
                cur.execute("""
                    SELECT 
                        e.email,
                        fn.text_value as firstname,
                        ln.text_value as lastname,
                        COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM eperson e
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE e.uuid = %s
                """, (eperson_id,))
                user = cur.fetchone()
                
                review = self._serialize_row(result)
                review['email'] = user['email']
                review['display_name'] = user['display_name']
                
                return review
    
    def add_review_by_email(
        self,
        resource_id: str,
        email: str,
        rating: int,
        comment: str = ""
    ) -> Dict[str, Any]:
        """Add or update a review using email. Returns the review with user info."""
        eperson = self.get_eperson_by_email(email)
        if not eperson:
            raise ValueError(f"User with email {email} not found in DSpace")
        
        return self.add_review(resource_id, str(eperson['uuid']), rating, comment)
    
    def _serialize_row(self, row: Dict) -> Dict[str, Any]:
        """Convert datetime fields to ISO strings and UUID to string."""
        if row is None:
            return None
        result = dict(row)
        for key in ['created_at', 'updated_at']:
            if key in result and result[key]:
                result[key] = result[key].isoformat()
        if 'eperson_id' in result and result['eperson_id']:
            result['eperson_id'] = str(result['eperson_id'])
        if 'uuid' in result and result['uuid']:
            result['uuid'] = str(result['uuid'])
        return result
    
    def get_reviews(
        self,
        resource_id: str,
        limit: int = 20,
        offset: int = 0,
        sort_by: str = "created_at",
        order: str = "desc"
    ) -> Dict[str, Any]:
        """Get reviews for a resource with user info."""
        valid_sort = ["created_at", "rating", "helpful_count"]
        if sort_by not in valid_sort:
            sort_by = "created_at"
        
        order = "DESC" if order.lower() == "desc" else "ASC"
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get total count
                cur.execute(
                    "SELECT COUNT(*) as count FROM oer_reviews WHERE resource_id = %s",
                    (resource_id,)
                )
                total = cur.fetchone()['count']
                
                # Get reviews with user info (names from metadatavalue)
                query = f"""
                    SELECT r.*, e.email,
                           fn.text_value as firstname,
                           ln.text_value as lastname,
                           COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM oer_reviews r
                    JOIN eperson e ON r.eperson_id = e.uuid
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE r.resource_id = %s
                    ORDER BY r.{sort_by} {order}
                    LIMIT %s OFFSET %s
                """
                cur.execute(query, (resource_id, limit, offset))
                
                reviews = [self._serialize_row(row) for row in cur.fetchall()]
                
                return {
                    "resource_id": resource_id,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "reviews": reviews
                }
    
    def get_resource_stats(self, resource_id: str) -> Dict[str, Any]:
        """Get rating statistics for a resource."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_reviews,
                        COALESCE(ROUND(AVG(rating)::numeric, 1), 0) as average_rating,
                        SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) as five_star,
                        SUM(CASE WHEN rating = 4 THEN 1 ELSE 0 END) as four_star,
                        SUM(CASE WHEN rating = 3 THEN 1 ELSE 0 END) as three_star,
                        SUM(CASE WHEN rating = 2 THEN 1 ELSE 0 END) as two_star,
                        SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) as one_star
                    FROM oer_reviews
                    WHERE resource_id = %s
                """, (resource_id,))
                
                stats = cur.fetchone()
                
                return {
                    "resource_id": resource_id,
                    "total_reviews": stats['total_reviews'] or 0,
                    "average_rating": float(stats['average_rating'] or 0),
                    "distribution": {
                        "5": stats['five_star'] or 0,
                        "4": stats['four_star'] or 0,
                        "3": stats['three_star'] or 0,
                        "2": stats['two_star'] or 0,
                        "1": stats['one_star'] or 0
                    }
                }
    
    def get_user_review(self, resource_id: str, eperson_id: str) -> Optional[Dict[str, Any]]:
        """Get a user's review for a specific resource."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT r.*, e.email,
                           fn.text_value as firstname,
                           ln.text_value as lastname,
                           COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM oer_reviews r
                    JOIN eperson e ON r.eperson_id = e.uuid
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE r.resource_id = %s AND r.eperson_id = %s
                """, (resource_id, eperson_id))
                row = cur.fetchone()
                return self._serialize_row(row) if row else None
    
    def get_user_review_by_email(self, resource_id: str, email: str) -> Optional[Dict[str, Any]]:
        """Get a user's review by email."""
        eperson = self.get_eperson_by_email(email)
        if not eperson:
            return None
        return self.get_user_review(resource_id, str(eperson['uuid']))
    
    def delete_review(self, resource_id: str, eperson_id: str) -> bool:
        """Delete a user's review."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM oer_reviews WHERE resource_id = %s AND eperson_id = %s",
                    (resource_id, eperson_id)
                )
                conn.commit()
                return cur.rowcount > 0
    
    def mark_helpful(self, review_id: int, eperson_id: str) -> bool:
        """Mark a review as helpful."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Try to insert helpful mark
                    cur.execute("""
                        INSERT INTO oer_review_helpful (review_id, eperson_id)
                        VALUES (%s, %s)
                    """, (review_id, eperson_id))
                    
                    # Update count
                    cur.execute("""
                        UPDATE oer_reviews 
                        SET helpful_count = helpful_count + 1 
                        WHERE id = %s
                    """, (review_id,))
                    
                    conn.commit()
                    return True
                except psycopg2.IntegrityError:
                    # Already marked as helpful
                    conn.rollback()
                    return False
    
    def get_user_reviews(self, eperson_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get all reviews by a user."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT r.*, e.email,
                           fn.text_value as firstname,
                           ln.text_value as lastname,
                           COALESCE(fn.text_value || ' ' || ln.text_value, e.email) as display_name
                    FROM oer_reviews r
                    JOIN eperson e ON r.eperson_id = e.uuid
                    LEFT JOIN metadatavalue fn ON e.uuid = fn.dspace_object_id 
                        AND fn.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'firstname' AND qualifier IS NULL
                        )
                    LEFT JOIN metadatavalue ln ON e.uuid = ln.dspace_object_id 
                        AND ln.metadata_field_id = (
                            SELECT metadata_field_id FROM metadatafieldregistry 
                            WHERE element = 'lastname' AND qualifier IS NULL
                        )
                    WHERE r.eperson_id = %s
                    ORDER BY r.created_at DESC
                    LIMIT %s
                """, (eperson_id, limit))
                
                return [self._serialize_row(row) for row in cur.fetchall()]
    
    def get_top_rated(self, limit: int = 10, min_reviews: int = 3) -> List[Dict[str, Any]]:
        """Get top rated resources."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        resource_id,
                        COUNT(*) as review_count,
                        AVG(rating) as avg_rating
                    FROM oer_reviews
                    GROUP BY resource_id
                    HAVING COUNT(*) >= %s
                    ORDER BY avg_rating DESC, review_count DESC
                    LIMIT %s
                """, (min_reviews, limit))
                
                return [dict(row) for row in cur.fetchall()]


# Global instance
_review_store: Optional[ReviewStore] = None


def get_review_store() -> ReviewStore:
    """Get or create ReviewStore instance."""
    global _review_store
    if _review_store is None:
        _review_store = ReviewStore()
    return _review_store
