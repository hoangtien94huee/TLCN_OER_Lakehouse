#!/usr/bin/env python3
"""
OER Lakehouse REST API
======================

Fast REST API layer for OER search and discovery.
Uses PostgreSQL ODS for optimized queries.

Based on the data flow: Gold Layer → ODS (PostgreSQL) → REST API
"""

import os
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

try:
    from flask import Flask, request, jsonify, g
    from flask_cors import CORS
    import psycopg2
    import psycopg2.extras
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("Warning: Flask/psycopg2 not available. Install with: pip install flask flask-cors psycopg2-binary")

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'oer_lakehouse'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432))
}

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def format_api_response(data: Any, success: bool = True, message: str = None, total: int = None) -> Dict:
    """Standardized API response format"""
    response = {
        'success': success,
        'timestamp': datetime.utcnow().isoformat(),
        'data': data
    }
    
    if message:
        response['message'] = message
    if total is not None:
        response['total'] = total
        
    return response

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
        conn.close()
        
        return jsonify(format_api_response({
            'status': 'healthy',
            'database': 'connected',
            'api_version': '1.0.0'
        }))
    except Exception as e:
        return jsonify(format_api_response(
            {'status': 'unhealthy', 'error': str(e)},
            success=False
        )), 500

@app.route('/api/search', methods=['GET'])
def search_resources():
    """
    Search OER resources with full-text search and filtering
    
    Query Parameters:
    - q: search query (required)
    - subject: filter by subject
    - source: filter by source system
    - language: filter by language code
    - has_multimedia: filter resources with multimedia (true/false)
    - min_quality: minimum quality score (0.0-1.0)
    - limit: number of results (default: 20, max: 100)
    - offset: pagination offset (default: 0)
    """
    try:
        # Parse query parameters
        query = request.args.get('q', '').strip()
        subject = request.args.get('subject')
        source = request.args.get('source') 
        language = request.args.get('language')
        has_multimedia = request.args.get('has_multimedia')
        min_quality = request.args.get('min_quality', type=float)
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))
        
        if not query:
            return jsonify(format_api_response(
                [], success=False, message="Query parameter 'q' is required"
            )), 400
        
        # Build SQL query
        sql = """
            SELECT 
                id, title, description, creators, subjects,
                source_system, publisher_name, language_code,
                resource_type, license_name, has_multimedia,
                data_quality_score, search_rank,
                ts_rank_cd(search_vector, plainto_tsquery('english', %s)) as relevance_score
            FROM api_resources_search
            WHERE search_vector @@ plainto_tsquery('english', %s)
        """
        
        params = [query, query]
        
        # Add filters
        if subject:
            sql += " AND %s = ANY(subjects)"
            params.append(subject)
            
        if source:
            sql += " AND source_system = %s"
            params.append(source)
            
        if language:
            sql += " AND language_code = %s"
            params.append(language)
            
        if has_multimedia is not None:
            sql += " AND has_multimedia = %s"
            params.append(has_multimedia.lower() == 'true')
            
        if min_quality is not None:
            sql += " AND data_quality_score >= %s"
            params.append(min_quality)
        
        # Order and pagination
        sql += " ORDER BY relevance_score DESC, search_rank DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        # Execute query
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            results = cur.fetchall()
            
            # Get total count for pagination
            count_sql = sql.split('ORDER BY')[0].replace(
                'SELECT id, title, description, creators, subjects, source_system, publisher_name, language_code, resource_type, license_name, has_multimedia, data_quality_score, search_rank, ts_rank_cd(search_vector, plainto_tsquery(\'english\', %s)) as relevance_score',
                'SELECT COUNT(*)'
            )
            cur.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total = cur.fetchone()['count']
            
        conn.close()
        
        # Format results
        formatted_results = []
        for row in results:
            formatted_results.append({
                'id': row['id'],
                'title': row['title'],
                'description': row['description'][:200] + '...' if row['description'] and len(row['description']) > 200 else row['description'],
                'creators': row['creators'],
                'subjects': row['subjects'],
                'source': {
                    'system': row['source_system'],
                    'publisher': row['publisher_name']
                },
                'language': row['language_code'],
                'type': row['resource_type'],
                'license': row['license_name'],
                'has_multimedia': row['has_multimedia'],
                'quality_score': float(row['data_quality_score']),
                'relevance_score': float(row['relevance_score'])
            })
        
        return jsonify(format_api_response(
            formatted_results,
            message=f"Found {total} resources matching '{query}'",
            total=total
        ))
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify(format_api_response(
            [], success=False, message=f"Search failed: {str(e)}"
        )), 500

@app.route('/api/resources/<resource_id>', methods=['GET'])
def get_resource_detail(resource_id: str):
    """Get detailed information for a specific resource"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM ods_oer_resources 
                WHERE id = %s
            """, [resource_id])
            
            result = cur.fetchone()
            
        conn.close()
        
        if not result:
            return jsonify(format_api_response(
                None, success=False, message=f"Resource '{resource_id}' not found"
            )), 404
        
        # Format detailed response
        resource_detail = {
            'id': result['id'],
            'title': result['title'],
            'description': result['description'],
            'creators': result['creators'],
            'subjects': result['subjects'],
            'source': {
                'system': result['source_system'],
                'system_name': result['source_full_name'],
                'publisher': result['publisher_name'],
                'publisher_type': result['publisher_type']
            },
            'language': {
                'code': result['language_code'],
                'name': result['language_name']
            },
            'type': {
                'name': result['resource_type'],
                'category': result['resource_category']
            },
            'license': {
                'name': result['license_name'],
                'openness_score': float(result['openness_score']) if result['openness_score'] else None
            },
            'content': {
                'word_count': result['content_length_words'],
                'has_multimedia': result['multimedia_count'] > 0,
                'multimedia_count': result['multimedia_count'],
                'pdf_count': result['pdf_count'],
                'video_count': result['video_count'],
                'transcript_available': result['transcript_available']
            },
            'quality': {
                'data_quality_score': float(result['data_quality_score']),
                'completeness_score': float(result['completeness_score']),
                'freshness_score': float(result['freshness_score']),
                'search_rank': float(result['search_rank'])
            },
            'timestamps': {
                'scraped_at': result['scraped_timestamp'].isoformat() if result['scraped_timestamp'] else None,
                'last_updated': result['last_updated'].isoformat() if result['last_updated'] else None
            }
        }
        
        return jsonify(format_api_response(resource_detail))
        
    except Exception as e:
        logger.error(f"Resource detail error: {e}")
        return jsonify(format_api_response(
            None, success=False, message=f"Failed to get resource details: {str(e)}"
        )), 500

@app.route('/api/popular', methods=['GET'])
def get_popular_resources():
    """Get popular/featured resources"""
    try:
        subject = request.args.get('subject')
        limit = min(int(request.args.get('limit', 20)), 50)
        
        sql = "SELECT * FROM api_popular_resources"
        params = []
        
        if subject:
            sql += " WHERE %s = ANY(subjects)"
            params.append(subject)
            
        sql += f" LIMIT %s"
        params.append(limit)
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            results = cur.fetchall()
            
        conn.close()
        
        formatted_results = []
        for row in results:
            formatted_results.append({
                'id': row['id'],
                'title': row['title'],
                'creators': row['creators'],
                'subjects': row['subjects'],
                'source_system': row['source_system'],
                'publisher': row['publisher_name'],
                'multimedia_count': row['multimedia_count'],
                'quality_score': float(row['data_quality_score'])
            })
        
        return jsonify(format_api_response(
            formatted_results,
            message=f"Retrieved {len(formatted_results)} popular resources"
        ))
        
    except Exception as e:
        logger.error(f"Popular resources error: {e}")
        return jsonify(format_api_response(
            [], success=False, message=f"Failed to get popular resources: {str(e)}"
        )), 500

@app.route('/api/subjects', methods=['GET'])
def get_subjects_stats():
    """Get subject statistics for faceted search"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM api_subject_stats ORDER BY resource_count DESC")
            results = cur.fetchall()
            
        conn.close()
        
        formatted_results = []
        for row in results:
            formatted_results.append({
                'subject': row['subject_name'],
                'resource_count': row['resource_count'],
                'avg_quality': float(row['avg_quality']),
                'multimedia_resources': row['multimedia_resources']
            })
        
        return jsonify(format_api_response(
            formatted_results,
            message=f"Retrieved statistics for {len(formatted_results)} subjects"
        ))
        
    except Exception as e:
        logger.error(f"Subject stats error: {e}")
        return jsonify(format_api_response(
            [], success=False, message=f"Failed to get subject statistics: {str(e)}"
        )), 500

@app.route('/api/stats', methods=['GET'])
def get_api_stats():
    """Get overall API and database statistics"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Overall stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_resources,
                    COUNT(DISTINCT source_system) as total_sources,
                    COUNT(*) FILTER (WHERE has_multimedia) as multimedia_resources,
                    AVG(data_quality_score) as avg_quality_score,
                    COUNT(DISTINCT language_code) as total_languages
                FROM ods_oer_resources
            """)
            overall_stats = cur.fetchone()
            
            # By source system
            cur.execute("""
                SELECT 
                    source_system,
                    COUNT(*) as resource_count,
                    AVG(data_quality_score) as avg_quality
                FROM ods_oer_resources
                GROUP BY source_system
                ORDER BY resource_count DESC
            """)
            source_stats = cur.fetchall()
            
        conn.close()
        
        stats = {
            'overall': {
                'total_resources': overall_stats['total_resources'],
                'total_sources': overall_stats['total_sources'],
                'multimedia_resources': overall_stats['multimedia_resources'],
                'avg_quality_score': float(overall_stats['avg_quality_score']),
                'total_languages': overall_stats['total_languages']
            },
            'by_source': [
                {
                    'source': row['source_system'],
                    'resource_count': row['resource_count'],
                    'avg_quality': float(row['avg_quality'])
                }
                for row in source_stats
            ]
        }
        
        return jsonify(format_api_response(stats))
        
    except Exception as e:
        logger.error(f"API stats error: {e}")
        return jsonify(format_api_response(
            {}, success=False, message=f"Failed to get statistics: {str(e)}"
        )), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify(format_api_response(
        None, success=False, message="Endpoint not found"
    )), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify(format_api_response(
        None, success=False, message="Internal server error"
    )), 500

if __name__ == '__main__':
    if not FLASK_AVAILABLE:
        print("❌ Flask dependencies not available!")
        print("Install with: pip install flask flask-cors psycopg2-binary")
        exit(1)
    
    print("🚀 OER Lakehouse REST API starting...")
    print(f"📊 Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("📋 Available endpoints:")
    print("   GET /api/health - Health check")
    print("   GET /api/search?q=<query> - Search resources")
    print("   GET /api/resources/<id> - Get resource details")
    print("   GET /api/popular - Get popular resources")
    print("   GET /api/subjects - Get subject statistics")
    print("   GET /api/stats - Get API statistics")
    
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('API_PORT', 8080)),
        debug=os.getenv('API_DEBUG', 'false').lower() == 'true'
    )