import pandas as pd
from elasticsearch import Elasticsearch
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logger = logging.getLogger(__name__)

class OERRecommender:
    def __init__(self, es_host: str = "http://localhost:9200", es_index: str = "oer_resources"):
        """
        Initialize the OER Recommender.
        """
        self.es_host = es_host
        self.es_index = es_index
        self.es = Elasticsearch(self.es_host)
        
    def check_connection(self) -> bool:
        """Check if Elasticsearch is reachable."""
        try:
            return self.es.ping()
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            return False

    def load_user_data(self, file_path: str) -> pd.DataFrame:
        """
        Load user borrowing history from CSV.
        """
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            raise

    def get_user_topics(self, df: pd.DataFrame, user_id: int) -> Dict[str, int]:
        """
        Extract topics and their frequency for a specific user.
        """
        user_history = df[df['So_the_ID'] == user_id]
        if user_history.empty:
            logger.warning(f"No history found for user {user_id}")
            return {}
            
        # Count frequency of each topic
        topic_counts = user_history['Chu_de'].dropna().value_counts().to_dict()
        return topic_counts

    def build_recommendation_query(self, topic_counts: Dict[str, int], size: int = 10) -> Dict[str, Any]:
        should_clauses = []
        
        # Normalize frequencies to avoid excessive boosting values
        # (e.g., if max freq is 100, we don't want boost=100, maybe log scale or cap)
        # For simplicity, we'll use raw frequency but capped at 5 for now
        
        for topic, count in topic_counts.items():
            # Cap boost to avoid skewing too much towards one topic
            boost_val = min(count, 5) 
            
            should_clauses.append({
                "multi_match": {
                    "query": topic,
                    "fields": ["title^3", "description", "subjects^2"],
                    "fuzziness": "AUTO",
                    "boost": boost_val
                }
            })
        
        query = {
            "size": size,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            }
        }
        return query

    def recommend_for_user(self, user_id: int, topic_counts: Dict[str, int], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Generate recommendations for a user based on topic frequencies.
        """
        if not topic_counts:
            return []

        query = self.build_recommendation_query(topic_counts, size=limit)
        
        try:
            response = self.es.search(index=self.es_index, body=query)
            hits = response['hits']['hits']
            
            recommendations = []
            for hit in hits:
                source = hit['_source']
                rec = {
                    "title": source.get("title"),
                    "url": source.get("url"), # Assuming url field exists, or construct it
                    "score": hit['_score'],
                    "matched_topics": list(topic_counts.keys()) # Simplified
                }
                recommendations.append(rec)
                
            return recommendations
        except Exception as e:
            logger.error(f"Error executing search for user {user_id}: {e}")
            return []

if __name__ == "__main__":
    # Example usage for testing
    import sys
    import os
    
    # Configure basic logging to stdout
    logging.basicConfig(level=logging.INFO)
    
    # Get configuration from environment variables or use Docker defaults
    es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
    data_path = os.getenv("USER_DATA_PATH", "/opt/airflow/database/user_final.csv")
    
    print(f"Connecting to Elasticsearch at {es_host}")
    print(f"Loading data from {data_path}")

    recommender = OERRecommender(es_host=es_host)
    
    if recommender.check_connection():
        print("Connected to Elasticsearch")
        
        try:
            df = recommender.load_user_data(data_path)
            
            # Pick a sample user
            if not df.empty:
                sample_user_id = df['So_the_ID'].value_counts().index[0]
                print(f"Testing recommendation for User ID: {sample_user_id}")
                
                topics = recommender.get_user_topics(df, sample_user_id)
                print(f"User Topics: {topics}")
                
                recs = recommender.recommend_for_user(sample_user_id, topics)
                
                print(f"\nFound {len(recs)} recommendations:")
                for rec in recs:
                    print(f"- {rec['title']} (Score: {rec['score']})")
            else:
                print("Dataframe is empty.")
                
        except FileNotFoundError:
            print(f"Data file not found at {data_path}")
            print("Make sure the file exists and the path is correct inside the container.")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print("Could not connect to Elasticsearch")
