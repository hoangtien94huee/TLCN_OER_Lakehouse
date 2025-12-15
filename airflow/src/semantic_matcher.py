#!/usr/bin/env python3
"""
Semantic Subject Matcher
========================

Uses sentence embeddings to match OER resources (English) with curriculum subjects.
More accurate than keyword matching - understands semantic meaning.

Example:
    Keyword match: "Learning Management System" → matches "Machine Learning" (WRONG!)
    Semantic match: "Learning Management System" → no match (CORRECT!)
    Semantic match: "Neural Networks and Deep Learning" → matches "Machine Learning" (CORRECT!)

Model: all-MiniLM-L6-v2 (~22MB, 384 dimensions)
"""

import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

_model = None
_subject_embeddings: Optional[np.ndarray] = None
_subject_data: Optional[List[Dict]] = None


def get_model():
    """Lazy load the sentence transformer model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info("Loaded all-MiniLM-L6-v2 model")
    return _model


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8))


def build_subject_embeddings(
    subjects: List[Dict],
    cache_path: str = "/tmp/subject_embeddings.npy"
) -> tuple:
    """Build embeddings for subject_name_en only."""
    global _subject_embeddings, _subject_data
    
    # Try cache first
    cache = Path(cache_path)
    data_cache = cache.with_suffix('.json')
    
    if cache.exists() and data_cache.exists():
        try:
            _subject_embeddings = np.load(cache)
            with open(data_cache, 'r') as f:
                _subject_data = json.load(f)
            if len(_subject_embeddings) == len(_subject_data):
                logger.info(f"Loaded {len(_subject_data)} cached embeddings")
                return _subject_embeddings, _subject_data
        except Exception as e:
            logger.warning(f"Cache load failed: {e}")
    
    # Build embeddings from subject_name_en only
    model = get_model()
    texts = []
    valid_subjects = []
    
    for subj in subjects:
        name_en = subj.get('subject_name_en')
        # Skip if None or empty after stripping
        if name_en and isinstance(name_en, str):
            name_en = name_en.strip()
            if name_en:
                texts.append(name_en)
                valid_subjects.append(subj)
    
    if not texts:
        return np.array([]), []
    
    logger.info(f"Building embeddings for {len(texts)} subjects...")
    # sentence-transformers 5.x: normalize_embeddings for cosine similarity
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    
    # Convert to numpy array if needed
    if hasattr(embeddings, 'cpu'):
        embeddings = embeddings.cpu().numpy()
    else:
        embeddings = np.array(embeddings)
    
    # Cache
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        np.save(cache, embeddings)
        with open(data_cache, 'w') as f:
            json.dump(valid_subjects, f)
        logger.info(f"Cached to {cache}")
    except Exception as e:
        logger.warning(f"Cache save failed: {e}")
    
    _subject_embeddings = embeddings
    _subject_data = valid_subjects
    return embeddings, valid_subjects


def match_subjects(
    title: str,
    description: Optional[str] = None,
    subjects: Optional[List[Dict]] = None,
    threshold: float = 0.45,
    top_k: int = 3,
    reference_path: str = "/opt/airflow/reference"
) -> List[Dict[str, Any]]:
    """
    Match OER to subjects using semantic similarity.
    
    Args:
        title: OER title (English)
        description: OER description (optional)
        subjects: Pre-loaded subjects list
        threshold: Min similarity (0.45 recommended for balanced precision/recall)
        top_k: Max subjects to return
        
    Returns:
        List of matched subjects with similarity scores
    """
    global _subject_embeddings, _subject_data
    
    # Load subjects
    if subjects is None:
        subj_file = Path(reference_path) / "subjects.json"
        if subj_file.exists():
            with open(subj_file, 'r', encoding='utf-8') as f:
                subjects = json.load(f)
        else:
            return []
    
    # Build embeddings if needed
    if _subject_embeddings is None:
        _subject_embeddings, _subject_data = build_subject_embeddings(subjects)
    
    if len(_subject_embeddings) == 0:
        return []
    
    # Create OER text (title weighted more than description)
    oer_text = title or ""
    if description:
        # Title repeated for emphasis + truncated description
        oer_text = f"{title}. {title}. {description[:300]}"
    
    if not oer_text.strip():
        return []
    
    # Encode OER
    model = get_model()
    oer_emb = model.encode(oer_text, normalize_embeddings=True)
    
    # Convert to numpy if needed
    if hasattr(oer_emb, 'cpu'):
        oer_emb = oer_emb.cpu().numpy()
    
    # Compute similarities (fast with normalized vectors: dot product = cosine similarity)
    similarities = np.dot(_subject_embeddings, oer_emb)
    
    # Filter and sort
    results = []
    for idx in np.argsort(similarities)[::-1]:
        sim = float(similarities[idx])
        if sim < threshold:
            break
        if len(results) >= top_k:
            break
            
        subj = _subject_data[idx]
        results.append({
            "subject_id": subj.get("subject_id"),
            "subject_name": subj.get("subject_name"),
            "subject_name_en": subj.get("subject_name_en"),
            "subject_code": subj.get("subject_code"),
            "similarity": round(sim, 4),
        })
    
    return results


class SemanticMatcher:
    """Batch-friendly semantic matcher."""
    
    def __init__(self, reference_path: str = "/opt/airflow/reference", threshold: float = 0.38, top_k: int = 3):
        self.reference_path = reference_path
        self.threshold = threshold
        self.top_k = top_k
        self._initialized = False
        
    def initialize(self):
        """Pre-load model and embeddings."""
        if self._initialized:
            return
        subj_file = Path(self.reference_path) / "subjects.json"
        if subj_file.exists():
            with open(subj_file, 'r', encoding='utf-8') as f:
                subjects = json.load(f)
            build_subject_embeddings(subjects)
        get_model()
        self._initialized = True
        
    def match(self, title: str, description: Optional[str] = None) -> List[Dict]:
        """Match single OER."""
        if not self._initialized:
            self.initialize()
        return match_subjects(title, description, threshold=self.threshold, top_k=self.top_k)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # Test subjects
    test_subjects = [
        {"subject_id": 1, "subject_name": "Học máy", "subject_name_en": "Machine Learning", "subject_code": "CS401"},
        {"subject_id": 2, "subject_name": "Cấu trúc dữ liệu", "subject_name_en": "Data Structures and Algorithms", "subject_code": "CS201"},
        {"subject_id": 3, "subject_name": "Khai phá dữ liệu", "subject_name_en": "Data Mining", "subject_code": "CS402"},
        {"subject_id": 4, "subject_name": "Mạng máy tính", "subject_name_en": "Computer Networks", "subject_code": "CS301"},
        {"subject_id": 5, "subject_name": "Cơ sở dữ liệu", "subject_name_en": "Database Management Systems", "subject_code": "CS302"},
        {"subject_id": 6, "subject_name": "Trí tuệ nhân tạo", "subject_name_en": "Artificial Intelligence", "subject_code": "CS403"},
    ]
    
    # Setup
    import tempfile
    ref_path = tempfile.mkdtemp()
    with open(f"{ref_path}/subjects.json", 'w') as f:
        json.dump(test_subjects, f)
    
    # Test cases: (title, description, expected_match, should_NOT_match)
    test_cases = [
        ("Deep Learning for Computer Vision", None, "Machine Learning", None),
        ("Introduction to Neural Networks", "Course about training neural networks", "Machine Learning", None),
        ("E-Learning Management System", "Building online learning platforms", None, "Machine Learning"),  # FALSE POSITIVE TEST
        ("Sorting and Searching Algorithms", "Covers quicksort, mergesort, binary search", "Data Structures", None),
        ("SQL and Relational Databases", None, "Database", None),
        ("TCP/IP and Network Protocols", None, "Computer Networks", None),
        ("Mining Customer Behavior Patterns", "Extract insights from transaction data", "Data Mining", None),
        ("Introduction to AI and Expert Systems", None, "Artificial Intelligence", None),
    ]
    
    print("\n" + "="*70)    
    print("SEMANTIC MATCHING TEST - Checking accuracy")
    print("="*70)
    
    correct = 0
    total = len(test_cases)
    
    for title, desc, expected, should_not in test_cases:
        matches = match_subjects(title, desc, subjects=test_subjects, threshold=0.38, reference_path=ref_path)
        
        match_names = [m['subject_name_en'] for m in matches]
        
        # Check result
        if expected:
            hit = any(expected.lower() in m.lower() for m in match_names)
        else:
            hit = len(matches) == 0  # Should have no matches
            
        if should_not and any(should_not.lower() in m.lower() for m in match_names):
            hit = False  # Should NOT match this
        
        status = "✓" if hit else "✗"
        correct += hit
        
        print(f"\n{status} '{title[:50]}'")
        if matches:
            for m in matches[:2]:
                print(f"   → {m['subject_name_en']}: {m['similarity']:.1%}")
        else:
            print("   → No match")
        
        if expected and not hit:
            print(f"   Expected: {expected}")
    
    print(f"\n{'='*70}")
    print(f"Accuracy: {correct}/{total} ({correct/total:.0%})")
    print("="*70)
