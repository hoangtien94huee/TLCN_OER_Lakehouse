#!/usr/bin/env python3
"""
Setup DSpace OER Collection
============================
Creates a Community and Collection for OER resources in DSpace.
Returns the Collection UUID needed for syncing.
"""

import argparse
import json
import os
import sys
import requests

# Configuration (can be overridden by args/env)
DSPACE_API = os.getenv("DSPACE_API_ENDPOINT", "http://localhost:8180/server/api")
DSPACE_USER = os.getenv("DSPACE_USER", "admin@dspace.org")
DSPACE_PASSWORD = os.getenv("DSPACE_PASSWORD", "dspace")

def login(session):
    """Authenticate with DSpace REST API."""
    print("Logging into DSpace...")

    # Step 1: Get CSRF token via status endpoint
    status_resp = session.get(f"{DSPACE_API}/authn/status")
    csrf_token = status_resp.headers.get("DSPACE-XSRF-TOKEN")
    if not csrf_token:
        print("✗ Login failed (missing CSRF token)")
        return False

    # Step 2: Set CSRF token in header and cookie
    session.headers.update({"X-XSRF-TOKEN": csrf_token})
    session.cookies.set("DSPACE-XSRF-COOKIE", csrf_token, domain="", path="/server")

    # Step 3: Authenticate (form data)
    payload = {"user": DSPACE_USER, "password": DSPACE_PASSWORD}
    resp = session.post(
        f"{DSPACE_API}/authn/login",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        allow_redirects=True,
    )
    resp.raise_for_status()

    # Step 4: Get Bearer Token
    auth_header = resp.headers.get("Authorization")
    if auth_header:
        session.headers.update({"Authorization": auth_header})
        new_csrf = resp.headers.get("DSPACE-XSRF-TOKEN")
        if new_csrf:
            session.headers.update({"X-XSRF-TOKEN": new_csrf})
            session.cookies.set("DSPACE-XSRF-COOKIE", new_csrf, domain="", path="/server")
        print("✓ Successfully logged in")
        return True

    print("✗ Login failed")
    return False

def get_top_community(session, community_name: str, community_desc: str):
    """Get the first top-level community UUID (or create one)."""
    print("\nFetching top-level communities...")
    resp = session.get(f"{DSPACE_API}/core/communities/search/top")
    resp.raise_for_status()
    
    communities = resp.json().get("_embedded", {}).get("communities", [])
    
    if communities:
        uuid = communities[0]["uuid"]
        name = communities[0]["name"]
        print(f"✓ Using existing community: {name} ({uuid})")
        return uuid
    
    # Create new top-level community if none exists
    print("Creating new top-level community...")
    return create_community(session, None, community_name, community_desc)

def create_community(session, parent_uuid, name, intro_text):
    """Create a new community."""
    endpoint = f"{DSPACE_API}/core/communities"
    
    metadata = {
        "dc.title": [{"value": name}],
        "dc.description": [{"value": intro_text}]
    }
    
    payload = {"name": name, "metadata": metadata}
    
    if parent_uuid:
        params = {"parent": parent_uuid}
        resp = session.post(endpoint, json=payload, params=params)
    else:
        resp = session.post(endpoint, json=payload)
    
    resp.raise_for_status()
    data = resp.json()
    uuid = data["uuid"]
    print(f"✓ Created community: {name} ({uuid})")
    return uuid

def create_collection(session, parent_community_uuid, name, intro_text):
    """Create a new collection under a community."""
    print(f"\nCreating collection: {name}")
    
    endpoint = f"{DSPACE_API}/core/collections"
    params = {"parent": parent_community_uuid}
    
    metadata = {
        "dc.title": [{"value": name}],
        "dc.description": [{"value": intro_text}]
    }
    
    payload = {"name": name, "metadata": metadata}
    
    resp = session.post(endpoint, json=payload, params=params)
    resp.raise_for_status()
    
    data = resp.json()
    uuid = data["uuid"]
    print(f"✓ Created collection: {name}")
    print(f"✓ Collection UUID: {uuid}")
    return uuid

def main():
    parser = argparse.ArgumentParser(description="Setup DSpace OER collection")
    parser.add_argument("--collection-name", default=os.getenv("DSPACE_COLLECTION_NAME", "Open Educational Resources"))
    parser.add_argument("--collection-desc", default=os.getenv(
        "DSPACE_COLLECTION_DESC",
        "Collection of OER materials from MIT OCW, OpenStax, OER Commons, and other sources. "
        "Includes lecture notes, textbooks, assignments, and supplementary materials.",
    ))
    parser.add_argument("--community-name", default=os.getenv("DSPACE_COMMUNITY_NAME", "OER Repository"))
    parser.add_argument("--community-desc", default=os.getenv(
        "DSPACE_COMMUNITY_DESC",
        "Open Educational Resources collected from various sources",
    ))
    args = parser.parse_args()

    session = requests.Session()
    
    try:
        # Login
        if not login(session):
            sys.exit(1)
        
        # Get or create parent community
        community_uuid = get_top_community(session, args.community_name, args.community_desc)
        
        # Create OER Collection
        collection_uuid = create_collection(
            session,
            community_uuid,
            args.collection_name,
            args.collection_desc,
        )
        
        print("\n" + "="*60)
        print("SUCCESS! DSpace OER Collection is ready.")
        print("="*60)
        print(f"\nCollection UUID: {collection_uuid}")
        print("\nAdd this to your docker-compose.yml:")
        print(f"  - DSPACE_COLLECTION_UUID={collection_uuid}")
        print("\nOr set it in Airflow Variables:")
        print(f"  dspace_collection_uuid = {collection_uuid}")
        print("\n" + "="*60)
        
        return collection_uuid
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
