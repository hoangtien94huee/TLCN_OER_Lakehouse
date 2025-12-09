#!/usr/bin/env python3
"""
Import users from user_final.csv to DSpace database.

This script generates SQL or DSpace CLI commands to create users.
Email: {mssv}@gmail.com
Password: {mssv}

Usage:
    python import_users_to_dspace.py --mode cli > create_users.sh
    python import_users_to_dspace.py --mode sql > create_users.sql
    python import_users_to_dspace.py --mode api --dspace-url http://localhost:8080
"""

import argparse
import csv
import hashlib
import uuid
import os
import sys
import requests
from typing import Set, Tuple

# Path to user data
USER_DATA_PATH = os.getenv("USER_DATA_PATH", "../data/user_final.csv")


def parse_name(full_name: str) -> Tuple[str, str]:
    """Split Vietnamese name into firstname and lastname.
    
    Vietnamese names: Họ + Tên đệm + Tên
    DSpace: firstname = Tên, lastname = Họ + Tên đệm
    
    Example: "Nguyễn Sĩ Thành Danh" -> firstname="Danh", lastname="Nguyễn Sĩ Thành"
    """
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("User", "Unknown")
    elif len(parts) == 1:
        return (parts[0], "")
    else:
        firstname = parts[-1]  # Last word is the given name
        lastname = " ".join(parts[:-1])  # Rest is family name
        return (firstname, lastname)


def load_unique_users(csv_path: str) -> list:
    """Load unique users from CSV file."""
    users = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            student_id = row.get('So_the', '').strip()
            full_name = row.get('Ho_ten', '').strip()
            
            if student_id and student_id not in users:
                firstname, lastname = parse_name(full_name)
                users[student_id] = {
                    'student_id': student_id,
                    'email': f"{student_id}@gmail.com",
                    'password': student_id,
                    'firstname': firstname,
                    'lastname': lastname,
                    'full_name': full_name
                }
    
    return list(users.values())


def generate_cli_commands(users: list) -> str:
    """Generate DSpace CLI commands to create users."""
    lines = [
        "#!/bin/bash",
        "# DSpace user import script",
        f"# Generated for {len(users)} users",
        "",
        "# Run inside DSpace container:",
        "# docker exec -it dspace bash",
        "# Then execute this script",
        "",
    ]
    
    for user in users:
        # Escape special characters in names
        firstname = user['firstname'].replace('"', '\\"')
        lastname = user['lastname'].replace('"', '\\"')
        
        cmd = (
            f'/dspace/bin/dspace user -a '
            f'-m "{user["email"]}" '
            f'-p "{user["password"]}" '
            f'-g "{firstname}" '
            f'-s "{lastname}"'
        )
        lines.append(cmd)
    
    return "\n".join(lines)


def generate_sql_commands(users: list) -> str:
    """Generate SQL commands to insert users into DSpace database.
    
    Note: DSpace 7+ uses different password hashing (bcrypt).
    This generates SQL for direct insertion - use with caution!
    """
    lines = [
        "-- DSpace user import SQL",
        f"-- Generated for {len(users)} users",
        "-- WARNING: Password hashing must match DSpace version!",
        "-- For DSpace 7+, passwords are bcrypt hashed.",
        "",
        "-- Run inside PostgreSQL:",
        "-- docker exec -it dspace-db psql -U dspace -d dspace -f create_users.sql",
        "",
    ]
    
    for user in users:
        user_uuid = str(uuid.uuid4())
        eperson_uuid = str(uuid.uuid4())
        
        # Note: This is a placeholder - actual password needs proper hashing
        # DSpace 7+ uses Spring Security with BCrypt
        lines.append(f"""
-- User: {user['full_name']} ({user['student_id']})
INSERT INTO eperson (uuid, email, firstname, lastname, can_log_in, self_registered)
VALUES (
    '{eperson_uuid}',
    '{user['email']}',
    '{user['firstname'].replace("'", "''")}',
    '{user['lastname'].replace("'", "''")}',
    true,
    false
) ON CONFLICT (email) DO NOTHING;
""")
    
    return "\n".join(lines)


def create_users_via_api(users: list, dspace_url: str, admin_email: str, admin_password: str) -> dict:
    """Create users via DSpace REST API.
    
    Requires admin authentication.
    """
    results = {'success': 0, 'failed': 0, 'errors': []}
    
    # Login as admin
    session = requests.Session()
    
    try:
        # Get CSRF token
        csrf_resp = session.get(f"{dspace_url}/server/api/authn/status")
        csrf_token = csrf_resp.headers.get('DSPACE-XSRF-TOKEN')
        
        # Login
        login_resp = session.post(
            f"{dspace_url}/server/api/authn/login",
            headers={
                'X-XSRF-TOKEN': csrf_token,
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={'user': admin_email, 'password': admin_password}
        )
        
        if login_resp.status_code != 200:
            return {'error': f'Admin login failed: {login_resp.status_code}'}
        
        auth_token = login_resp.headers.get('Authorization')
        
        # Create each user
        for user in users:
            try:
                # Create EPerson
                create_resp = session.post(
                    f"{dspace_url}/server/api/eperson/epersons",
                    headers={
                        'Authorization': auth_token,
                        'X-XSRF-TOKEN': csrf_token,
                        'Content-Type': 'application/json'
                    },
                    json={
                        'email': user['email'],
                        'firstname': user['firstname'],
                        'lastname': user['lastname'],
                        'canLogIn': True,
                        'requireCertificate': False
                    }
                )
                
                if create_resp.status_code in [200, 201]:
                    # Set password (separate endpoint in DSpace 7+)
                    eperson_data = create_resp.json()
                    eperson_id = eperson_data.get('id')
                    
                    # Note: Password setting may require different endpoint
                    results['success'] += 1
                    print(f"Created: {user['email']}")
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{user['email']}: {create_resp.status_code}")
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{user['email']}: {str(e)}")
        
    except Exception as e:
        return {'error': str(e)}
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Import users to DSpace')
    parser.add_argument('--mode', choices=['cli', 'sql', 'api', 'count'], 
                        default='cli', help='Output mode')
    parser.add_argument('--csv', default=USER_DATA_PATH, help='Path to user CSV')
    parser.add_argument('--dspace-url', default='http://localhost:8080', 
                        help='DSpace server URL (for API mode)')
    parser.add_argument('--admin-email', default='admin@example.com',
                        help='Admin email (for API mode)')
    parser.add_argument('--admin-password', default='admin',
                        help='Admin password (for API mode)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of users (0 = all)')
    parser.add_argument('--output', '-o', default=None,
                        help='Output file path (default: stdout)')
    
    args = parser.parse_args()
    
    # Load users
    csv_path = os.path.join(os.path.dirname(__file__), args.csv)
    if not os.path.exists(csv_path):
        csv_path = args.csv
    
    users = load_unique_users(csv_path)
    
    if args.limit > 0:
        users = users[:args.limit]
    
    if args.mode == 'count':
        print(f"Total unique users: {len(users)}")
        print(f"\nSample users:")
        for u in users[:5]:
            # Safe print for Windows
            try:
                print(f"  {u['email']} - {u['full_name']}")
            except UnicodeEncodeError:
                print(f"  {u['email']} - {u['student_id']}")
        return
    
    # Generate output
    if args.mode == 'cli':
        output = generate_cli_commands(users)
    elif args.mode == 'sql':
        output = generate_sql_commands(users)
    elif args.mode == 'api':
        results = create_users_via_api(
            users, 
            args.dspace_url,
            args.admin_email,
            args.admin_password
        )
        print(f"Results: {results}")
        return
    
    # Write output
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"Written to {args.output} ({len(users)} users)")
    else:
        # Write to stdout with UTF-8 encoding
        sys.stdout.reconfigure(encoding='utf-8')
        print(output)


if __name__ == '__main__':
    main()
