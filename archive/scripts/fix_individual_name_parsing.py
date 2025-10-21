#!/usr/bin/env python3
"""
Fix Individual Name Parsing Script

Problem: 8,189 individuals have names in the 'name' column but empty
         western_first_name and western_last_name fields.

Solution: Parse the name column into first and last names:
- Names with " and " (e.g., "Ruth and William Scott") -> keep entire name as first_name
- Regular names: first word -> western_first_name, rest -> western_last_name

Usage:
    python scripts/fix_individual_name_parsing.py [--dry-run] [--batch-size N]
"""

import sys
import os
import argparse
import psycopg2
from psycopg2.extras import execute_values

# Database connection string
DB_URL = os.getenv('SUPABASE_DATABASE_URL', os.getenv('DATABASE_URL',
    "postgresql://postgres.oticlgskebuoswlbpucb:8i5oRvtQN8ciglnR@aws-0-us-east-2.pooler.supabase.com:6543/postgres"))


def get_unparsed_individuals(cursor):
    """Get all individuals with unparsed names"""
    cursor.execute("""
        SELECT uuid, name
        FROM ny_puc_data.individuals
        WHERE western_first_name = ''
          AND western_last_name = ''
          AND name != ''
        ORDER BY created_at
    """)
    return cursor.fetchall()


def parse_name(full_name):
    """
    Parse full name into first and last names.

    Rules:
    - If name contains " and ", keep entire name as first_name, last_name = ''
    - Otherwise: first word -> first_name, remaining -> last_name

    Returns: (first_name, last_name)
    """
    full_name = full_name.strip()

    # Handle names with " and " (e.g., "Ruth and William Scott")
    if ' and ' in full_name.lower():
        return (full_name, '')

    # Handle single-word names
    if ' ' not in full_name:
        return (full_name, '')

    # Regular parsing: first word = first name, rest = last name
    parts = full_name.split(' ', 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ''

    return (first_name, last_name)


def preview_changes(individuals, limit=20):
    """Preview name parsing changes"""
    print(f"\n{'='*80}")
    print(f"PREVIEW: First {limit} name parsing changes:")
    print(f"{'='*80}\n")

    for i, (uuid, name) in enumerate(individuals[:limit]):
        first, last = parse_name(name)
        print(f"{i+1}. '{name}'")
        print(f"   → First: '{first}'")
        print(f"   → Last:  '{last}'")
        if ' and ' in name.lower():
            print(f"   ⚠️  Contains 'and' - keeping full name as first_name")
        print()

    if len(individuals) > limit:
        print(f"... and {len(individuals) - limit} more records\n")


def update_individuals(conn, cursor, individuals, batch_size=1000):
    """Update individuals with parsed names"""
    total = len(individuals)
    print(f"\nUpdating {total} individuals...")

    updated_count = 0
    and_name_count = 0

    for i in range(0, total, batch_size):
        batch = individuals[i:i+batch_size]

        # Prepare update data
        update_data = []
        for uuid, name in batch:
            first, last = parse_name(name)
            update_data.append((first, last, uuid))
            if ' and ' in name.lower():
                and_name_count += 1

        # Batch update
        cursor.executemany("""
            UPDATE ny_puc_data.individuals
            SET western_first_name = %s,
                western_last_name = %s,
                updated_at = NOW()
            WHERE uuid = %s
        """, update_data)

        # Commit after each batch
        conn.commit()

        updated_count += len(batch)
        print(f"  Progress: {updated_count}/{total} ({100*updated_count/total:.1f}%) - committed")

    return updated_count, and_name_count


def verify_results(cursor):
    """Verify the update results"""
    print(f"\n{'='*80}")
    print("VERIFICATION")
    print(f"{'='*80}\n")

    # Count remaining unparsed individuals
    cursor.execute("""
        SELECT COUNT(*)
        FROM ny_puc_data.individuals
        WHERE western_first_name = ''
          AND western_last_name = ''
          AND name != ''
    """)
    remaining = cursor.fetchone()[0]

    # Count individuals with parsed names
    cursor.execute("""
        SELECT COUNT(*)
        FROM ny_puc_data.individuals
        WHERE western_first_name != '' OR western_last_name != ''
    """)
    parsed = cursor.fetchone()[0]

    # Count "and" names
    cursor.execute("""
        SELECT COUNT(*)
        FROM ny_puc_data.individuals
        WHERE western_first_name ILIKE '% and %'
          AND western_last_name = ''
    """)
    and_names = cursor.fetchone()[0]

    print(f"Remaining unparsed individuals: {remaining}")
    print(f"Total individuals with parsed names: {parsed}")
    print(f"Individuals with 'and' in name (kept as first_name): {and_names}")

    if remaining == 0:
        print("\n✅ All individuals successfully parsed!")
    else:
        print(f"\n⚠️  Warning: {remaining} individuals still unparsed")

    # Sample some results
    print(f"\n{'='*80}")
    print("Sample of updated records:")
    print(f"{'='*80}\n")

    cursor.execute("""
        SELECT name, western_first_name, western_last_name
        FROM ny_puc_data.individuals
        WHERE updated_at > NOW() - INTERVAL '5 minutes'
          AND western_first_name != ''
        ORDER BY updated_at DESC
        LIMIT 10
    """)

    for name, first, last in cursor.fetchall():
        print(f"Name: {name}")
        print(f"  First: '{first}'")
        print(f"  Last:  '{last}'")
        print()


def main():
    parser = argparse.ArgumentParser(description='Fix individual name parsing')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview changes without updating database')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for updates (default: 1000)')
    parser.add_argument('--preview-limit', type=int, default=20,
                       help='Number of records to preview (default: 20)')
    parser.add_argument('--yes', '-y', action='store_true',
                       help='Skip confirmation prompt')

    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print("FIX INDIVIDUAL NAME PARSING")
        print(f"{'='*80}\n")

        # Get unparsed individuals
        print("Fetching unparsed individuals...")
        individuals = get_unparsed_individuals(cursor)
        print(f"Found {len(individuals)} individuals to parse")

        if len(individuals) == 0:
            print("\n✅ No unparsed individuals found. Nothing to do!")
            return

        # Preview changes
        preview_changes(individuals, limit=args.preview_limit)

        if args.dry_run:
            print("\n🔍 DRY RUN MODE - No changes will be made")
            print(f"\nTo apply changes, run without --dry-run flag:")
            print(f"  python scripts/fix_individual_name_parsing.py")
            return

        # Confirm
        if not args.yes:
            response = input("\nProceed with update? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("\n❌ Update cancelled")
                return
        else:
            print("\n✓ Auto-confirmed (--yes flag)")

        # Update
        updated, and_count = update_individuals(conn, cursor, individuals, args.batch_size)

        # All batches already committed
        print(f"\n✅ Successfully updated {updated} individuals")
        print(f"   - Regular names: {updated - and_count}")
        print(f"   - Names with 'and': {and_count}")

        # Verify
        verify_results(cursor)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
