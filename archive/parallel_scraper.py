#!/usr/bin/env python3
"""
Parallel scraper and uploader for NY PUC cases.
Runs multiple scraper subprocesses and uploads successful results.
"""

import json
import subprocess
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool, Queue, Process, Manager
from queue import Empty
import time
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) and EOF (Ctrl+D) gracefully"""
    global shutdown_flag
    signal_name = 'SIGINT' if signum == signal.SIGINT else 'EOF'
    logger.info(f"\n{signal_name} received. Shutting down gracefully...")
    shutdown_flag = True

    # Force exit on second interrupt
    signal.signal(signal.SIGINT, signal.SIG_DFL)

def get_timestamp():
    """Generate timestamp in YYYYMMDD_HHMMSS format"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def scraper_worker(task):
    """
    Worker function to scrape a single case.

    Args:
        task: tuple of (govid, output_dir, retry_count, headed)

    Returns:
        dict with status, govid, output_file, error
    """
    govid, output_dir, retry_count, headed = task
    timestamp = get_timestamp()
    output_file = output_dir / f"{govid}-{timestamp}.json"

    # Build command
    cmd = [
        "npx", "tsx", "playwright/ny_puc_scraper.spec.ts",
        "--gov-ids", govid,
        "-o", str(output_file)
    ]

    if headed:
        cmd.append("--headed")

    logger.info(f"[{govid}] Starting scrape (attempt {retry_count + 1})")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout per case
        )

        # Check if output file was created and is valid
        if output_file.exists() and output_file.stat().st_size > 0:
            # Validate JSON
            try:
                with open(output_file, 'r') as f:
                    json.load(f)
                logger.info(f"[{govid}] ✓ Successfully scraped to {output_file.name}")
                return {
                    'status': 'success',
                    'govid': govid,
                    'output_file': str(output_file),
                    'error': None
                }
            except json.JSONDecodeError as e:
                logger.error(f"[{govid}] ✗ Invalid JSON output: {e}")
                return {
                    'status': 'failed',
                    'govid': govid,
                    'output_file': None,
                    'error': f"Invalid JSON: {e}"
                }
        else:
            error_msg = result.stderr if result.stderr else "No output file generated"
            logger.error(f"[{govid}] ✗ Scraping failed: {error_msg[:200]}")
            return {
                'status': 'failed',
                'govid': govid,
                'output_file': None,
                'error': error_msg
            }

    except subprocess.TimeoutExpired:
        logger.error(f"[{govid}] ✗ Timeout after 10 minutes")
        return {
            'status': 'failed',
            'govid': govid,
            'output_file': None,
            'error': 'Timeout'
        }
    except Exception as e:
        logger.error(f"[{govid}] ✗ Exception: {e}")
        return {
            'status': 'failed',
            'govid': govid,
            'output_file': None,
            'error': str(e)
        }

def uploader_worker(upload_queue, db_url):
    """
    Worker process that uploads completed scrapes to database.

    Args:
        upload_queue: Queue containing paths to JSON files to upload
        db_url: Database connection string
    """
    logger.info("[UPLOADER] Starting uploader process")

    while not shutdown_flag:
        try:
            # Wait for items with timeout to check shutdown flag
            json_file = upload_queue.get(timeout=1)

            if json_file is None:  # Poison pill
                logger.info("[UPLOADER] Received shutdown signal")
                break

            logger.info(f"[UPLOADER] Uploading {json_file}")

            try:
                cmd = ["python3", "upload_script.py", json_file]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout for upload
                )

                if result.returncode == 0:
                    logger.info(f"[UPLOADER] ✓ Successfully uploaded {Path(json_file).name}")
                else:
                    logger.error(f"[UPLOADER] ✗ Upload failed for {Path(json_file).name}: {result.stderr[:200]}")

            except subprocess.TimeoutExpired:
                logger.error(f"[UPLOADER] ✗ Upload timeout for {Path(json_file).name}")
            except Exception as e:
                logger.error(f"[UPLOADER] ✗ Upload exception for {Path(json_file).name}: {e}")

        except Empty:
            continue

    logger.info("[UPLOADER] Uploader process shutting down")

def load_cases(cases_file):
    """Load case govids from cases.json"""
    logger.info(f"Loading cases from {cases_file}")

    with open(cases_file, 'r') as f:
        cases = json.load(f)

    govids = [case['case_govid'] for case in cases]
    logger.info(f"Loaded {len(govids)} case govids")

    return govids

def save_state(state_file, state):
    """Save current processing state to JSON file"""
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"State saved to {state_file}")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_state(state_file):
    """Load previous processing state from JSON file"""
    if not Path(state_file).exists():
        logger.info("No previous state file found, starting fresh")
        return None

    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        logger.info(f"Loaded previous state from {state_file}")
        logger.info(f"  - Successful: {len(state.get('successful_cases', []))}")
        logger.info(f"  - Failed: {len(state.get('failed_cases', []))}")
        logger.info(f"  - Pending: {len(state.get('pending_govids', []))}")
        return state
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
        return None

def main():
    global shutdown_flag

    parser = argparse.ArgumentParser(description='Parallel NY PUC scraper and uploader')
    parser.add_argument('--cases-file', default='cases.json', help='Input JSON file with cases')
    parser.add_argument('--output-dir', default='output', help='Output directory for scraped JSON files')
    parser.add_argument('--state-file', default='scraper_state.json', help='State file to track progress')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel scraper workers')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts per case')
    parser.add_argument('--db-url', default='postgresql://postgres.oticlgskebuoswlbpucb:6kDStpZh0E6v07Uy@aws-0-us-east-2.pooler.supabase.com:5432/postgres',
                        help='Database URL for uploads')
    parser.add_argument('--skip-upload', action='store_true', help='Skip uploading (scrape only)')
    parser.add_argument('--reset', action='store_true', help='Ignore previous state and start fresh')

    args = parser.parse_args()

    # Setup signal handlers for Ctrl+C and Ctrl+D (EOF on stdin)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Load or initialize state
    previous_state = None if args.reset else load_state(args.state_file)

    # Load cases
    try:
        all_govids = load_cases(args.cases_file)
    except Exception as e:
        logger.error(f"Failed to load cases: {e}")
        sys.exit(1)

    # Initialize queues and state tracking with Manager
    manager = Manager()
    upload_queue = manager.Queue()

    # Initialize state from previous run or start fresh
    if previous_state:
        successful_cases = previous_state.get('successful_cases', [])
        failed_cases = previous_state.get('failed_cases', [])
        retry_counts = previous_state.get('retry_counts', {})

        # Determine which cases still need processing
        completed_or_failed = set(successful_cases) | set(failed_cases)
        pending_govids = [gid for gid in all_govids if gid not in completed_or_failed]

        logger.info(f"Resuming from previous state:")
        logger.info(f"  - Already successful: {len(successful_cases)}")
        logger.info(f"  - Already failed: {len(failed_cases)}")
        logger.info(f"  - Remaining to process: {len(pending_govids)}")
    else:
        successful_cases = []
        failed_cases = []
        retry_counts = {govid: 0 for govid in all_govids}
        pending_govids = all_govids.copy()

    # Create task queue (govid, output_dir, retry_count, headed)
    # All scrapes MUST run in headed mode (not headless)
    tasks = [(govid, output_dir, retry_counts.get(govid, 0), True) for govid in pending_govids]

    # Start uploader process
    uploader_process = None
    if not args.skip_upload:
        uploader_process = Process(target=uploader_worker, args=(upload_queue, args.db_url))
        uploader_process.start()
        logger.info("Uploader process started")

    logger.info(f"Starting {args.workers} scraper workers")
    logger.info(f"Processing {len(tasks)} total cases")

    # Process tasks with retry logic
    remaining_tasks = tasks.copy()

    pool = None
    try:
        # Create pool once for all tasks
        pool = Pool(processes=args.workers)

        while remaining_tasks and not shutdown_flag:
            current_batch = remaining_tasks.copy()
            remaining_tasks = []

            try:
                # Use imap_unordered to process results as they complete
                for result in pool.imap_unordered(scraper_worker, current_batch):
                    if shutdown_flag:
                        break

                    govid = result['govid']

                    if result['status'] == 'success':
                        successful_cases.append(govid)
                        # Immediately queue for upload
                        if not args.skip_upload:
                            upload_queue.put(result['output_file'])
                            logger.info(f"[{govid}] Queued for upload")

                    elif result['status'] == 'failed':
                        retry_counts[govid] += 1

                        if retry_counts[govid] < args.max_retries:
                            # Re-queue for retry
                            logger.warning(f"[{govid}] Retrying (attempt {retry_counts[govid] + 1}/{args.max_retries})")
                            remaining_tasks.append((govid, output_dir, retry_counts[govid], True))
                        else:
                            # Max retries exceeded
                            logger.error(f"[{govid}] Failed after {args.max_retries} attempts")
                            failed_cases.append(govid)

                    # Log progress after each result
                    total = len(all_govids)
                    success_count = len(successful_cases)
                    failed_count = len(failed_cases)
                    pending_count = len(remaining_tasks) + (len(current_batch) - success_count - failed_count)

                    logger.info(f"Progress: {success_count} success, {failed_count} failed, {pending_count} pending")

                    # Save state after each completed case
                    state = {
                        'successful_cases': successful_cases,
                        'failed_cases': failed_cases,
                        'retry_counts': retry_counts,
                        'pending_govids': [t[0] for t in remaining_tasks]
                    }
                    save_state(args.state_file, state)

            except KeyboardInterrupt:
                logger.info("Interrupting pool workers...")
                pool.terminate()
                pool.join()
                raise

        # Close the pool
        if pool:
            pool.close()
            pool.join()

        # Final summary
        logger.info("="*60)
        logger.info("SCRAPING COMPLETE")
        logger.info("="*60)
        logger.info(f"Total cases: {len(all_govids)}")
        logger.info(f"Successful: {len(successful_cases)}")
        logger.info(f"Failed: {len(failed_cases)}")

        if failed_cases:
            logger.info(f"\nFailed cases: {', '.join(failed_cases)}")

        # Save final state
        final_state = {
            'successful_cases': successful_cases,
            'failed_cases': failed_cases,
            'retry_counts': retry_counts,
            'pending_govids': []
        }
        save_state(args.state_file, final_state)

    except KeyboardInterrupt:
        logger.info("\nKeyboardInterrupt in main loop - saving state...")
        if pool:
            pool.terminate()
            pool.join()

        # Save state on interrupt
        interrupt_state = {
            'successful_cases': successful_cases,
            'failed_cases': failed_cases,
            'retry_counts': retry_counts,
            'pending_govids': [t[0] for t in remaining_tasks] if 'remaining_tasks' in locals() else []
        }
        save_state(args.state_file, interrupt_state)

    finally:
        # Shutdown uploader
        if uploader_process:
            logger.info("Waiting for uploader to finish...")
            upload_queue.put(None)  # Poison pill
            uploader_process.join(timeout=60)
            if uploader_process.is_alive():
                logger.warning("Uploader did not finish in time, terminating...")
                uploader_process.terminate()
                uploader_process.join()

        logger.info("Shutdown complete")

if __name__ == '__main__':
    main()
