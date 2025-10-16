# Checkpoint/Resume System for NY PUC Scraper

## Overview

The NY PUC scraper now includes a checkpoint/resume system that automatically saves progress during long-running scrapes. This prevents data loss when the Node process runs out of heap memory or crashes unexpectedly.

## How It Works

### Automatic Checkpointing

When scraping 10+ items, the scraper automatically:
- Saves task queue state after each task completion
- Persists completed results to a buffer file
- Tracks which items are pending, in-progress, completed, or failed
- Saves checkpoints periodically (default: every 5% of tasks or minimum every 5 items)

### Files Created

All checkpoint files are stored in `playwright/raw/ny/`:

1. **checkpoint.json** - Complete checkpoint state including queue and results
2. **results_buffer.json** - Intermediate results buffer
3. **progress_state.json** - Additional progress tracking metadata

## Usage

### Basic Scraping (Auto-Checkpoint Enabled)

```bash
# Normal scraping - checkpoints enabled automatically for 10+ items
npx tsx ny_puc_scraper.spec.ts --mode all --gov-ids "12345,67890,11111,22222,..."
```

### Resume from Checkpoint

If the scraper crashes, resume from the last checkpoint:

```bash
# Resume from where you left off
npx tsx ny_puc_scraper.spec.ts --resume
```

The scraper will:
- Load the checkpoint file
- Skip completed items
- Retry any items that were in-progress when it crashed
- Continue processing pending items

### Clear Checkpoint

To start fresh and remove all checkpoint files:

```bash
# Clear checkpoint files
npx tsx ny_puc_scraper.spec.ts --clear-checkpoint
```

### Safety Warnings

If you try to start a new scrape while a checkpoint exists, you'll see:

```
⚠️  WARNING: Found existing checkpoint file!
   Use --resume to continue from the checkpoint, or
   Use --clear-checkpoint to start fresh.
```

## Checkpoint Frequency

The checkpoint frequency is automatically calculated based on batch size:

- **Small batches (< 10 items)**: No checkpointing (not needed)
- **Medium batches (10-100 items)**: Checkpoint every 5 items
- **Large batches (100+ items)**: Checkpoint every 5% of items

Example:
- 15 items → checkpoint every 5 items
- 100 items → checkpoint every 5 items
- 1000 items → checkpoint every 50 items

## Implementation Details

### Task States

Each task in the queue has one of these states:

- `pending` - Not yet started
- `in_progress` - Currently being processed
- `completed` - Successfully finished
- `failed` - Encountered an error

### What Gets Saved

The checkpoint includes:
- Complete task queue with status for each item
- All completed results
- Metadata (mode, total tasks, completion counts, timestamps)
- Error messages for failed tasks

### Recovery Behavior

On resume:
- **Completed tasks**: Skipped entirely (results loaded from checkpoint)
- **In-progress tasks**: Reset to pending and retried
- **Failed tasks**: Remain failed (can be manually retried if needed)
- **Pending tasks**: Processed normally

## Examples

### Example 1: Long-running scrape with checkpoint

```bash
# Start scraping 500 cases
npx tsx ny_puc_scraper.spec.ts --mode all --begin-date "01/01/2024" --end-date "12/31/2024" -o results.json

# After 100 items, Node crashes due to heap memory...
# Checkpoint saved at: playwright/raw/ny/checkpoint.json

# Resume from checkpoint
npx tsx ny_puc_scraper.spec.ts --resume

# Scraper continues from item 101, completing the remaining 400 items
```

### Example 2: Incremental scraping

```bash
# Run incremental scrape for today
npx tsx ny_puc_scraper.spec.ts --mode all --incremental-today

# If you need to clear and start over
npx tsx ny_puc_scraper.spec.ts --clear-checkpoint
```

### Example 3: Scraping from file with resume

```bash
# Create a file with gov IDs
echo "12345\n67890\n11111" > cases.txt

# Start scraping
npx tsx ny_puc_scraper.spec.ts --mode all --from-file cases.txt -o output.json

# If interrupted, resume
npx tsx ny_puc_scraper.spec.ts --resume
```

## Benefits

1. **No Data Loss**: Completed results are saved even if the process crashes
2. **Efficient Resume**: Skip already-processed items
3. **Progress Tracking**: See exactly how many items completed/failed
4. **Automatic**: Enabled by default for large batches
5. **Memory Safe**: Checkpoint files allow process restart without losing work

## Console Output

The scraper provides clear feedback about checkpoint operations:

```
💾 Checkpoint mode enabled (saving every 5 items)
Starting task 1/100...
💾 Checkpoint saved: 5/100 tasks completed
💾 Checkpoint saved: 10/100 tasks completed
...
✅ All tasks completed, clearing checkpoint...
```

On resume:

```
📂 Loaded checkpoint from 2025-10-12T12:30:00.000Z
   - Progress: 45/100 completed, 2 failed
   - Pending tasks: 53
🔄 Resuming from checkpoint...
Starting task 46/100...
```

## Troubleshooting

### Checkpoint file corrupted

```bash
# Remove corrupted checkpoint and start fresh
npx tsx ny_puc_scraper.spec.ts --clear-checkpoint
```

### Want to see checkpoint contents

```bash
# View checkpoint JSON
cat playwright/raw/ny/checkpoint.json | jq '.'
```

### Different mode on resume

The checkpoint stores the scraping mode. If you need to change mode, clear the checkpoint first:

```bash
npx tsx ny_puc_scraper.spec.ts --clear-checkpoint
npx tsx ny_puc_scraper.spec.ts --mode filing --gov-ids "..."
```
