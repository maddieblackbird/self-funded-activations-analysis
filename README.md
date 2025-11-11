# Activation Performance Analysis

This script analyzes promotional activation performance for "Spend $X get $Y" offers during the last two complete calendar weeks.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Ensure the following files are in the same directory as `analysis.py`:
- `all_transactions.csv` -- comes from here: https://blackbird-labs.metabaseapp.com/question/13934-full-history-of-payments-at-this-restaurant?restaurant_name=&market=
- `all_activations.csv`: comes from here: https://blackbird-labs.metabaseapp.com/question/19603-all-activations-paired-with-restaurant-group

Then run:

```bash
python3 analysis.py
```

## Output

The script generates `activation_performance_analysis.csv` with the following columns:

**Note:** Each activation creates separate rows for Week 1 and Week 2, with metrics calculated independently for each week. If an activation spans both weeks, it will appear twice with different performance metrics.

| Column | Description |
|--------|-------------|
| `week` | Which analysis week (Week 1 or Week 2) |
| `activation_id` | Unique activation identifier |
| `restaurant_name` | Restaurant name |
| `location_name` | Location name |
| `activation_description` | Full promotion description |
| `activation_start` | Start date/time |
| `activation_end` | End date/time |
| `unique_users_count` | Number of unique users who transacted |
| `total_tpv` | Total payment volume during activation |
| `tpv_vs_baseline` | TPV change vs baseline (% or null if no baseline) |
| `median_check_vs_baseline` | Median check change vs baseline (%) |
| `marketing_spend` | Total rewards earned (qualifying transactions × reward amount) |
| `remaining_group_budget` | Remaining budget for restaurant group |
| `new_users_count` | Users with no prior transactions at restaurant |
| `returning_users_count` | Users with prior transactions |
| `new_user_percentage` | Percentage of users who are new |

## Analysis Details

### Date Range
- Analyzes the last two complete calendar weeks (Monday-Sunday)
- Based on current date of November 11, 2025:
  - Week 1: October 27 - November 2, 2025
  - Week 2: November 3 - 9, 2025
- **Performance is tracked separately for each week**
  - Each activation gets a row for Week 1 and/or Week 2
  - Metrics (TPV, users, etc.) only count transactions within that specific week
  - If an activation spans both weeks, it appears twice with different metrics

### Baseline Comparison
- Compares performance to same day-of-week and time windows from previous 4 weeks
- Excludes any baseline periods that overlap with activations for the same restaurant
- Returns null if no valid baseline data is available

### Marketing Spend Calculation
- Only counts transactions that meet the minimum spend threshold
- Multiplies qualifying transaction count by reward amount

### Group Budget Tracking
- Tracks all spending for restaurant groups since October 13, 2025
- Calculates remaining budget: initial_budget - total_group_spend

### New vs Returning Users
- "New" users have no prior transactions at the restaurant before the activation
- "Returning" users have at least one prior transaction

## Data Requirements

### all_transactions.csv
Must contain: `created_at_edt`, `user_id`, `adj_amount`, `rest_name`, `location_name`

### all_activations.csv
Must contain: `id`, `restaurant_id`, `restaurant_group_id`, `restaurant_name`, `location_name`, `description`, `start_date`, `end_date`, `group_initial_budget`

## Performance

- Processes 300,000+ transactions and 23,000+ activations
- Typical run time: 30-60 seconds
- Output: ~250 rows (248 unique activations broken out by week)
  - Week 1: ~113 activation-week combinations
  - Week 2: ~137 activation-week combinations
  - A small number of activations may appear in both weeks if they span the week boundary

