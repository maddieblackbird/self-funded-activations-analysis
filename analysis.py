"""
Activation Performance Analysis Script
Analyzes promotional activation performance for the last two complete calendar weeks
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from collections import defaultdict

# ============================================================================
# CONFIGURATION & DATE SETUP
# ============================================================================

CURRENT_DATE = datetime.now()  # November 11, 2025

# Calculate last two complete calendar weeks (Monday-Sunday)
def get_last_complete_weeks(current_date):
    """Calculate the last two complete calendar weeks ending before current_date"""
    # Find the Monday of the current week
    days_since_monday = current_date.weekday()  # Monday is 0
    current_week_monday = current_date - timedelta(days=days_since_monday)
    
    # Last complete week ends on Sunday before current week's Monday
    last_week_end = current_week_monday - timedelta(days=1)  # Sunday
    last_week_start = last_week_end - timedelta(days=6)  # Monday
    
    # Second last complete week
    second_last_week_end = last_week_start - timedelta(days=1)  # Sunday
    second_last_week_start = second_last_week_end - timedelta(days=6)  # Monday
    
    return {
        'week1_start': second_last_week_start.replace(hour=0, minute=0, second=0),
        'week1_end': second_last_week_end.replace(hour=23, minute=59, second=59),
        'week2_start': last_week_start.replace(hour=0, minute=0, second=0),
        'week2_end': last_week_end.replace(hour=23, minute=59, second=59)
    }

weeks = get_last_complete_weeks(CURRENT_DATE)
ANALYSIS_START = weeks['week1_start']
ANALYSIS_END = weeks['week2_end']
WEEK1_START = weeks['week1_start']
WEEK1_END = weeks['week1_end']
WEEK2_START = weeks['week2_start']
WEEK2_END = weeks['week2_end']

print(f"Analysis Period:")
print(f"  Week 1: {weeks['week1_start'].strftime('%B %d, %Y')} - {weeks['week1_end'].strftime('%B %d, %Y')}")
print(f"  Week 2: {weeks['week2_start'].strftime('%B %d, %Y')} - {weeks['week2_end'].strftime('%B %d, %Y')}")
print()

# ============================================================================
# DATA LOADING & PARSING
# ============================================================================

def parse_transaction_date(date_str):
    """Parse transaction date format: 'November 17, 2025, 1:03 PM'"""
    try:
        return pd.to_datetime(date_str, format='%B %d, %Y, %I:%M %p')
    except:
        try:
            return pd.to_datetime(date_str)
        except:
            return pd.NaT

def parse_activation_date(date_str):
    """Parse activation date format: 'November 30, 2025, 17:30'"""
    try:
        return pd.to_datetime(date_str, format='%B %d, %Y, %H:%M')
    except:
        try:
            return pd.to_datetime(date_str)
        except:
            return pd.NaT

def parse_amount(amount_str):
    """Parse amount strings, handling $ and commas"""
    if pd.isna(amount_str) or amount_str == '':
        return 0.0
    if isinstance(amount_str, (int, float)):
        return float(amount_str)
    
    # Remove $ and commas
    amount_str = str(amount_str).replace('$', '').replace(',', '').strip()
    try:
        return float(amount_str)
    except:
        return 0.0

def parse_spend_description(description):
    """
    Parse 'Spend $X ... get $Y' descriptions
    Returns: (minimum_spend, reward_amount)
    """
    if pd.isna(description) or not isinstance(description, str):
        return None, None
    
    # Pattern: "Spend $X" ... "get $Y"
    spend_match = re.search(r'Spend\s+\$(\d+(?:\.\d+)?)', description, re.IGNORECASE)
    reward_match = re.search(r'get\s+\$(\d+(?:\.\d+)?)', description, re.IGNORECASE)
    
    minimum_spend = float(spend_match.group(1)) if spend_match else None
    reward_amount = float(reward_match.group(1)) if reward_match else None
    
    return minimum_spend, reward_amount

print("Loading data files...")

# Load transactions
transactions = pd.read_csv('all_transactions.csv', low_memory=False)
print(f"Loaded {len(transactions):,} transactions")

# Load activations (handle duplicate location_id column)
activations = pd.read_csv('all_activations.csv', low_memory=False)
print(f"Loaded {len(activations):,} activations")

# Parse dates
print("Parsing dates...")
transactions['created_at_dt'] = transactions['created_at_edt'].apply(parse_transaction_date)
activations['start_dt'] = activations['start_date'].apply(parse_activation_date)
activations['end_dt'] = activations['end_date'].apply(parse_activation_date)

# Parse amounts
transactions['amount'] = transactions['adj_amount'].apply(parse_amount)
transactions['refund_amount'] = transactions['adj_refund_amount'].apply(parse_amount)

# Parse activations initial budget
activations['initial_budget'] = activations['group_initial_budget'].apply(parse_amount)

# Parse activation descriptions
print("Parsing activation descriptions...")
activations[['minimum_spend', 'reward_amount']] = activations['description'].apply(
    lambda x: pd.Series(parse_spend_description(x))
)

# ============================================================================
# FILTER ACTIVATIONS
# ============================================================================

print("\nFiltering activations...")

# Filter 1: Only "Spend $" descriptions
spend_activations = activations[
    activations['description'].fillna('').str.startswith('Spend $', na=False)
].copy()
print(f"  Activations starting with 'Spend $': {len(spend_activations):,}")

# Filter 2: Only activations in analysis period
spend_activations = spend_activations[
    (spend_activations['start_dt'] >= ANALYSIS_START) & 
    (spend_activations['end_dt'] <= ANALYSIS_END)
].copy()
print(f"  Activations in analysis period: {len(spend_activations):,}")

# Validate parsed values
spend_activations = spend_activations[
    spend_activations['minimum_spend'].notna() & 
    spend_activations['reward_amount'].notna()
].copy()
print(f"  Activations with valid parsed values: {len(spend_activations):,}")

if len(spend_activations) == 0:
    print("\nWARNING: No qualifying activations found!")
    print("Creating empty output file...")
    empty_df = pd.DataFrame(columns=[
        'week', 'activation_id', 'restaurant_name', 'location_name', 'activation_description',
        'activation_start', 'activation_end', 'unique_users_count', 'total_tpv',
        'tpv_vs_baseline', 'median_check_vs_baseline', 'marketing_spend',
        'remaining_group_budget', 'new_users_count', 'returning_users_count',
        'new_user_percentage'
    ])
    empty_df.to_csv('activation_performance_analysis.csv', index=False)
    print("Empty output file created.")
    exit(0)

# ============================================================================
# CREATE MATCHING KEYS
# ============================================================================

print("\nCreating composite keys for matching...")

# Create composite keys on filtered activations
spend_activations['match_key'] = (
    spend_activations['restaurant_name'].fillna('').str.strip().str.lower() + '||' +
    spend_activations['location_name'].fillna('').str.strip().str.lower()
)

# Also create on full activations for baseline exclusion logic
activations['match_key'] = (
    activations['restaurant_name'].fillna('').str.strip().str.lower() + '||' +
    activations['location_name'].fillna('').str.strip().str.lower()
)

transactions['match_key'] = (
    transactions['rest_name'].fillna('').str.strip().str.lower() + '||' +
    transactions['location_name'].fillna('').str.strip().str.lower()
)

# ============================================================================
# BUILD BASELINE EXCLUSION PERIODS
# ============================================================================

print("\nBuilding activation exclusion map for baseline calculations...")

# For each restaurant_id, collect all activation periods
activation_periods = defaultdict(list)
for _, act in activations.iterrows():
    if pd.notna(act['restaurant_id']) and pd.notna(act['start_dt']) and pd.notna(act['end_dt']):
        activation_periods[act['restaurant_id']].append({
            'start': act['start_dt'],
            'end': act['end_dt']
        })

def is_in_activation_period(restaurant_id, check_start, check_end):
    """Check if a time period overlaps with any activation for this restaurant"""
    if restaurant_id not in activation_periods:
        return False
    
    for period in activation_periods[restaurant_id]:
        # Check for overlap
        if not (check_end < period['start'] or check_start > period['end']):
            return True
    return False

# ============================================================================
# CALCULATE USER HISTORY
# ============================================================================

print("\nCalculating user history for new/returning classification...")

# Sort transactions by date
transactions_sorted = transactions.sort_values('created_at_dt')

# For each restaurant, track first transaction date per user
user_first_transaction = {}

for _, txn in transactions_sorted.iterrows():
    if pd.isna(txn['created_at_dt']):
        continue
    
    rest_name = txn['rest_name']
    user_id = txn['user_id']
    txn_date = txn['created_at_dt']
    
    key = (rest_name, user_id)
    if key not in user_first_transaction:
        user_first_transaction[key] = txn_date

# ============================================================================
# ANALYZE EACH ACTIVATION
# ============================================================================

print("\nAnalyzing activations...")
results = []

for idx, activation in spend_activations.iterrows():
    if idx % 100 == 0:
        print(f"  Processing activation {idx}/{len(spend_activations)}...")
    
    # Extract activation details
    activation_id = activation['id']
    restaurant_name = activation['restaurant_name']
    location_name = activation['location_name']
    restaurant_id = activation['restaurant_id']
    restaurant_group_id = activation['restaurant_group_id']
    match_key = activation['match_key']
    start_dt = activation['start_dt']
    end_dt = activation['end_dt']
    minimum_spend = activation['minimum_spend']
    reward_amount = activation['reward_amount']
    description = activation['description']
    
    # Determine which week(s) this activation falls into
    weeks_to_analyze = []
    if start_dt <= WEEK1_END and end_dt >= WEEK1_START:
        weeks_to_analyze.append({
            'week_number': 1,
            'week_label': 'Week 1',
            'week_start': WEEK1_START,
            'week_end': WEEK1_END
        })
    if start_dt <= WEEK2_END and end_dt >= WEEK2_START:
        weeks_to_analyze.append({
            'week_number': 2,
            'week_label': 'Week 2',
            'week_start': WEEK2_START,
            'week_end': WEEK2_END
        })
    
    # Analyze performance for each week separately
    for week_info in weeks_to_analyze:
        week_label = week_info['week_label']
        week_start = week_info['week_start']
        week_end = week_info['week_end']
        
        # Get the effective date range for this activation within this week
        effective_start = max(start_dt, week_start)
        effective_end = min(end_dt, week_end)
        
        # Match transactions during activation period AND within this specific week
        matching_txns = transactions[
            (transactions['match_key'] == match_key) &
            (transactions['created_at_dt'] >= effective_start) &
            (transactions['created_at_dt'] <= effective_end)
        ].copy()
        
        # Calculate basic metrics
        unique_users = matching_txns['user_id'].nunique()
        total_tpv = matching_txns['amount'].sum()
        
        # Calculate median check
        if len(matching_txns) > 0:
            median_check = matching_txns['amount'].median()
        else:
            median_check = 0.0
        
        # Calculate marketing spend (transactions meeting minimum spend)
        qualifying_txns = matching_txns[matching_txns['amount'] >= minimum_spend]
        marketing_spend = len(qualifying_txns) * reward_amount
        
        # New vs Returning users
        new_users = 0
        returning_users = 0
        
        for user_id in matching_txns['user_id'].unique():
            key = (restaurant_name, user_id)
            if key in user_first_transaction:
                first_txn_date = user_first_transaction[key]
                if first_txn_date >= effective_start:
                    new_users += 1
                else:
                    returning_users += 1
            else:
                new_users += 1  # No prior history
        
        new_user_percentage = (new_users / unique_users * 100) if unique_users > 0 else 0.0
        
        # ====================================================================
        # BASELINE COMPARISON
        # ====================================================================
        
        # Get day-of-week and time window for activation
        activation_start_time = effective_start.time()
        activation_end_time = effective_end.time()
        activation_dow = effective_start.weekday()  # Monday=0
        
        # Calculate baseline from previous 4 weeks (same day-of-week)
        baseline_tpv_values = []
        baseline_check_values = []
        
        for week_offset in range(1, 5):  # Previous 4 weeks
            baseline_start = effective_start - timedelta(weeks=week_offset)
            baseline_end = effective_end - timedelta(weeks=week_offset)
            
            # Skip if this baseline period overlaps with any activation
            if is_in_activation_period(restaurant_id, baseline_start, baseline_end):
                continue
            
            # Get transactions in baseline period
            baseline_txns = transactions[
                (transactions['match_key'] == match_key) &
                (transactions['created_at_dt'] >= baseline_start) &
                (transactions['created_at_dt'] <= baseline_end)
            ]
            
            if len(baseline_txns) > 0:
                baseline_tpv_values.append(baseline_txns['amount'].sum())
                baseline_check_values.extend(baseline_txns['amount'].tolist())
        
        # Calculate baseline comparison
        if len(baseline_tpv_values) > 0:
            avg_baseline_tpv = np.mean(baseline_tpv_values)
            if avg_baseline_tpv > 0:
                tpv_vs_baseline = ((total_tpv - avg_baseline_tpv) / avg_baseline_tpv) * 100
            else:
                tpv_vs_baseline = 0.0 if total_tpv == 0 else 999.0  # Infinite growth
        else:
            tpv_vs_baseline = None  # No baseline data
        
        if len(baseline_check_values) > 0:
            baseline_median_check = np.median(baseline_check_values)
            if baseline_median_check > 0:
                median_check_vs_baseline = ((median_check - baseline_median_check) / baseline_median_check) * 100
            else:
                median_check_vs_baseline = 0.0 if median_check == 0 else 999.0
        else:
            median_check_vs_baseline = None
        
        # ====================================================================
        # CALCULATE REMAINING GROUP BUDGET
        # ====================================================================
        
        # Get all activations for this restaurant group
        group_activations = activations[
            activations['restaurant_group_id'] == restaurant_group_id
        ]
        
        # Get initial budget for this group
        initial_budget = activation['initial_budget']
        
        # Calculate total marketing spend for group (only rewards earned on/after Oct 13, 2025)
        oct_13_2025 = datetime(2025, 10, 13)
        total_group_spend = 0.0
        
        for _, group_act in group_activations.iterrows():
            if pd.isna(group_act['minimum_spend']) or pd.isna(group_act['reward_amount']):
                continue
            
            # Only count rewards earned on or after Oct 13, 2025
            act_start = group_act['start_dt']
            if pd.isna(act_start) or act_start < oct_13_2025:
                continue
            
            # Get transactions for this activation
            group_match_key = group_act['match_key']
            group_start = group_act['start_dt']
            group_end = group_act['end_dt']
            
            group_txns = transactions[
                (transactions['match_key'] == group_match_key) &
                (transactions['created_at_dt'] >= group_start) &
                (transactions['created_at_dt'] <= group_end) &
                (transactions['amount'] >= group_act['minimum_spend'])
            ]
            
            group_spend = len(group_txns) * group_act['reward_amount']
            total_group_spend += group_spend
        
        remaining_group_budget = initial_budget - total_group_spend if initial_budget > 0 else 0.0
        
        # ====================================================================
        # STORE RESULTS
        # ====================================================================
        
        results.append({
            'week': week_label,
            'activation_id': activation_id,
            'restaurant_name': restaurant_name,
            'location_name': location_name,
            'activation_description': description,
            'activation_start': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'activation_end': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'unique_users_count': unique_users,
            'total_tpv': round(total_tpv, 2),
            'tpv_vs_baseline': round(tpv_vs_baseline, 2) if tpv_vs_baseline is not None else None,
            'median_check_vs_baseline': round(median_check_vs_baseline, 2) if median_check_vs_baseline is not None else None,
            'marketing_spend': round(marketing_spend, 2),
            'remaining_group_budget': round(remaining_group_budget, 2),
            'new_users_count': new_users,
            'returning_users_count': returning_users,
            'new_user_percentage': round(new_user_percentage, 2)
        })

# ============================================================================
# OUTPUT RESULTS
# ============================================================================

print(f"\nCreating output file with {len(results)} activations...")

results_df = pd.DataFrame(results)
output_file = 'activation_performance_analysis.csv'
results_df.to_csv(output_file, index=False)

print(f"\n✓ Analysis complete!")
print(f"  Output saved to: {output_file}")
print(f"\nSummary Statistics:")
print(f"  Total activations analyzed: {len(results_df)}")
print(f"  Total marketing spend: ${results_df['marketing_spend'].sum():,.2f}")
print(f"  Average TPV per activation: ${results_df['total_tpv'].mean():,.2f}")
print(f"  Average users per activation: {results_df['unique_users_count'].mean():.1f}")
print(f"  Average new user percentage: {results_df['new_user_percentage'].mean():.1f}%")

