"""
Activation Performance Analysis Script
Analyzes promotional activation performance for the last two complete calendar weeks

FIXED:
1. 1-hour window now includes transactions at exactly 1 hour (changed < to <=)
2. New users are ONLY those who have NEVER transacted at this restaurant/location before
3. Uses Claude API to parse activation descriptions when regex fails
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from collections import defaultdict
import os
from anthropic import Anthropic

# ============================================================================
# CONFIGURATION & DATE SETUP
# ============================================================================

CURRENT_DATE = datetime.now()  

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

def parse_spend_description_with_claude(description, claude_client):
    """
    Use Claude to parse activation description
    Returns: (minimum_spend, reward_amount)
    """
    if not claude_client:
        return None, None
    
    try:
        prompt = f"""Parse this restaurant promotion description and extract the spend threshold and reward amount.

Description: "{description}"

Respond with ONLY two numbers separated by a comma:
- First number: the minimum spend amount (just the number, no $ symbol)
- Second number: the reward amount (just the number, no $ symbol)

Example response: 25,10
(meaning: spend $25, get $10 reward)

Your response:"""

        message = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=50,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response = message.content[0].text.strip()
        
        # Parse the response
        parts = response.split(',')
        if len(parts) == 2:
            minimum_spend = float(parts[0].strip())
            reward_amount = float(parts[1].strip())
            return minimum_spend, reward_amount
        
    except Exception as e:
        print(f"  ⚠ Claude parsing error for '{description}': {str(e)}")
    
    return None, None

def parse_spend_description(description, claude_client=None):
    """
    Parse 'Spend $X ... get/receive $Y' descriptions
    First tries regex, then falls back to Claude if needed
    Returns: (minimum_spend, reward_amount)
    """
    if pd.isna(description) or not isinstance(description, str):
        return None, None
    
    # Try regex first (fast)
    spend_match = re.search(r'Spend\s+\$(\d+(?:\.\d+)?)', description, re.IGNORECASE)
    reward_match = re.search(r'(?:get|receive|earn)\s+\$(\d+(?:\.\d+)?)', description, re.IGNORECASE)
    
    minimum_spend = float(spend_match.group(1)) if spend_match else None
    reward_amount = float(reward_match.group(1)) if reward_match else None
    
    # If regex worked, return
    if minimum_spend is not None and reward_amount is not None:
        return minimum_spend, reward_amount
    
    # If regex failed and we have Claude, use Claude
    if claude_client:
        return parse_spend_description_with_claude(description, claude_client)
    
    return minimum_spend, reward_amount

print("Loading data files...")

# Initialize Claude client for parsing
claude_client = None
api_key = os.environ.get('ANTHROPIC_API_KEY')
if api_key:
    claude_client = Anthropic(api_key=api_key)
    print("✓ Claude API initialized for description parsing")
else:
    print("⚠ ANTHROPIC_API_KEY not found - will use regex only for parsing")
    print("  Set it with: export ANTHROPIC_API_KEY='your-key-here'")

# Load transactions
transactions = pd.read_csv('all_transactions.csv', low_memory=False)
print(f"Loaded {len(transactions):,} transactions")

# Load activations
activations = pd.read_csv('all_activations.csv', low_memory=False)
print(f"Loaded {len(activations):,} activations")

# Parse dates
print("Parsing dates...")
transactions['created_at_dt'] = transactions['created_at_edt'].apply(parse_transaction_date)
activations['start_dt'] = activations['start_date'].apply(parse_activation_date)
activations['end_dt'] = activations['end_date'].apply(parse_activation_date)

# Parse amounts
transactions['amount'] = transactions['adj_amount'].apply(parse_amount)

# Parse activations initial budget
activations['initial_budget'] = activations['group_initial_budget'].apply(parse_amount)

# Parse activation descriptions
print("Parsing activation descriptions...")

# First pass: Try regex on all
activations[['minimum_spend', 'reward_amount']] = activations['description'].apply(
    lambda x: pd.Series(parse_spend_description(x, None))
)

# Second pass: Use Claude for failed parses (if available)
if claude_client:
    failed_parses = activations[
        (activations['description'].fillna('').str.startswith('Spend $', na=False)) &
        (activations['minimum_spend'].isna() | activations['reward_amount'].isna())
    ]
    
    if len(failed_parses) > 0:
        print(f"  ⚙ Using Claude to parse {len(failed_parses)} descriptions that regex couldn't handle...")
        
        for idx, row in failed_parses.iterrows():
            min_spend, reward = parse_spend_description(row['description'], claude_client)
            if min_spend is not None and reward is not None:
                activations.at[idx, 'minimum_spend'] = min_spend
                activations.at[idx, 'reward_amount'] = reward
                print(f"    ✓ Parsed: '{row['description'][:60]}...' → ${min_spend} / ${reward}")
            else:
                print(f"    ✗ Failed: '{row['description'][:60]}...'")
        
        remaining_failed = activations[
            (activations['description'].fillna('').str.startswith('Spend $', na=False)) &
            (activations['minimum_spend'].isna() | activations['reward_amount'].isna())
        ]
        print(f"  Final: {len(remaining_failed)} descriptions could not be parsed")

# ============================================================================
# FILTER ACTIVATIONS
# ============================================================================

print("\nFiltering activations...")

# Filter 1: Only "Spend $" descriptions
spend_activations = activations[
    activations['description'].fillna('').str.startswith('Spend $', na=False)
].copy()
print(f"  Activations starting with 'Spend $': {len(spend_activations):,}")

# Filter 2: Only activations that overlap with analysis period (using overlap logic)
spend_activations = spend_activations[
    (spend_activations['start_dt'] <= ANALYSIS_END) & 
    (spend_activations['end_dt'] >= ANALYSIS_START)
].copy()
print(f"  Activations overlapping with analysis period: {len(spend_activations):,}")

# Validate parsed values
spend_activations = spend_activations[
    spend_activations['minimum_spend'].notna() & 
    spend_activations['reward_amount'].notna()
].copy()
print(f"  Activations with valid parsed values: {len(spend_activations):,}")

if len(spend_activations) == 0:
    print("\nWARNING: No qualifying activations found!")
    print("Creating empty output files...")
    
    # Empty weekly file
    weekly_empty_df = pd.DataFrame(columns=[
        'week', 'activation_id', 'restaurant_name', 'location_name', 'activation_description',
        'minimum_spend_threshold', 'reward_amount', 'activation_start', 'activation_end',
        'unique_users_count', 'unique_users_count_REDEEMED', 'total_tpv', 'tpv_vs_baseline',
        'median_check_vs_baseline', 'marketing_spend', 'remaining_group_budget', 'new_users_count',
        'returning_users_count', 'new_user_percentage', 'notes'
    ])
    weekly_empty_df.to_csv('activation_performance_analysis_weekly.csv', index=False)
    
    # Empty daily file
    daily_empty_df = pd.DataFrame(columns=[
        'date', 'day_of_week', 'activation_id', 'restaurant_name', 'location_name', 
        'activation_description', 'minimum_spend_threshold', 'reward_amount', 'activation_start',
        'activation_end', 'unique_users_count', 'unique_users_count_REDEEMED', 'total_tpv',
        'tpv_vs_baseline', 'median_check_vs_baseline', 'marketing_spend', 'remaining_group_budget',
        'new_users_count', 'returning_users_count', 'new_user_percentage', 'notes'
    ])
    daily_empty_df.to_csv('activation_performance_analysis_daily.csv', index=False)
    
    print("Empty output files created.")
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

# For each match_key (restaurant_name + location_name), collect all activation periods
activation_periods = defaultdict(list)
for _, act in activations.iterrows():
    if pd.notna(act['match_key']) and pd.notna(act['start_dt']) and pd.notna(act['end_dt']):
        activation_periods[act['match_key']].append({
            'start': act['start_dt'],
            'end': act['end_dt']
        })

def is_in_activation_period(match_key, check_start, check_end):
    """Check if a time period overlaps with any activation for this restaurant location"""
    if match_key not in activation_periods:
        return False
    
    for period in activation_periods[match_key]:
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

# For each restaurant (using match_key), track first transaction date per user
user_first_transaction = {}

for _, txn in transactions_sorted.iterrows():
    if pd.isna(txn['created_at_dt']):
        continue
    
    match_key = txn['match_key']
    user_id = txn['user_id']
    txn_date = txn['created_at_dt']
    
    key = (match_key, user_id)
    if key not in user_first_transaction:
        user_first_transaction[key] = txn_date

# ============================================================================
# ANALYZE EACH ACTIVATION
# ============================================================================

print("\nAnalyzing activations...")

# First, create unique groupings by (restaurant_name, location_name, minimum_spend, reward_amount)
spend_activations['grouping_key'] = (
    spend_activations['restaurant_name'].fillna('').astype(str) + '||' +
    spend_activations['location_name'].fillna('').astype(str) + '||' +
    spend_activations['minimum_spend'].astype(str) + '||' +
    spend_activations['reward_amount'].astype(str)
)

unique_groupings = spend_activations.groupby('grouping_key')

weekly_results = []
daily_results = []

print(f"Found {len(unique_groupings)} unique restaurant/location/offer groupings")

for grouping_idx, (grouping_key, group_activations) in enumerate(unique_groupings):
    if grouping_idx % 100 == 0:
        print(f"  Processing grouping {grouping_idx}/{len(unique_groupings)}...")
    
    # Extract common details from the group (use first activation as reference)
    first_activation = group_activations.iloc[0]
    restaurant_name = first_activation['restaurant_name']
    location_name = first_activation['location_name']
    restaurant_id = first_activation['restaurant_id']
    restaurant_group_id = first_activation['restaurant_group_id']
    match_key = first_activation['match_key']
    minimum_spend = first_activation['minimum_spend']
    reward_amount = first_activation['reward_amount']
    initial_budget = first_activation['initial_budget']
    
    # Combine all unique descriptions (in case they're phrased differently)
    unique_descriptions = group_activations['description'].unique()
    if len(unique_descriptions) == 1:
        description = unique_descriptions[0]
    else:
        # Use the shortest description (usually the most concise)
        description = min(unique_descriptions, key=len)
    
    # Collect all individual activation periods (not continuous range!)
    activation_periods_list = []
    for _, act in group_activations.iterrows():
        activation_periods_list.append({
            'start': act['start_dt'],
            'end': act['end_dt'],
            'id': act['id']
        })
    
    # Get the overall date range for display purposes only
    overall_start_dt = group_activations['start_dt'].min()
    overall_end_dt = group_activations['end_dt'].max()
    
    # Collect all activation IDs in this group
    activation_ids = group_activations['id'].tolist()
    activation_id_display = ', '.join(map(str, activation_ids))
    
    # Determine which week(s) this grouping overlaps with
    weeks_to_analyze = []
    if overall_start_dt <= WEEK1_END and overall_end_dt >= WEEK1_START:
        weeks_to_analyze.append({
            'week_number': 1,
            'week_label': 'Week 1',
            'week_start': WEEK1_START,
            'week_end': WEEK1_END
        })
    if overall_start_dt <= WEEK2_END and overall_end_dt >= WEEK2_START:
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
        
        # Get transactions during ANY of the activation periods within this week
        matching_txns = pd.DataFrame()
        
        for period in activation_periods_list:
            period_start = period['start']
            period_end = period['end']
            
            # Only consider the intersection with this week
            effective_period_start = max(period_start, week_start)
            effective_period_end = min(period_end, week_end)
            
            # Skip if this period doesn't overlap with this week
            if effective_period_start > effective_period_end:
                continue
            
            # Get transactions during this specific activation window
            period_txns = transactions[
                (transactions['match_key'] == match_key) &
                (transactions['created_at_dt'] >= effective_period_start) &
                (transactions['created_at_dt'] <= effective_period_end)
            ]
            
            matching_txns = pd.concat([matching_txns, period_txns], ignore_index=True)
        
        # Remove duplicate transactions (in case periods overlap)
        matching_txns = matching_txns.drop_duplicates().copy()
        
        # Check if there are zero transactions
        has_transactions = len(matching_txns) > 0
        notes = "" if has_transactions else "No transactions during activation period"
        
        # Calculate basic metrics
        unique_users = matching_txns['user_id'].nunique() if has_transactions else 0
        total_tpv = matching_txns['amount'].sum() if has_transactions else 0.0
        
        # Calculate median check
        if has_transactions:
            median_check = matching_txns['amount'].median()
        else:
            median_check = None
        
        # FIXED: Calculate marketing spend (group transactions by user within 1-hour windows)
        if has_transactions:
            qualifying_redemptions = 0
            redeemed_users = set()
            
            for user_id in matching_txns['user_id'].unique():
                user_txns = matching_txns[matching_txns['user_id'] == user_id].sort_values('created_at_dt')
                
                # Track which transactions have been used in a qualifying group
                used_indices = set()
                
                for idx, txn in user_txns.iterrows():
                    if idx in used_indices:
                        continue
                    
                    # FIXED: Window is 1 hour from THIS (first) transaction
                    window_start = txn['created_at_dt']
                    window_end = window_start + timedelta(hours=1)
                    
                    # Get ALL subsequent transactions within 1 hour of this FIRST transaction
                    window_txns = user_txns[
                        (user_txns['created_at_dt'] >= window_start) &
                        (user_txns['created_at_dt'] <= window_end) &  # FIXED: Changed < to <= to include exactly 1 hour
                        (~user_txns.index.isin(used_indices))
                    ]
                    
                    window_total = window_txns['amount'].sum()
                    
                    # If this window meets the minimum spend, count as a redemption
                    if window_total >= minimum_spend:
                        qualifying_redemptions += 1
                        redeemed_users.add(user_id)
                        used_indices.update(window_txns.index.tolist())
            
            marketing_spend = qualifying_redemptions * reward_amount
            unique_users_redeemed = len(redeemed_users)
        else:
            marketing_spend = 0.0
            unique_users_redeemed = 0
        
        # FIXED: New vs Returning users
        new_users = 0
        returning_users = 0
        
        if has_transactions:
            for user_id in matching_txns['user_id'].unique():
                key = (match_key, user_id)
                
                if key in user_first_transaction:
                    first_txn_date = user_first_transaction[key]
                    
                    # User is "new" if their FIRST EVER transaction at this location
                    # happened during ANY of the activation periods (not continuous range)
                    is_new = False
                    for period in activation_periods_list:
                        if first_txn_date >= period['start'] and first_txn_date <= period['end']:
                            is_new = True
                            break
                    
                    if is_new:
                        new_users += 1
                    else:
                        returning_users += 1
                else:
                    # Should not happen, but if it does, count as new
                    new_users += 1
        
        new_user_percentage = (new_users / unique_users * 100) if unique_users > 0 else None
        
        # ====================================================================
        # BASELINE COMPARISON
        # ====================================================================
        
        tpv_vs_baseline = None
        median_check_vs_baseline = None
        
        if has_transactions:
            # Calculate baseline from previous 4 weeks (same days in prior weeks)
            baseline_tpv_values = []
            baseline_median_values = []  # Store median from each baseline week
            
            for week_offset in range(1, 5):  # Previous 4 weeks
                baseline_start = week_start - timedelta(weeks=week_offset)
                baseline_end = week_end - timedelta(weeks=week_offset)
                
                # Skip if this baseline period overlaps with any activation
                if is_in_activation_period(match_key, baseline_start, baseline_end):
                    continue
                
                # Get transactions in baseline period
                baseline_txns = transactions[
                    (transactions['match_key'] == match_key) &
                    (transactions['created_at_dt'] >= baseline_start) &
                    (transactions['created_at_dt'] <= baseline_end)
                ]
                
                if len(baseline_txns) > 0:
                    baseline_tpv_values.append(baseline_txns['amount'].sum())
                    baseline_median_values.append(baseline_txns['amount'].median())
            
            # Calculate baseline comparison
            if len(baseline_tpv_values) > 0:
                avg_baseline_tpv = np.mean(baseline_tpv_values)
                if avg_baseline_tpv > 0:
                    tpv_vs_baseline = ((total_tpv - avg_baseline_tpv) / avg_baseline_tpv) * 100
                else:
                    tpv_vs_baseline = 0.0 if total_tpv == 0 else 999.0  # Infinite growth
            
            if len(baseline_median_values) > 0:
                baseline_median_check = np.median(baseline_median_values)
                if baseline_median_check > 0:
                    median_check_vs_baseline = ((median_check - baseline_median_check) / baseline_median_check) * 100
                else:
                    median_check_vs_baseline = 0.0 if median_check == 0 else 999.0
        
        # ====================================================================
        # CALCULATE REMAINING GROUP BUDGET
        # ====================================================================
        
        # Get all activations for this restaurant group
        group_activations_all = activations[
            activations['restaurant_group_id'] == restaurant_group_id
        ]
        
        # Initial budget was already extracted earlier from first_activation
        
        # Calculate total marketing spend for group (only rewards earned on/after specific date)
        # Special case: The Bar at The Spectator starts on Oct 26, 2025
        # All others start on Oct 13, 2025
        if restaurant_name == "The Bar at The Spectator":
            budget_start_date = datetime(2025, 10, 26)
        else:
            budget_start_date = datetime(2025, 10, 13)
        
        total_group_spend = 0.0
        
        for _, group_act in group_activations_all.iterrows():
            if pd.isna(group_act['minimum_spend']) or pd.isna(group_act['reward_amount']):
                continue
            
            # Only count rewards earned on or after the budget start date
            act_start = group_act['start_dt']
            if pd.isna(act_start) or act_start < budget_start_date:
                continue
            
            # Get transactions for this activation
            group_match_key = group_act['match_key']
            group_start = group_act['start_dt']
            group_end = group_act['end_dt']
            group_min_spend = group_act['minimum_spend']
            group_reward = group_act['reward_amount']
            
            group_txns = transactions[
                (transactions['match_key'] == group_match_key) &
                (transactions['created_at_dt'] >= group_start) &
                (transactions['created_at_dt'] <= group_end)
            ]
            
            # Calculate qualifying redemptions using 1-hour window logic
            if len(group_txns) > 0:
                group_qualifying_redemptions = 0
                
                for user_id in group_txns['user_id'].unique():
                    user_group_txns = group_txns[group_txns['user_id'] == user_id].sort_values('created_at_dt')
                    used_indices = set()
                    
                    for idx, txn in user_group_txns.iterrows():
                        if idx in used_indices:
                            continue
                        
                        window_start = txn['created_at_dt']
                        window_end = window_start + timedelta(hours=1)
                        
                        window_txns = user_group_txns[
                            (user_group_txns['created_at_dt'] >= window_start) &
                            (user_group_txns['created_at_dt'] <= window_end) &  # FIXED: <= instead of <
                            (~user_group_txns.index.isin(used_indices))
                        ]
                        
                        window_total = window_txns['amount'].sum()
                        
                        if window_total >= group_min_spend:
                            group_qualifying_redemptions += 1
                            used_indices.update(window_txns.index.tolist())
                
                group_spend = group_qualifying_redemptions * group_reward
                total_group_spend += group_spend
        
        remaining_group_budget = initial_budget - total_group_spend if initial_budget > 0 else 0.0
        
        # ====================================================================
        # STORE WEEKLY RESULTS
        # ====================================================================
        
        weekly_results.append({
            'week': week_label,
            'activation_id': activation_id_display,
            'restaurant_name': restaurant_name,
            'location_name': location_name,
            'activation_description': description,
            'minimum_spend_threshold': minimum_spend,
            'reward_amount': reward_amount,
            'activation_start': overall_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'activation_end': overall_end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'unique_users_count': unique_users,
            'unique_users_count_REDEEMED': unique_users_redeemed,
            'total_tpv': round(total_tpv, 2) if has_transactions else 0.0,
            'tpv_vs_baseline': round(tpv_vs_baseline, 2) if tpv_vs_baseline is not None else None,
            'median_check_vs_baseline': round(median_check_vs_baseline, 2) if median_check_vs_baseline is not None else None,
            'marketing_spend': round(marketing_spend, 2),
            'remaining_group_budget': round(remaining_group_budget, 2),
            'new_users_count': new_users,
            'returning_users_count': returning_users,
            'new_user_percentage': round(new_user_percentage, 2) if new_user_percentage is not None else None,
            'notes': notes
        })
    
    # ========================================================================
    # DAILY ANALYSIS
    # ========================================================================
    
    # Collect all unique days when activations were active
    active_days = set()
    for period in activation_periods_list:
        current = period['start'].replace(hour=0, minute=0, second=0, microsecond=0)
        end = period['end'].replace(hour=0, minute=0, second=0, microsecond=0)
        while current <= end:
            active_days.add(current)
            current += timedelta(days=1)
    
    # Analyze each active day
    for current_day in sorted(active_days):
        day_start = current_day
        day_end = current_day.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Only analyze days within the analysis period
        if day_end < ANALYSIS_START or day_start > ANALYSIS_END:
            continue
        
        # Get transactions during ANY activation period on this day
        daily_txns = pd.DataFrame()
        
        for period in activation_periods_list:
            period_start = period['start']
            period_end = period['end']
            
            # Only consider the intersection with this day
            effective_day_start = max(day_start, period_start, ANALYSIS_START)
            effective_day_end = min(day_end, period_end, ANALYSIS_END)
            
            # Skip if this period doesn't overlap with this day
            if effective_day_start > effective_day_end:
                continue
            
            # Get transactions during this specific activation window on this day
            period_daily_txns = transactions[
                (transactions['match_key'] == match_key) &
                (transactions['created_at_dt'] >= effective_day_start) &
                (transactions['created_at_dt'] <= effective_day_end)
            ]
            
            daily_txns = pd.concat([daily_txns, period_daily_txns], ignore_index=True)
        
        # Remove duplicate transactions (in case periods overlap)
        daily_txns = daily_txns.drop_duplicates().copy()
        
        # Check if there are zero transactions for this day
        has_daily_transactions = len(daily_txns) > 0
        daily_notes = "" if has_daily_transactions else "No transactions during activation period"
        
        # Calculate daily metrics
        daily_unique_users = daily_txns['user_id'].nunique() if has_daily_transactions else 0
        daily_total_tpv = daily_txns['amount'].sum() if has_daily_transactions else 0.0
        
        if has_daily_transactions:
            daily_median_check = daily_txns['amount'].median()
        else:
            daily_median_check = None
        
        # FIXED: Calculate daily marketing spend (group transactions by user within 1-hour windows)
        if has_daily_transactions:
            daily_qualifying_redemptions = 0
            daily_redeemed_users = set()
            
            for user_id in daily_txns['user_id'].unique():
                user_daily_txns = daily_txns[daily_txns['user_id'] == user_id].sort_values('created_at_dt')
                
                # Track which transactions have been used in a qualifying group
                used_indices = set()
                
                for idx, txn in user_daily_txns.iterrows():
                    if idx in used_indices:
                        continue
                    
                    # Get transactions within 1 hour window
                    window_start = txn['created_at_dt']
                    window_end = window_start + timedelta(hours=1)
                    
                    window_txns = user_daily_txns[
                        (user_daily_txns['created_at_dt'] >= window_start) &
                        (user_daily_txns['created_at_dt'] <= window_end) &  # FIXED: <= instead of <
                        (~user_daily_txns.index.isin(used_indices))
                    ]
                    
                    window_total = window_txns['amount'].sum()
                    
                    # If this window meets the minimum spend, count as a redemption
                    if window_total >= minimum_spend:
                        daily_qualifying_redemptions += 1
                        daily_redeemed_users.add(user_id)
                        used_indices.update(window_txns.index.tolist())
            
            daily_marketing_spend = daily_qualifying_redemptions * reward_amount
            daily_unique_users_redeemed = len(daily_redeemed_users)
        else:
            daily_marketing_spend = 0.0
            daily_unique_users_redeemed = 0
        
        # FIXED: New vs Returning users for this day
        daily_new_users = 0
        daily_returning_users = 0
        
        if has_daily_transactions:
            for user_id in daily_txns['user_id'].unique():
                key = (match_key, user_id)
                
                if key in user_first_transaction:
                    first_txn_date = user_first_transaction[key]
                    
                    # User is "new" if their FIRST EVER transaction at this location
                    # happened during ANY of the activation periods (not continuous range)
                    is_new = False
                    for period in activation_periods_list:
                        if first_txn_date >= period['start'] and first_txn_date <= period['end']:
                            is_new = True
                            break
                    
                    if is_new:
                        daily_new_users += 1
                    else:
                        daily_returning_users += 1
                else:
                    # Should not happen, but if it does, count as new
                    daily_new_users += 1
        
        daily_new_user_percentage = (daily_new_users / daily_unique_users * 100) if daily_unique_users > 0 else None
        
        # Daily baseline comparison (same day-of-week, previous 4 weeks)
        daily_tpv_vs_baseline = None
        daily_median_check_vs_baseline = None
        
        if has_daily_transactions:
            daily_baseline_tpv_values = []
            daily_baseline_median_values = []
            
            for week_offset in range(1, 5):
                baseline_day_start = effective_day_start - timedelta(weeks=week_offset)
                baseline_day_end = effective_day_end - timedelta(weeks=week_offset)
                
                # Skip if baseline period overlaps with any activation
                if is_in_activation_period(match_key, baseline_day_start, baseline_day_end):
                    continue
                
                baseline_day_txns = transactions[
                    (transactions['match_key'] == match_key) &
                    (transactions['created_at_dt'] >= baseline_day_start) &
                    (transactions['created_at_dt'] <= baseline_day_end)
                ]
                
                if len(baseline_day_txns) > 0:
                    daily_baseline_tpv_values.append(baseline_day_txns['amount'].sum())
                    daily_baseline_median_values.append(baseline_day_txns['amount'].median())
            
            # Calculate daily baseline comparison
            if len(daily_baseline_tpv_values) > 0:
                avg_daily_baseline_tpv = np.mean(daily_baseline_tpv_values)
                if avg_daily_baseline_tpv > 0:
                    daily_tpv_vs_baseline = ((daily_total_tpv - avg_daily_baseline_tpv) / avg_daily_baseline_tpv) * 100
                else:
                    daily_tpv_vs_baseline = 0.0 if daily_total_tpv == 0 else 999.0
            
            if len(daily_baseline_median_values) > 0:
                daily_baseline_median_check = np.median(daily_baseline_median_values)
                if daily_baseline_median_check > 0:
                    daily_median_check_vs_baseline = ((daily_median_check - daily_baseline_median_check) / daily_baseline_median_check) * 100
                else:
                    daily_median_check_vs_baseline = 0.0 if daily_median_check == 0 else 999.0
        
        # Store daily results
        daily_results.append({
            'date': current_day.strftime('%Y-%m-%d'),
            'day_of_week': current_day.strftime('%A'),
            'activation_id': activation_id_display,
            'restaurant_name': restaurant_name,
            'location_name': location_name,
            'activation_description': description,
            'minimum_spend_threshold': minimum_spend,
            'reward_amount': reward_amount,
            'activation_start': overall_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'activation_end': overall_end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'unique_users_count': daily_unique_users,
            'unique_users_count_REDEEMED': daily_unique_users_redeemed,
            'total_tpv': round(daily_total_tpv, 2) if has_daily_transactions else 0.0,
            'tpv_vs_baseline': round(daily_tpv_vs_baseline, 2) if daily_tpv_vs_baseline is not None else None,
            'median_check_vs_baseline': round(daily_median_check_vs_baseline, 2) if daily_median_check_vs_baseline is not None else None,
            'marketing_spend': round(daily_marketing_spend, 2),
            'remaining_group_budget': round(remaining_group_budget, 2),
            'new_users_count': daily_new_users,
            'returning_users_count': daily_returning_users,
            'new_user_percentage': round(daily_new_user_percentage, 2) if daily_new_user_percentage is not None else None,
            'notes': daily_notes
        })

# ============================================================================
# OUTPUT RESULTS
# ============================================================================

print(f"\nCreating output files...")
print(f"  Weekly results: {len(weekly_results)} entries")
print(f"  Daily results: {len(daily_results)} entries")

# Create weekly output
weekly_df = pd.DataFrame(weekly_results)
weekly_output_file = 'activation_performance_analysis_weekly.csv'
weekly_df.to_csv(weekly_output_file, index=False)

# Create daily output
daily_df = pd.DataFrame(daily_results)
daily_output_file = 'activation_performance_analysis_daily.csv'
daily_df.to_csv(daily_output_file, index=False)

print(f"\n✓ Analysis complete!")
print(f"  Weekly output saved to: {weekly_output_file}")
print(f"  Daily output saved to: {daily_output_file}")

print(f"\nWeekly Summary Statistics:")
print(f"  Total weekly entries: {len(weekly_df)}")
print(f"  Activations with zero transactions: {len(weekly_df[weekly_df['notes'] != ''])}")
print(f"  Total marketing spend: ${weekly_df['marketing_spend'].sum():,.2f}")
print(f"  Average TPV per week: ${weekly_df[weekly_df['total_tpv'] > 0]['total_tpv'].mean():,.2f}")
print(f"  Average users per week: {weekly_df[weekly_df['unique_users_count'] > 0]['unique_users_count'].mean():.1f}")
print(f"  Average new user percentage: {weekly_df[weekly_df['new_user_percentage'].notna()]['new_user_percentage'].mean():.1f}%")

print(f"\nDaily Summary Statistics:")
print(f"  Total daily entries: {len(daily_df)}")
print(f"  Days with zero transactions: {len(daily_df[daily_df['notes'] != ''])}")
print(f"  Total marketing spend: ${daily_df['marketing_spend'].sum():,.2f}")
print(f"  Average TPV per day: ${daily_df[daily_df['total_tpv'] > 0]['total_tpv'].mean():,.2f}")
print(f"  Average users per day: {daily_df[daily_df['unique_users_count'] > 0]['unique_users_count'].mean():.1f}")
print(f"  Average new user percentage: {daily_df[daily_df['new_user_percentage'].notna()]['new_user_percentage'].mean():.1f}%")