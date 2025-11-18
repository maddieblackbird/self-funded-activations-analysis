#!/usr/bin/env python3
"""
Generate email for The Bar at The Spectator with 3 weeks of performance data
"""

import pandas as pd
from datetime import datetime

def format_datetime(dt_str):
    """Convert '2025-11-05 15:00:00' to 'Tuesday, 3:00 PM'"""
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    day_name = dt.strftime('%A')
    time_12hr = dt.strftime('%I:%M %p').lstrip('0')  # Remove leading zero
    return f"{day_name}, {time_12hr}"

def format_money(value):
    """Format number as money with commas"""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"

def format_percent(value):
    """Format percentage with + or - sign"""
    if pd.isna(value):
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"

def generate_week_section(week_data):
    """Generate one week's section of the email"""
    
    # Format activation times
    activation_times = f"{format_datetime(week_data['activation_start'])} to {format_datetime(week_data['activation_end'])}"
    
    section = f"""
<b>Week {week_data['week'].replace('Week ', '')} ({week_data['week_label']}):</b>

The <b>{week_data['activation_description']}</b> promotion ran from <b>{activation_times}</b>.

<b>The Stats:</b>
• <b>Successful Redemptions:</b> {week_data['unique_users_count_REDEEMED']} customers hit the ${week_data['minimum_spend_threshold']:.0f} minimum and earned ${week_data['reward_amount']:.0f} in $FLY
• <b>Total Unique Customers:</b> {week_data['unique_users_count']} unique customers total transacted at your location during the activation period
• <b>Total Payment Volume:</b> {format_money(week_data['total_tpv'])} processed through Blackbird Pay (this includes all customers who paid during the activation window, even if they didn't hit the minimum spend!)
• <b>Median Check Size:</b> {format_money(week_data['median_check'])}
• <b>New vs Returning:</b> {week_data['new_users_count']} new customers ({week_data['new_user_percentage']:.1f}%) and {week_data['returning_users_count']} returning customers visited during the promotion
• <b>Marketing Budget Spent at This Location:</b> {format_money(week_data['marketing_spend'])} (based on {week_data['unique_users_count_REDEEMED']} redemptions × ${week_data['reward_amount']:.0f} reward)
• <b>Remaining Group Budget*:</b> {format_money(week_data['remaining_group_budget'])}

<b>Performance vs Your Baseline:</b>
• <b>TPV Lift:</b> {format_percent(week_data['tpv_vs_baseline'])} compared to the average of the previous 4 weeks (same days of the week, excluding other promotion periods)
• <b>Check Size Change:</b> {format_percent(week_data['median_check_vs_baseline'])} change in median check compared to baseline
"""
    
    if week_data['notes']:
        section += f"\n<b>Note:</b> {week_data['notes']}\n"
    
    return section

def main():
    # Load weekly data
    df = pd.read_csv('activation_performance_analysis_weekly.csv')
    
    # Filter for The Bar at The Spectator
    spectator_data = df[df['restaurant_name'] == 'The Bar at The Spectator'].copy()
    
    if len(spectator_data) == 0:
        print("❌ No data found for 'The Bar at The Spectator'")
        return
    
    # Sort by week
    week_order = {'Week 1': 1, 'Week 2': 2, 'Week 3': 3}
    spectator_data['week_num'] = spectator_data['week'].map(week_order)
    spectator_data = spectator_data.sort_values('week_num')
    
    # Add week labels
    week_labels = {
        'Week 1': 'Three weeks ago',
        'Week 2': 'Two weeks ago', 
        'Week 3': 'Last week'
    }
    spectator_data['week_label'] = spectator_data['week'].map(week_labels)
    
    # Generate email
    print("=" * 80)
    print("EMAIL FOR THE BAR AT THE SPECTATOR")
    print("=" * 80)
    print()
    
    email = f"""Hi there, hope you are having a great start to your week!

I wanted to update you on how <b>The Bar at The Spectator</b> <b>French Quarter</b> has performed over the past three weeks with your Blackbird promotions. Here's a breakdown for each week:

---
"""
    
    # Add each week's section
    for _, week_data in spectator_data.iterrows():
        email += generate_week_section(week_data)
        email += "\n---\n"
    
    email += """
<b>Overall Thoughts:</b>

Looking at the three-week trend, how do you think these promotions are performing? Are there any patterns you're seeing that we should capitalize on or adjust?

I'd love to hear your thoughts on:
• Which time slots or days seem to be working best?
• Any changes you'd like to make to the spend thresholds or reward amounts?
• Different days or times you'd like to test?

Best, 
Maddie

*Reminder: If you have multiple restaurants under your hospitality group, this Initial Allotted Marketing Budget is shared across all locations.
"""
    
    print(email)
    print()
    print("=" * 80)
    
    # Save to file
    with open('spectator_email.html', 'w') as f:
        f.write(email)
    
    print("✓ Email saved to: spectator_email.html")
    print()

if __name__ == '__main__':
    main()

