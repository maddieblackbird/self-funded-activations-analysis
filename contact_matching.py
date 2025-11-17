#!/usr/bin/env python3
"""
Restaurant Email Matcher

PURPOSE: Match restaurants from activation_performance_analysis_weekly.csv to contacts in all_contacts.csv
and append email addresses for matches above 70% confidence.

For each restaurant (restaurant_name + location_name), find matching contacts and collect their emails.
Emails from all matches with confidence >= 70% are included in the "emails" column.
"""

import csv
import re
import os
from difflib import SequenceMatcher
from anthropic import Anthropic

def normalize_name(name):
    """Normalize restaurant names for better matching"""
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to lowercase
    name = name.lower()
    
    # Remove common business suffixes and prefixes
    patterns = [
        r'\s+(llc|inc|corp|corporation|ltd|limited|co\.?)\b',
        r'\s+(restaurant|restaurants|rest\.?)\b',
        r'\s+(group|hospitality|concepts?)\b',
        r'\bthe\s+',
        r'\s+&\s+',
        r'[^\w\s]',  # Remove special characters except spaces
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, ' ', name)
    
    # Remove extra spaces and strip
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def fuzzy_match_score(str1, str2):
    """Calculate fuzzy match score using SequenceMatcher"""
    norm1 = normalize_name(str1)
    norm2 = normalize_name(str2)
    
    if not norm1 or not norm2:
        return 0.0
    
    # Base similarity
    base_score = SequenceMatcher(None, norm1, norm2).ratio()
    
    return base_score

def reasoning_match_boost(str1, str2):
    """
    Apply reasoning-based matching boost for semantic similarities
    This uses word overlap and containment logic
    """
    norm1 = normalize_name(str1)
    norm2 = normalize_name(str2)
    
    if not norm1 or not norm2:
        return 0.0
    
    boost = 0.0
    
    # Check for exact substring containment
    if norm1 in norm2 or norm2 in norm1:
        boost += 0.15
    
    # Check for word overlap
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    if words1 and words2:
        # Calculate Jaccard similarity of words
        intersection = words1 & words2
        union = words1 | words2
        word_overlap = len(intersection) / len(union)
        
        if word_overlap >= 0.7:
            boost += 0.10
        elif word_overlap >= 0.5:
            boost += 0.05
    
    # Check for common abbreviations (e.g., "mgmt" vs "management")
    abbrev_pairs = [
        ('mgmt', 'management'),
        ('hosp', 'hospitality'),
        ('rest', 'restaurant'),
        ('grp', 'group'),
    ]
    
    for abbrev, full in abbrev_pairs:
        if (abbrev in norm1 and full in norm2) or (abbrev in norm2 and full in norm1):
            boost += 0.03
    
    return min(boost, 0.25)  # Cap boost at 25%

def verify_match_with_claude(restaurant_name, contact_name, client):
    """
    Use Claude Sonnet to verify if the restaurant and contact are actually the same place
    Returns (is_match: bool, confidence_adjustment: float, reasoning: str)
    """
    if not client:
        return (True, 0.0, "Claude not available")
    
    try:
        prompt = f"""You are helping match restaurant names from activation data to contact restaurant names.

Restaurant from activation data: "{restaurant_name}"
Contact restaurant name: "{contact_name}"

Question: Are these referring to the SAME restaurant? 

IMPORTANT CONSIDERATIONS:
1. Do the restaurant names match (allowing for location suffixes in the activation data)?
2. The activation data name may include a location suffix (e.g., "Crave Fishbar Upper West Side")
   while the contact name is just the base name (e.g., "Crave Fishbar")
3. This is acceptable - they're the SAME restaurant if the core name matches
4. Only reject if restaurant names are clearly DIFFERENT

Examples of SAME restaurant:
- "Crave Fishbar Upper West Side" vs "Crave Fishbar" → SAME (activation has location suffix)
- "Joe's Pizza Soho" vs "Joe's Pizza" → SAME (activation has location suffix)
- "Andros Taverna North Side" vs "Andros Taverna" → SAME (activation has location suffix)

Examples of DIFFERENT restaurants:
- "Carbone" vs "Carbone Miami" → DIFFERENT (these are different locations of same brand)
- "Smith Restaurant" vs "The Smith" → POTENTIALLY DIFFERENT (need to verify)
- "Momofuku Noodle Bar" vs "Momofuku Ko" → DIFFERENT (different restaurants in same group)

Respond with ONLY ONE of these exact formats:

Format 1 (if SAME restaurant):
MATCH
Confidence: [number between 0.0 and 1.0]
Reasoning: [one sentence explanation]

Format 2 (if DIFFERENT restaurants):
NO_MATCH
Reasoning: [one sentence explanation]

Be strict - only say MATCH if you're confident they're the same restaurant."""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response = message.content[0].text.strip()
        
        # Parse response
        lines = response.split('\n')
        is_match = lines[0].strip().upper() == 'MATCH'
        
        if is_match:
            # Extract confidence and reasoning
            confidence_line = [l for l in lines if l.startswith('Confidence:')]
            reasoning_line = [l for l in lines if l.startswith('Reasoning:')]
            
            confidence_adjustment = 0.0
            if confidence_line:
                try:
                    conf_str = confidence_line[0].split(':', 1)[1].strip()
                    claude_conf = float(conf_str)
                    # Adjust overall confidence based on Claude's assessment
                    confidence_adjustment = (claude_conf - 0.85) * 0.5  # Scale Claude's impact
                except:
                    pass
            
            reasoning = reasoning_line[0].split(':', 1)[1].strip() if reasoning_line else "Match verified"
            
            return (True, confidence_adjustment, reasoning)
        else:
            reasoning_line = [l for l in lines if l.startswith('Reasoning:')]
            reasoning = reasoning_line[0].split(':', 1)[1].strip() if reasoning_line else "Not the same restaurant"
            return (False, -1.0, reasoning)
            
    except Exception as e:
        print(f"\n     ⚠ Claude API error: {str(e)}")
        return (True, 0.0, "Claude verification failed - defaulting to fuzzy match")

def find_matching_contacts(restaurant_name, contacts, claude_client, min_confidence=0.70):
    """
    Find all matching contacts for a given restaurant name
    Returns list of matches with their emails and confidence scores
    """
    candidate_matches = []
    
    # First pass: fuzzy matching
    for contact in contacts:
        contact_restaurant = contact.get('restaurant name', '')
        if not contact_restaurant:
            continue
        
        # Calculate base fuzzy score
        base_score = fuzzy_match_score(restaurant_name, contact_restaurant)
        
        # Apply reasoning boost
        boost = reasoning_match_boost(restaurant_name, contact_restaurant)
        
        # Combined score
        combined_score = min(base_score + boost, 1.0)
        
        # Only consider if above minimum threshold
        if combined_score >= min_confidence:
            candidate_matches.append({
                'contact': contact,
                'restaurant_name': contact_restaurant,
                'fuzzy_score': combined_score,
                'email': contact.get('email_address', '')
            })
    
    # Sort by fuzzy score
    candidate_matches.sort(key=lambda x: x['fuzzy_score'], reverse=True)
    
    # Second pass: Claude verification for top candidates
    verified_matches = []
    
    for candidate in candidate_matches[:10]:  # Only verify top 10 candidates
        if claude_client:
            is_match, confidence_adj, reasoning = verify_match_with_claude(
                restaurant_name, 
                candidate['restaurant_name'], 
                claude_client
            )
            
            if is_match:
                final_confidence = min(candidate['fuzzy_score'] + confidence_adj, 1.0)
                verified_matches.append({
                    'restaurant_name': candidate['restaurant_name'],
                    'email': candidate['email'],
                    'confidence': final_confidence,
                    'claude_verified': True,
                    'claude_reasoning': reasoning
                })
        else:
            # No Claude verification - use fuzzy score only
            verified_matches.append({
                'restaurant_name': candidate['restaurant_name'],
                'email': candidate['email'],
                'confidence': candidate['fuzzy_score'],
                'claude_verified': False,
                'claude_reasoning': 'No Claude verification'
            })
    
    return verified_matches

def load_contacts(contacts_file):
    """Load all contacts from CSV"""
    contacts = []
    with open(contacts_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            contacts.append(row)
    return contacts

def load_activation_data(activation_file):
    """Load activation performance data"""
    data = []
    with open(activation_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            data.append(row)
    return data, fieldnames

def main():
    print("Script starting...")
    import sys
    sys.stdout.flush()
    
    # File paths
    activation_file = 'activation_performance_analysis_weekly.csv'
    contacts_file = 'all_contacts.csv'
    output_file = 'activation_performance_analysis_weekly_with_emails.csv'
    
    # Confidence threshold for fuzzy matching
    min_confidence = 0.70
    
    # Initialize Claude client
    claude_client = None
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if api_key:
        claude_client = Anthropic(api_key=api_key)
        print("✓ Claude Sonnet API initialized for match verification")
    else:
        print("⚠ ANTHROPIC_API_KEY not found - Claude verification disabled")
        print("  Set it with: export ANTHROPIC_API_KEY='your-key-here'")
    
    print("=" * 70)
    print("Restaurant Email Matcher")
    print("=" * 70)
    
    # Load data
    print(f"\n📁 Loading contacts from {contacts_file}...")
    contacts = load_contacts(contacts_file)
    print(f"   ✓ Loaded {len(contacts)} contacts")
    
    print(f"\n📁 Loading activation data from {activation_file}...")
    activation_data, original_fieldnames = load_activation_data(activation_file)
    print(f"   ✓ Loaded {len(activation_data)} restaurants")
    
    # Add new columns to fieldnames
    new_fieldnames = list(original_fieldnames) + ['emails', 'email_match_confidence', 'email_match_notes']
    
    # Process each restaurant
    print(f"\n🔍 Matching restaurants to contacts (min confidence: {min_confidence:.0%})...")
    print("=" * 70)
    
    results = []
    perfect_match_count = 0
    partial_match_count = 0
    no_match_count = 0
    
    for idx, row in enumerate(activation_data):
        restaurant_name = row.get('restaurant_name', '')
        location_name = row.get('location_name', '')
        
        # Combine restaurant name and location
        combined_name = f"{restaurant_name} {location_name}".strip()
        
        if not combined_name:
            print(f"\n[{idx + 1}/{len(activation_data)}] ⚠ Empty restaurant name - skipping")
            row['emails'] = ''
            row['email_match_confidence'] = 'N/A'
            row['email_match_notes'] = 'Empty restaurant name'
            results.append(row)
            no_match_count += 1
            continue
        
        print(f"\n[{idx + 1}/{len(activation_data)}] Matching: \"{combined_name}\"")
        
        # Find matching contacts
        matches = find_matching_contacts(combined_name, contacts, claude_client, min_confidence)
        
        if not matches:
            print(f"  ⏭️  No matches found")
            row['emails'] = ''
            row['email_match_confidence'] = 'No match'
            row['email_match_notes'] = 'No contacts matched above confidence threshold'
            results.append(row)
            no_match_count += 1
            continue
        
        # Separate 100% confidence matches from others
        perfect_matches = [m for m in matches if m['confidence'] >= 0.999]  # 100% or very close
        partial_matches = [m for m in matches if m['confidence'] < 0.999]
        
        # Collect emails from all matches above 70% confidence
        all_match_emails = []
        for match in matches:  # Include all matches (already filtered to >= 70% by find_matching_contacts)
            if match['email']:
                all_match_emails.append(match['email'])
        
        # Display results
        if perfect_matches:
            print(f"  ✅ Found {len(perfect_matches)} 100% confidence match(es):")
            for match in perfect_matches:
                email_display = match['email'] if match['email'] else '(no email)'
                print(f"     ✓ \"{match['restaurant_name']}\" - {match['confidence']:.1%} - {email_display}")
                if match.get('claude_reasoning'):
                    print(f"        Claude: {match['claude_reasoning']}")
            perfect_match_count += 1
        
        if partial_matches:
            print(f"  📋 Found {len(partial_matches)} partial match(es) (not 100%):")
            for match in partial_matches:
                email_display = match['email'] if match['email'] else '(no email)'
                print(f"     ○ \"{match['restaurant_name']}\" - {match['confidence']:.1%} - {email_display}")
            if not perfect_matches:
                partial_match_count += 1
        
        # Populate result
        row['emails'] = ', '.join(all_match_emails) if all_match_emails else ''
        
        if perfect_matches:
            row['email_match_confidence'] = '100%'
            row['email_match_notes'] = f"{len(all_match_emails)} email(s) from {len(matches)} match(es) (including {len(perfect_matches)} perfect)"
        elif partial_matches:
            best_partial = max(partial_matches, key=lambda x: x['confidence'])
            row['email_match_confidence'] = f"{best_partial['confidence']:.1%}"
            row['email_match_notes'] = f"{len(all_match_emails)} email(s) from {len(matches)} match(es) above 70%"
        else:
            row['email_match_confidence'] = 'No match'
            row['email_match_notes'] = 'No matches found'
        
        results.append(row)
    
    # Write output
    print(f"\n💾 Writing results to {output_file}...")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Summary
    print(f"\n" + "=" * 70)
    print("📊 MATCHING SUMMARY")
    print("=" * 70)
    print(f"Total restaurants processed:      {len(activation_data)}")
    print(f"  ✅ With 100% match & emails:    {perfect_match_count}")
    print(f"  📋 With partial matches only:   {partial_match_count}")
    print(f"  ⏭️  No matches found:            {no_match_count}")
    print(f"")
    print(f"✅ Done! Results saved to: {output_file}")
    print("=" * 70)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()