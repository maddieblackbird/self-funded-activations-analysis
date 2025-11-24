-- OPTIMIZED VERSION: Uses temporary tables and indexes for better performance

-- Step 1: Create temporary tables with indexes
CREATE TEMP TABLE IF NOT EXISTS temp_weeks AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY week_start) AS week_number,
    'Week ' || ROW_NUMBER() OVER (ORDER BY week_start) AS week_label,
    week_start::timestamp,
    (week_start + INTERVAL '6 days 23 hours 59 minutes 59 seconds')::timestamp AS week_end
FROM (
    SELECT DATE_TRUNC('week', generate_series(
        DATE_TRUNC('week', '2025-10-13'::date),
        DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day',
        INTERVAL '7 days'
    ))::timestamp AS week_start
) weeks;

CREATE INDEX idx_temp_weeks ON temp_weeks(week_start, week_end);

-- Step 2: Create enhanced transactions table with indexes
CREATE TEMP TABLE IF NOT EXISTS temp_transactions AS
SELECT 
    cs.check_share_id,
    cs.created_at_edt::timestamp AS created_at_dt,
    cs.adj_amount AS amount,
    cs.user_id,
    cs.location_id,
    l.name AS location_name,
    r.restaurant_id,
    r.name AS restaurant_name,
    r.restaurant_group_id,
    LOWER(TRIM(r.name)) || '||' || LOWER(TRIM(l.name)) AS match_key
FROM {{#1159-core-query-all-bbpay-check-shares}} cs
JOIN locations l ON cs.location_id = l.location_id
JOIN restaurants r ON l.restaurant_id = r.restaurant_id
WHERE cs.created_at_edt IS NOT NULL
  AND cs.created_at_edt::timestamp >= '2025-10-13'::timestamp;

CREATE INDEX idx_temp_tx_match ON temp_transactions(match_key, user_id, created_at_dt);
CREATE INDEX idx_temp_tx_user ON temp_transactions(user_id, created_at_dt);
CREATE INDEX idx_temp_tx_dt ON temp_transactions(created_at_dt);

-- Step 3: Create activations table with parsed values
CREATE TEMP TABLE IF NOT EXISTS temp_activations AS
SELECT 
    ae.id,
    ae.description,
    ae.start_date::timestamp AS start_dt,
    ae.end_date::timestamp AS end_dt,
    ae.location_id,
    l.name AS location_name,
    r.restaurant_id,
    r.name AS restaurant_name,
    r.restaurant_group_id,
    rg.name AS restaurant_group_name,
    LOWER(TRIM(r.name)) || '||' || LOWER(TRIM(l.name)) AS match_key,
    -- Parse minimum spend
    CAST(SUBSTRING(ae.description FROM 'Spend \$([0-9]+)') AS NUMERIC) AS minimum_spend,
    -- Parse reward amount
    CAST(SUBSTRING(ae.description FROM 'get \$([0-9]+)') AS NUMERIC) AS reward_amount,
    -- Get initial budget
    (SELECT SUM(allocation_fly::numeric / 1e20) 
     FROM gtm.fly_contract_drops 
     WHERE restaurant_group_id = r.restaurant_group_id) AS group_initial_budget
FROM activation_events ae
JOIN locations l ON ae.location_id = l.location_id
JOIN restaurants r ON l.restaurant_id = r.restaurant_id
LEFT JOIN restaurant_groups rg ON r.restaurant_group_id = rg.restaurant_group_id
WHERE ae.description LIKE 'Spend $%'
  AND ae.start_date::timestamp <= CURRENT_DATE
  AND ae.end_date::timestamp >= '2025-10-13';

CREATE INDEX idx_temp_act_match ON temp_activations(match_key);
CREATE INDEX idx_temp_act_dates ON temp_activations(start_dt, end_dt);

-- Step 4: Filter to valid activations only
DELETE FROM temp_activations 
WHERE minimum_spend IS NULL OR reward_amount IS NULL;

-- Step 5: Pre-calculate qualifying windows (most expensive operation)
CREATE TEMP TABLE IF NOT EXISTS temp_qualifying_windows AS
WITH window_calculations AS (
    SELECT 
        t1.match_key,
        t1.user_id,
        t1.created_at_dt AS window_start,
        a.id AS activation_id,
        a.minimum_spend,
        a.reward_amount,
        a.restaurant_group_id,
        w.week_number,
        w.week_label,
        SUM(t2.amount) AS window_total
    FROM temp_transactions t1
    JOIN temp_activations a 
        ON t1.match_key = a.match_key
        AND t1.created_at_dt >= a.start_dt
        AND t1.created_at_dt <= a.end_dt
    JOIN temp_weeks w
        ON t1.created_at_dt >= w.week_start
        AND t1.created_at_dt <= w.week_end
    JOIN temp_transactions t2
        ON t1.match_key = t2.match_key
        AND t1.user_id = t2.user_id
        AND t2.created_at_dt >= t1.created_at_dt
        AND t2.created_at_dt <= t1.created_at_dt + INTERVAL '1 hour'
    GROUP BY 
        t1.match_key, t1.user_id, t1.created_at_dt,
        a.id, a.minimum_spend, a.reward_amount, a.restaurant_group_id,
        w.week_number, w.week_label
)
SELECT 
    match_key,
    user_id,
    window_start,
    activation_id,
    restaurant_group_id,
    week_number,
    week_label,
    minimum_spend,
    reward_amount,
    window_total,
    CASE WHEN window_total >= minimum_spend THEN 1 ELSE 0 END AS is_qualified
FROM window_calculations;

CREATE INDEX idx_temp_windows ON temp_qualifying_windows(activation_id, week_number, is_qualified);

-- Step 6: Calculate redemptions per activation per week
CREATE TEMP TABLE IF NOT EXISTS temp_weekly_redemptions AS
SELECT 
    a.restaurant_name,
    a.location_name,
    a.restaurant_group_id,
    a.group_initial_budget,
    qw.week_number,
    qw.week_label,
    a.minimum_spend,
    a.reward_amount,
    COUNT(DISTINCT CASE WHEN qw.is_qualified = 1 THEN qw.user_id || '||' || qw.window_start END) AS redemption_count,
    COUNT(DISTINCT CASE WHEN qw.is_qualified = 1 THEN qw.user_id || '||' || qw.window_start END) * a.reward_amount AS marketing_spend
FROM temp_activations a
JOIN temp_qualifying_windows qw ON a.id = qw.activation_id
GROUP BY 
    a.restaurant_name,
    a.location_name,
    a.restaurant_group_id,
    a.group_initial_budget,
    qw.week_number,
    qw.week_label,
    a.minimum_spend,
    a.reward_amount;

-- Step 7: Calculate cumulative spend and output final results
WITH cumulative_spend AS (
    SELECT 
        restaurant_group_id,
        week_number,
        SUM(SUM(marketing_spend)) OVER (
            PARTITION BY restaurant_group_id 
            ORDER BY week_number
            ROWS UNBOUNDED PRECEDING
        ) AS total_spent_to_date
    FROM temp_weekly_redemptions
    GROUP BY restaurant_group_id, week_number
)
SELECT 
    wr.week_label AS week,
    wr.restaurant_name || ' - ' || wr.location_name AS "restaurant name / restaurant location",
    wr.marketing_spend AS "total marketing spend",
    ROUND(
        COALESCE(wr.group_initial_budget, 0) - 
        COALESCE(cs.total_spent_to_date, 0), 
        2
    ) AS "total remaining budget"
FROM temp_weekly_redemptions wr
LEFT JOIN cumulative_spend cs
    ON wr.restaurant_group_id = cs.restaurant_group_id
    AND wr.week_number = cs.week_number
WHERE wr.marketing_spend > 0
ORDER BY 
    wr.week_number,
    wr.restaurant_name,
    wr.location_name;

-- Clean up temporary tables
DROP TABLE IF EXISTS temp_weeks;
DROP TABLE IF EXISTS temp_transactions;
DROP TABLE IF EXISTS temp_activations;
DROP TABLE IF EXISTS temp_qualifying_windows;
DROP TABLE IF EXISTS temp_weekly_redemptions;
