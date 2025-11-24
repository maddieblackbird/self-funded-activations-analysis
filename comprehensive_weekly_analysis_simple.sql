-- SIMPLIFIED VERSION: Assumes each qualifying transaction independently earns the reward
-- This is less accurate than the 1-hour window logic but MUCH faster

WITH 
-- Generate weeks
weeks AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY week_start) AS week_number,
        'Week ' || ROW_NUMBER() OVER (ORDER BY week_start) AS week_label,
        DATE_TRUNC('week', dates.d)::timestamp AS week_start,
        DATE_TRUNC('week', dates.d)::timestamp + INTERVAL '6 days 23 hours 59 minutes 59 seconds' AS week_end
    FROM (
        SELECT generate_series(
            DATE_TRUNC('week', '2025-10-13'::date),
            DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day',
            INTERVAL '7 days'
        )::date AS d
    ) dates
),

-- Get restaurant/location info
location_info AS (
    SELECT 
        l.location_id,
        l.name AS location_name,
        r.restaurant_id,
        r.name AS restaurant_name,
        r.restaurant_group_id,
        LOWER(TRIM(r.name)) || '||' || LOWER(TRIM(l.name)) AS match_key
    FROM locations l
    JOIN restaurants r ON l.restaurant_id = r.restaurant_id
),

-- Get transactions with location info
transactions_with_info AS (
    SELECT 
        cs.created_at_edt::timestamp AS created_at_dt,
        cs.adj_amount AS amount,
        cs.user_id,
        li.restaurant_name,
        li.location_name,
        li.restaurant_group_id,
        li.match_key
    FROM {{#1159-core-query-all-bbpay-check-shares}} cs
    JOIN location_info li ON cs.location_id = li.location_id
    WHERE cs.created_at_edt IS NOT NULL
      AND cs.created_at_edt::timestamp >= '2025-10-13'
),

-- Parse activations
parsed_activations AS (
    SELECT 
        ae.id,
        ae.start_date::timestamp AS start_dt,
        ae.end_date::timestamp AS end_dt,
        li.restaurant_name,
        li.location_name,
        li.restaurant_group_id,
        li.match_key,
        -- Extract spend threshold
        CAST(SUBSTRING(ae.description FROM 'Spend \$([0-9]+)') AS NUMERIC) AS minimum_spend,
        -- Extract reward amount
        CAST(SUBSTRING(ae.description FROM 'get \$([0-9]+)') AS NUMERIC) AS reward_amount,
        ae.description
    FROM activation_events ae
    JOIN location_info li ON ae.location_id = li.location_id
    WHERE ae.description LIKE 'Spend $%'
      AND ae.start_date::timestamp <= CURRENT_DATE
      AND ae.end_date::timestamp >= '2025-10-13'
),

-- Get initial budgets
budgets AS (
    SELECT 
        restaurant_group_id,
        SUM(allocation_fly::numeric / 1e20) AS initial_budget
    FROM gtm.fly_contract_drops
    GROUP BY restaurant_group_id
),

-- Calculate weekly metrics
weekly_metrics AS (
    SELECT 
        w.week_number,
        w.week_label,
        pa.restaurant_name,
        pa.location_name,
        pa.restaurant_group_id,
        pa.minimum_spend,
        pa.reward_amount,
        -- Count qualifying transactions (simplified: each transaction >= minimum_spend counts)
        COUNT(CASE WHEN t.amount >= pa.minimum_spend THEN 1 END) AS qualifying_transactions,
        COUNT(CASE WHEN t.amount >= pa.minimum_spend THEN 1 END) * pa.reward_amount AS marketing_spend
    FROM weeks w
    CROSS JOIN parsed_activations pa
    LEFT JOIN transactions_with_info t
        ON t.match_key = pa.match_key
        AND t.created_at_dt >= GREATEST(pa.start_dt, w.week_start)
        AND t.created_at_dt <= LEAST(pa.end_dt, w.week_end)
    WHERE pa.minimum_spend IS NOT NULL 
      AND pa.reward_amount IS NOT NULL
      AND pa.start_dt <= w.week_end 
      AND pa.end_dt >= w.week_start
    GROUP BY 
        w.week_number,
        w.week_label,
        pa.restaurant_name,
        pa.location_name,
        pa.restaurant_group_id,
        pa.minimum_spend,
        pa.reward_amount
),

-- Calculate cumulative spend
cumulative_metrics AS (
    SELECT 
        wm.*,
        b.initial_budget,
        SUM(wm.marketing_spend) OVER (
            PARTITION BY wm.restaurant_group_id 
            ORDER BY wm.week_number 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_spend
    FROM weekly_metrics wm
    LEFT JOIN budgets b ON wm.restaurant_group_id = b.restaurant_group_id
    WHERE wm.marketing_spend > 0
)

-- Final output
SELECT 
    week_label AS week,
    restaurant_name || ' - ' || location_name AS "restaurant name / restaurant location",
    marketing_spend AS "total marketing spend",
    ROUND(COALESCE(initial_budget, 0) - cumulative_spend, 2) AS "total remaining budget"
FROM cumulative_metrics
ORDER BY week_number, restaurant_name, location_name;
