WITH 
-- Configuration
config AS (
    SELECT 
        '2025-10-13'::timestamp AS program_start_date,
        CURRENT_DATE AS analysis_end_date
),

-- Generate all complete calendar weeks since program start
all_weeks AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY week_start) AS week_number,
        'Week ' || ROW_NUMBER() OVER (ORDER BY week_start) AS week_label,
        week_start,
        week_start + INTERVAL '6 days 23 hours 59 minutes 59 seconds' AS week_end
    FROM (
        SELECT 
            DATE_TRUNC('week', generate_series.date)::timestamp AS week_start
        FROM (
            SELECT generate_series(
                DATE_TRUNC('week', (SELECT program_start_date FROM config))::date,
                DATE_TRUNC('week', CURRENT_DATE)::date - INTERVAL '1 day',
                INTERVAL '7 days'
            )::date AS date
        ) generate_series
    ) weeks
),

-- Market mapping for locations
location_markets AS (
    SELECT 
        DISTINCT l.location_id,
        l.name AS location_name,
        r.restaurant_id,
        r.name AS restaurant_name,
        r.restaurant_group_id,
        rg.name AS restaurant_group_name
    FROM locations l
    JOIN restaurants r ON l.restaurant_id = r.restaurant_id
    LEFT JOIN restaurant_groups rg ON r.restaurant_group_id = rg.restaurant_group_id
),

-- Get all transactions with enhanced info
transactions_enhanced AS (
    SELECT 
        cs.check_share_id,
        cs.created_at_edt::timestamp AS created_at_dt,
        cs.adj_amount AS amount,
        cs.user_id,
        cs.location_id,
        lm.restaurant_name,
        lm.location_name,
        lm.restaurant_id,
        lm.restaurant_group_id,
        lm.restaurant_group_name,
        LOWER(TRIM(lm.restaurant_name)) || '||' || LOWER(TRIM(lm.location_name)) AS match_key
    FROM {{#1159-core-query-all-bbpay-check-shares}} cs
    JOIN location_markets lm ON cs.location_id = lm.location_id
    WHERE cs.created_at_edt IS NOT NULL
),

-- Budget info by restaurant group
budget_info AS (
    SELECT
        restaurant_group_id,
        SUM(allocation_fly::numeric / 1e20) AS group_initial_budget
    FROM gtm.fly_contract_drops
    GROUP BY restaurant_group_id
),

-- Parse activation descriptions and amounts
activations_parsed AS (
    SELECT 
        ae.id,
        ae.description,
        ae.start_date::timestamp AS start_dt,
        ae.end_date::timestamp AS end_dt,
        ae.location_id,
        lm.restaurant_id,
        lm.restaurant_name,
        lm.location_name,
        lm.restaurant_group_id,
        lm.restaurant_group_name,
        bi.group_initial_budget,
        LOWER(TRIM(lm.restaurant_name)) || '||' || LOWER(TRIM(lm.location_name)) AS match_key,
        -- Parse minimum spend from description
        CASE 
            WHEN ae.description LIKE 'Spend $%' THEN
                CAST(
                    SUBSTRING(ae.description FROM 'Spend \$([0-9]+)')
                AS NUMERIC)
            ELSE NULL
        END AS minimum_spend,
        -- Parse reward amount from description
        CASE 
            WHEN ae.description LIKE '%get $% FLY%' OR ae.description LIKE '%get $% in%' THEN
                CAST(
                    SUBSTRING(ae.description FROM 'get \$([0-9]+)')
                AS NUMERIC)
            ELSE NULL
        END AS reward_amount
    FROM activation_events ae
    JOIN location_markets lm ON ae.location_id = lm.location_id
    LEFT JOIN budget_info bi ON lm.restaurant_group_id = bi.restaurant_group_id
    WHERE ae.description LIKE 'Spend $%'
),

-- Filter to valid activations
spend_activations AS (
    SELECT *
    FROM activations_parsed
    WHERE minimum_spend IS NOT NULL
    AND reward_amount IS NOT NULL
    AND start_dt <= (SELECT week_end FROM all_weeks ORDER BY week_number DESC LIMIT 1)
    AND end_dt >= (SELECT week_start FROM all_weeks ORDER BY week_number LIMIT 1)
),

-- Group activations by unique combination
activation_groups AS (
    SELECT 
        restaurant_name,
        location_name,
        restaurant_id,
        restaurant_group_id,
        match_key,
        minimum_spend,
        reward_amount,
        MAX(group_initial_budget) AS group_initial_budget,
        MIN(description) AS description,
        STRING_AGG(id::text, ', ' ORDER BY id) AS activation_ids,
        MIN(start_dt) AS overall_start_dt,
        MAX(end_dt) AS overall_end_dt
    FROM spend_activations
    GROUP BY 
        restaurant_name,
        location_name,
        restaurant_id,
        restaurant_group_id,
        match_key,
        minimum_spend,
        reward_amount
),

-- Calculate redemptions per week per activation group
weekly_redemptions AS (
    SELECT 
        ag.restaurant_name,
        ag.location_name,
        ag.restaurant_group_id,
        ag.match_key,
        ag.minimum_spend,
        ag.reward_amount,
        ag.group_initial_budget,
        w.week_label,
        w.week_number,
        COUNT(DISTINCT CASE 
            WHEN user_window_total.total_in_window >= ag.minimum_spend 
            THEN user_window_total.user_id 
        END) * ag.reward_amount AS marketing_spend
    FROM activation_groups ag
    CROSS JOIN all_weeks w
    -- Get each activation period for this group
    JOIN spend_activations sa 
        ON sa.restaurant_name = ag.restaurant_name
        AND sa.location_name = ag.location_name
        AND sa.minimum_spend = ag.minimum_spend
        AND sa.reward_amount = ag.reward_amount
    -- Get transactions during activation within week
    JOIN transactions_enhanced t
        ON t.match_key = ag.match_key
        AND t.created_at_dt >= GREATEST(sa.start_dt, w.week_start)
        AND t.created_at_dt <= LEAST(sa.end_dt, w.week_end)
    -- Calculate 1-hour window totals
    JOIN LATERAL (
        SELECT 
            t.user_id,
            t.created_at_dt AS window_start,
            SUM(t2.amount) AS total_in_window
        FROM transactions_enhanced t2
        WHERE t2.match_key = t.match_key
        AND t2.user_id = t.user_id
        AND t2.created_at_dt >= t.created_at_dt
        AND t2.created_at_dt <= t.created_at_dt + INTERVAL '1 hour'
        GROUP BY t.user_id, t.created_at_dt
    ) user_window_total ON true
    WHERE ag.overall_start_dt <= w.week_end 
    AND ag.overall_end_dt >= w.week_start
    GROUP BY 
        ag.restaurant_name,
        ag.location_name,
        ag.restaurant_group_id,
        ag.match_key,
        ag.minimum_spend,
        ag.reward_amount,
        ag.group_initial_budget,
        w.week_label,
        w.week_number
),

-- Calculate cumulative spend by restaurant group
cumulative_group_spend AS (
    SELECT 
        restaurant_group_id,
        week_label,
        week_number,
        SUM(SUM(marketing_spend)) OVER (
            PARTITION BY restaurant_group_id 
            ORDER BY week_number
            ROWS UNBOUNDED PRECEDING
        ) AS total_spent_to_date
    FROM weekly_redemptions
    GROUP BY restaurant_group_id, week_label, week_number
),

-- Final results
final_results AS (
    SELECT 
        wr.week_label AS week,
        wr.restaurant_name || ' - ' || wr.location_name AS "restaurant name / restaurant location",
        wr.marketing_spend AS "total marketing spend",
        ROUND(
            COALESCE(wr.group_initial_budget, 0) - 
            COALESCE(cgs.total_spent_to_date, 0), 
            2
        ) AS "total remaining budget"
    FROM weekly_redemptions wr
    LEFT JOIN cumulative_group_spend cgs
        ON wr.restaurant_group_id = cgs.restaurant_group_id
        AND wr.week_number = cgs.week_number
    WHERE wr.marketing_spend > 0
)

-- Output
SELECT 
    week,
    "restaurant name / restaurant location",
    "total marketing spend",
    "total remaining budget"
FROM final_results
ORDER BY 
    week,
    "restaurant name / restaurant location";
