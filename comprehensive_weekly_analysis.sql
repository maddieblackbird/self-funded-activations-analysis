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
        rg.name AS restaurant_group_name,
        cz.market,
        cz.time_zone
    FROM locations l
    JOIN restaurants r ON l.restaurant_id = r.restaurant_id
    LEFT JOIN restaurant_groups rg ON r.restaurant_group_id = rg.restaurant_group_id
    JOIN {{#17260-census-state-city-zip}} cz ON cz.zipcode = l.zipcode
),

-- Get all transactions with enhanced info
transactions_enhanced AS (
    SELECT 
        t.*,
        t.created_at_edt::timestamp AS created_at_dt,
        t.adj_amount AS amount,
        lm.restaurant_name,
        lm.location_name,
        lm.restaurant_id,
        lm.restaurant_group_id,
        lm.restaurant_group_name,
        LOWER(TRIM(lm.restaurant_name)) || '||' || LOWER(TRIM(lm.location_name)) AS match_key,
        -- Get refund amount if exists
        COALESCE(ref.amount / 100.0, 0.0) AS adj_refund_amount,
        -- Industry user tag
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM employees e 
                WHERE e.user_id = t.user_id 
                AND e.active = TRUE
            ) THEN 'industry'
            ELSE NULL
        END AS user_tag
    FROM {{#1159-core-query-all-bbpay-check-shares}} t
    LEFT JOIN locations l ON l.location_id = t.location_id
    LEFT JOIN location_markets lm ON l.location_id = lm.location_id
    LEFT JOIN refunds ref ON ref.check_share_id = t.check_share_id
    WHERE t.created_at_edt IS NOT NULL
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
        ae.*,
        ae.start_date::timestamp AS start_dt,
        ae.end_date::timestamp AS end_dt,
        lm.restaurant_id,
        lm.restaurant_name,
        lm.location_id,
        lm.location_name,
        lm.restaurant_group_id,
        lm.restaurant_group_name,
        bi.group_initial_budget,
        LOWER(TRIM(lm.restaurant_name)) || '||' || LOWER(TRIM(lm.location_name)) AS match_key,
        -- Parse minimum spend and reward amount from description
        CASE 
            WHEN ae.description LIKE 'Spend $%' THEN
                CAST(REGEXP_REPLACE(
                    REGEXP_REPLACE(ae.description, '^.*Spend \$([0-9]+(?:\.[0-9]+)?).*$', '\1'),
                    '[^0-9.]', '', 'g'
                ) AS NUMERIC)
            ELSE NULL
        END AS minimum_spend,
        CASE 
            WHEN ae.description LIKE 'Spend $%' THEN
                CAST(REGEXP_REPLACE(
                    REGEXP_REPLACE(ae.description, '^.*(?:get|receive|earn) \$([0-9]+(?:\.[0-9]+)?).*$', '\1'),
                    '[^0-9.]', '', 'g'
                ) AS NUMERIC)
            ELSE NULL
        END AS reward_amount
    FROM activation_events ae
    LEFT JOIN location_markets lm ON ae.location_id = lm.location_id
    LEFT JOIN budget_info bi ON lm.restaurant_group_id = bi.restaurant_group_id
),

-- Filter to only "Spend $" activations that overlap with analysis period
spend_activations AS (
    SELECT *
    FROM activations_parsed
    WHERE description LIKE 'Spend $%'
    AND minimum_spend IS NOT NULL
    AND reward_amount IS NOT NULL
    AND start_dt <= (SELECT week_end FROM all_weeks ORDER BY week_number DESC LIMIT 1)
    AND end_dt >= (SELECT week_start FROM all_weeks ORDER BY week_number LIMIT 1)
),

-- Group activations by unique restaurant/location/offer combination
activation_groups AS (
    SELECT 
        restaurant_name,
        location_name,
        restaurant_id,
        restaurant_group_id,
        restaurant_group_name,
        match_key,
        minimum_spend,
        reward_amount,
        group_initial_budget,
        MIN(description) AS description, -- Use shortest description
        STRING_AGG(id::text, ', ' ORDER BY id) AS activation_id_display,
        MIN(start_dt) AS overall_start_dt,
        MAX(end_dt) AS overall_end_dt,
        ARRAY_AGG(
            JSON_BUILD_OBJECT(
                'id', id,
                'start', start_dt,
                'end', end_dt
            ) ORDER BY start_dt
        ) AS activation_periods
    FROM spend_activations
    GROUP BY 
        restaurant_name,
        location_name,
        restaurant_id,
        restaurant_group_id,
        restaurant_group_name,
        match_key,
        minimum_spend,
        reward_amount,
        group_initial_budget
),

-- Calculate first transaction date for each user at each location
user_first_transactions AS (
    SELECT 
        match_key,
        user_id,
        MIN(created_at_dt) AS first_transaction_date
    FROM transactions_enhanced
    GROUP BY match_key, user_id
),

-- Get qualifying redemptions per user per activation
user_redemptions AS (
    SELECT DISTINCT ON (ag.match_key, w.week_label, te.user_id, first_txn.created_at_dt)
        ag.match_key,
        ag.restaurant_group_id,
        ag.minimum_spend,
        ag.reward_amount,
        w.week_label,
        w.week_start,
        w.week_end,
        te.user_id,
        first_txn.created_at_dt AS window_start,
        SUM(te.amount) AS window_total,
        CASE 
            WHEN SUM(te.amount) >= ag.minimum_spend THEN 1 
            ELSE 0 
        END AS qualified_redemption
    FROM activation_groups ag
    CROSS JOIN all_weeks w
    CROSS JOIN LATERAL (
        SELECT period->>'start' AS period_start,
               period->>'end' AS period_end
        FROM JSONB_ARRAY_ELEMENTS(TO_JSONB(ag.activation_periods)) AS period
    ) periods
    JOIN transactions_enhanced first_txn 
        ON first_txn.match_key = ag.match_key
        AND first_txn.created_at_dt >= GREATEST(periods.period_start::timestamp, w.week_start)
        AND first_txn.created_at_dt <= LEAST(periods.period_end::timestamp, w.week_end)
    JOIN transactions_enhanced te 
        ON te.match_key = first_txn.match_key
        AND te.user_id = first_txn.user_id
        AND te.created_at_dt >= first_txn.created_at_dt
        AND te.created_at_dt <= first_txn.created_at_dt + INTERVAL '1 hour'
    WHERE ag.overall_start_dt <= w.week_end 
    AND ag.overall_end_dt >= w.week_start
    GROUP BY 
        ag.match_key,
        ag.restaurant_group_id,
        ag.minimum_spend,
        ag.reward_amount,
        w.week_label,
        w.week_start,
        w.week_end,
        te.user_id,
        first_txn.created_at_dt
),

-- Main analysis per activation group per week
weekly_analysis AS (
    SELECT 
        w.week_label AS week,
        ag.activation_id_display AS activation_id,
        ag.restaurant_name,
        ag.location_name,
        ag.description AS activation_description,
        ag.minimum_spend AS minimum_spend_threshold,
        ag.reward_amount,
        ag.overall_start_dt::text AS activation_start,
        ag.overall_end_dt::text AS activation_end,
        -- Transaction metrics
        COUNT(DISTINCT t.user_id) AS unique_users_count,
        COUNT(DISTINCT CASE WHEN ur.qualified_redemption = 1 THEN ur.user_id END) AS unique_users_count_redeemed,
        COALESCE(SUM(t.amount), 0) AS total_tpv,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount) AS median_check,
        -- Marketing spend calculation
        COALESCE(SUM(ur.qualified_redemption) * ag.reward_amount, 0) AS marketing_spend,
        -- New vs returning users
        COUNT(DISTINCT CASE 
            WHEN uft.first_transaction_date >= ag.overall_start_dt 
                AND uft.first_transaction_date <= ag.overall_end_dt 
            THEN t.user_id 
        END) AS new_users_count,
        COUNT(DISTINCT CASE 
            WHEN uft.first_transaction_date < ag.overall_start_dt 
            THEN t.user_id 
        END) AS returning_users_count,
        -- New user percentage
        CASE 
            WHEN COUNT(DISTINCT t.user_id) > 0 
            THEN ROUND(
                COUNT(DISTINCT CASE 
                    WHEN uft.first_transaction_date >= ag.overall_start_dt 
                        AND uft.first_transaction_date <= ag.overall_end_dt 
                    THEN t.user_id 
                END) * 100.0 / COUNT(DISTINCT t.user_id), 2
            )
            ELSE NULL 
        END AS new_user_percentage,
        -- Group budget tracking
        ag.group_initial_budget,
        ag.restaurant_group_id,
        ag.restaurant_name AS group_restaurant_name,
        -- Notes
        CASE 
            WHEN COUNT(t.user_id) = 0 THEN 'No transactions during activation period'
            ELSE ''
        END AS notes
    FROM all_weeks w
    CROSS JOIN activation_groups ag
    LEFT JOIN LATERAL (
        -- Get transactions during activation periods within this week
        SELECT DISTINCT te.*
        FROM transactions_enhanced te
        CROSS JOIN LATERAL (
            SELECT (period->>'start')::timestamp AS period_start,
                   (period->>'end')::timestamp AS period_end
            FROM JSONB_ARRAY_ELEMENTS(TO_JSONB(ag.activation_periods)) AS period
        ) periods
        WHERE te.match_key = ag.match_key
        AND te.created_at_dt >= GREATEST(periods.period_start, w.week_start)
        AND te.created_at_dt <= LEAST(periods.period_end, w.week_end)
    ) t ON true
    LEFT JOIN user_first_transactions uft 
        ON uft.match_key = ag.match_key 
        AND uft.user_id = t.user_id
    LEFT JOIN user_redemptions ur
        ON ur.match_key = ag.match_key
        AND ur.week_label = w.week_label
        AND ur.user_id = t.user_id
    WHERE ag.overall_start_dt <= w.week_end 
    AND ag.overall_end_dt >= w.week_start
    GROUP BY 
        w.week_label,
        w.week_number,
        ag.activation_id_display,
        ag.restaurant_name,
        ag.location_name,
        ag.description,
        ag.minimum_spend,
        ag.reward_amount,
        ag.overall_start_dt::text,
        ag.overall_end_dt::text,
        ag.group_initial_budget,
        ag.restaurant_group_id
),

-- Calculate total group marketing spend up to each week
group_spend_by_week AS (
    SELECT 
        wa.week,
        wa.restaurant_group_id,
        wa.group_restaurant_name,
        -- Calculate cumulative spend for the group
        SUM(wa.marketing_spend) OVER (
            PARTITION BY wa.restaurant_group_id 
            ORDER BY wa.week
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_group_spend
    FROM weekly_analysis wa
    WHERE wa.restaurant_group_id IS NOT NULL
),

-- Final output with remaining budget calculation
final_results AS (
    SELECT 
        wa.restaurant_name || ' - ' || wa.location_name AS "restaurant name / restaurant location",
        wa.marketing_spend AS "total marketing spend",
        ROUND(COALESCE(wa.group_initial_budget, 0) - COALESCE(gsw.cumulative_group_spend, 0), 2) AS "total remaining budget",
        wa.week
    FROM weekly_analysis wa
    LEFT JOIN group_spend_by_week gsw 
        ON wa.week = gsw.week 
        AND wa.restaurant_group_id = gsw.restaurant_group_id
    WHERE wa.unique_users_count > 0 OR wa.notes != ''
)

-- Output final results
SELECT 
    "restaurant name / restaurant location",
    "total marketing spend",
    "total remaining budget"
FROM final_results
ORDER BY 
    week,
    "restaurant name / restaurant location";
