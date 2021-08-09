--Ranking the rows for the latest custom pricing for customer and payment method
WITH ranked_custom_pricing AS
    (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY customer_id, payment_method_id ORDER BY starts_at DESC, custom_pricing_id DESC) AS rank_custom_pricing_row
        FROM `mollie_assignment_2.custom_pricing_2b` AS custom_pricing
    )
--Joining the ranked custom pricing table to the raw payments table to find if a custom pricing was active for that payment
   ,joined_custom_pricing AS
    (
        SELECT payments.*,
               custom_pricing.* except(customer_id,rank_custom_pricing_row,payment_method_id,custom_pricing_id,starts_at,ends_at  )
        FROM `mollie-assignment.mollie_assignment_2.payments` payments
                 LEFT JOIN ranked_custom_pricing custom_pricing
                           ON payments.customer_id = custom_pricing.customer_id
                               AND payments.payment_method_id = custom_pricing.payment_method_id
                               AND payments.payment_date >= custom_pricing.starts_at
                               AND (
                                          payments.payment_date <= custom_pricing.ends_at
                                      OR
                                          (custom_pricing.ends_at IS NULL AND custom_pricing.rank_custom_pricing_row =1)
                                  )
    )
--Ranking the rows for the latest default pricing for payment method
   ,ranked_default_pricing AS
    (
        SELECT default_pricing_id ,
               payment_method_id,
               starts_at AS default_starts_at,
               ends_at AS default_ends_at,
               fixed_rate AS default_fixed_rate,
               variable_rate AS default_variable_rate,
               ROW_NUMBER() OVER(PARTITION BY payment_method_id ORDER BY starts_at DESC, default_pricing_id DESC) AS rank_default_pricing_row
        FROM `mollie_assignment_2.default_pricing` AS default_pricing
    )
--Joining the ranked default pricing table to the ranked custom pricing table to find if a default pricing was active for that payment method if custom pricing was unavailable
   ,joined_default_pricing AS
    (
        SELECT joined_custom_pricing.*,
               ranked_default_pricing.* except(rank_default_pricing_row,payment_method_id,default_pricing_id ,default_starts_at,default_ends_at)
        FROM joined_custom_pricing joined_custom_pricing
                 LEFT JOIN ranked_default_pricing ranked_default_pricing
                           ON  joined_custom_pricing.payment_method_id = ranked_default_pricing.payment_method_id
                               AND joined_custom_pricing.payment_date >= ranked_default_pricing.default_starts_at
                               AND (
                                           joined_custom_pricing.payment_date <= ranked_default_pricing.default_ends_at
                                       OR
                                           (ranked_default_pricing.default_ends_at IS NULL AND ranked_default_pricing.rank_default_pricing_row =1)
                                   )
    )
   ,rates_calculated AS
    (
        SELECT * EXCEPT(fixed_rate,variable_rate,default_fixed_rate,default_variable_rate),
               CASE WHEN fixed_rate IS NOT NULL THEN fixed_rate
                    WHEN fixed_rate IS NULL THEN default_fixed_rate ELSE -99 END AS fixed_rate,
               CASE WHEN variable_rate IS NOT NULL THEN variable_rate
                    WHEN variable_rate IS NULL THEN default_variable_rate ELSE -99 END AS variable_rate,
        FROM joined_default_pricing
    )
--Calculating Total Transactions by that customer, on each day for each payment method
   , total_transactions_calculated AS
    (
        SELECT customer_id,
               payment_method_id,
               payment_date,
               COUNT(DISTINCT payment_id) AS total_transactions
        FROM `mollie-assignment.mollie_assignment_2.payments` payments
        GROUP BY 1,2,3
    )
-- Calculating average volume per payment
   ,volume_calculated AS
    (
        SELECT rates_calculated.*,
               total_transactions_calculated.total_transactions,
               ROUND(total_volume/NULLIF(total_transactions,0),2) AS avg_volume_per_payment
        FROM rates_calculated AS rates_calculated
                 LEFT JOIN total_transactions_calculated AS total_transactions_calculated
                           USING(customer_id,payment_method_id,payment_date)
    )
--Calculating fees
SELECT *,
       (fixed_rate*1) AS total_fixed_fee,
       round((variable_rate*avg_volume_per_payment),2) AS total_variable_fee,
       round((fixed_rate*1)+(variable_rate*avg_volume_per_payment),2) AS total_fee
FROM volume_calculated
