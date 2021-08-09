-- All the old rates are added to the updates chronologically using window function
WITH old_rates_added AS
         (
             SELECT customer_id,
                    payment_method_id,
                    starts_at     AS pricing_updated_at,
                    fixed_rate    AS new_fixed_rate,
                    variable_rate AS new_variable_rate,
                    LEAD(fixed_rate,1)    OVER(PARTITION BY customer_id,payment_method_id ORDER BY starts_at DESC,custom_pricing_id	DESC) AS old_fixed_rate,
                     LEAD(variable_rate,1) OVER(PARTITION BY customer_id,payment_method_id ORDER BY starts_at DESC,custom_pricing_id	DESC) AS old_variable_rate
             FROM `mollie-assignment.mollie_assignment_2.custom_pricing`
         )
-- Remove the first rows or rows which doesn't have an update in any of the rates yet
SELECT *
FROM old_rates_added
WHERE old_fixed_rate IS NOT NULL
  AND old_variable_rate IS NOT NULL
order by 1 ASC,2 ASC,3 DESC