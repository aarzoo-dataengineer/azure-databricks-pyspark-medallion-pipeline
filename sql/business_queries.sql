-- Run these examples after registering the Gold Delta outputs as tables/views.
-- Example local/Databricks SQL after creating a table named gold_daily_sales:

-- 1. Revenue by day
SELECT order_date, total_orders, units_sold, total_revenue
FROM gold_daily_sales
ORDER BY order_date;

-- 2. Best-selling products by revenue
SELECT product_name, category, units_sold, total_revenue
FROM gold_product_sales
ORDER BY total_revenue DESC;

-- 3. Highest-value customers
SELECT customer_id, customer_name, city, lifetime_value
FROM gold_customer_sales
ORDER BY lifetime_value DESC;
