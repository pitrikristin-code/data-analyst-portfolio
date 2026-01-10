-- This query creates an analytical order-item level table
-- for business analysis and dashboarding using the
-- Brazilian E-Commerce Public Dataset by Olist.
--
-- The table integrates:
-- - Orders and delivery performance
-- - Order items and revenue allocation
-- - Payments, customers, sellers, and product categories
-- - Aggregated customer review scores


CREATE TABLE analytical_orders AS
WITH order_item_revenue AS (
    SELECT
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.price AS item_price,
        oi.freight_value,
        -- Cast to numeric to avoid integer division during revenue allocation
        SUM(oi.price) OVER (PARTITION BY oi.order_id)::NUMERIC AS total_item_price_per_order
    FROM order_items oi
),
-- Menghitung total payment
payment_per_order AS (
    SELECT
        op.order_id,
        SUM(op.payment_value) AS total_payment_value
    FROM order_payments op
    GROUP BY op.order_id
),
-- Aggregate reviews at order level to avoid duplication
review_per_order AS (
    SELECT 
        order_id,
        AVG(review_score) AS avg_review_score
    FROM order_reviews
    GROUP BY order_id
),
order_date AS (
    SELECT
      	order_id,
        order_status,
        customer_id,
        order_purchase_timestamp,
     	  DATE_TRUNC('month', order_purchase_timestamp::DATE) AS order_month,
        order_delivered_customer_date::DATE,
        order_estimated_delivery_date::DATE,
        (order_delivered_customer_date::DATE  - order_purchase_timestamp::DATE ) AS delivery_time_days,
      	(order_delivered_customer_date::DATE  - order_estimated_delivery_date::DATE)AS delivery_delay_days
    FROM orders
    WHERE order_status = 'delivered'
)
SELECT o.*,
	-- Membuat kategori delivery time (Delivery Bucket)	
	CASE
   		WHEN delivery_delay_days > 7 THEN 'Late > 7 days'
    	WHEN delivery_delay_days > 0 THEN 'Late ≤ 7 days'
    	WHEN delivery_delay_days = 0 THEN 'On time'
    	ELSE 'Early'
	END AS delivery_bucket,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    p.product_id,
    pct.product_category_name_english AS product_category,
    s.seller_id,
    s.seller_state,
    -- Menggunakan hasil rata-rata review
    r.avg_review_score AS review_score,
    oi.order_item_id,
    oi.item_price,
    oi.freight_value,
    -- Revenue Allocation
    CASE
        WHEN oi.total_item_price_per_order > 0 THEN
            (oi.item_price::NUMERIC / oi.total_item_price_per_order) * pp.total_payment_value
        ELSE 0
    END AS allocated_revenue,
    1 AS order_item_count
FROM order_date o
LEFT JOIN order_item_revenue oi ON o.order_id = oi.order_id
LEFT JOIN payment_per_order pp ON o.order_id = pp.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON oi.product_id = p.product_id
LEFT JOIN translation pct ON p.product_category_name = pct.product_category_name
LEFT JOIN seller s ON oi.seller_id = s.seller_id
LEFT JOIN review_per_order r ON o.order_id = r.order_id 
;

