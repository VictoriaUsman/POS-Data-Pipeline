-- Companion to merge_fct_orders.sql: dedupes/upserts fct_payments on payment_id. Recency is taken
-- from the parent order's updated_at (staging_payments.order_updated_at), same rationale as
-- merge_fct_line_items.sql -- a payment has no independent update timestamp of its own.
--
-- Replace `project.dataset` with the real project/dataset before scheduling.
MERGE `project.dataset.fct_payments` AS target
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY order_updated_at DESC) AS rn
    FROM `project.dataset.staging_payments`
  )
  WHERE rn = 1
) AS source
ON target.payment_id = source.payment_id
WHEN MATCHED AND source.order_updated_at > target.order_updated_at THEN
  UPDATE SET
    order_id = source.order_id,
    store_id = source.store_id,
    date_key = source.date_key,
    tender_type = source.tender_type,
    amount = source.amount,
    tip_amount = source.tip_amount,
    order_updated_at = source.order_updated_at
WHEN NOT MATCHED THEN
  INSERT (
    payment_id, order_id, store_id, date_key, tender_type, amount, tip_amount, order_updated_at
  )
  VALUES (
    source.payment_id, source.order_id, source.store_id, source.date_key, source.tender_type,
    source.amount, source.tip_amount, source.order_updated_at
  );
