-- Upserts newly-landed orders into the curated fct_orders fact table, deduping on order_id and
-- keeping only the newest version when the same order was polled (and therefore loaded) more than
-- once within the lookback window.
--
-- Run as a BigQuery scheduled query, triggered after each BigQuery Data Transfer Service run
-- against `staging_orders` completes (DTS loads S3 silver/ straight into staging, append-only --
-- see README "Curated Warehouse: DECIDED -- BigQuery"). staging_orders carries the same columns
-- as fct_orders plus `updated_at`, which this merge uses for recency and does not persist onward.
--
-- Replace `project.dataset` with the real project/dataset before scheduling.
MERGE `project.dataset.fct_orders` AS target
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM `project.dataset.staging_orders`
  )
  WHERE rn = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
    store_id = source.store_id,
    date_key = source.date_key,
    subtotal_amount = source.subtotal_amount,
    discount_amount = source.discount_amount,
    tax_amount = source.tax_amount,
    total_amount = source.total_amount,
    line_item_count = source.line_item_count,
    payment_count = source.payment_count,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    order_id, store_id, date_key, subtotal_amount, discount_amount, tax_amount, total_amount,
    line_item_count, payment_count, updated_at
  )
  VALUES (
    source.order_id, source.store_id, source.date_key, source.subtotal_amount,
    source.discount_amount, source.tax_amount, source.total_amount, source.line_item_count,
    source.payment_count, source.updated_at
  );
