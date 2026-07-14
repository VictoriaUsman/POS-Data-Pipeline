-- Companion to merge_fct_orders.sql: dedupes/upserts fct_line_items on line_item_id. Recency is
-- taken from the parent order's updated_at (staging_line_items.order_updated_at), since an
-- individual line item has no independent update timestamp of its own -- it changes only when its
-- parent order is re-polled and re-loaded.
--
-- Replace `project.dataset` with the real project/dataset before scheduling.
MERGE `project.dataset.fct_line_items` AS target
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY order_updated_at DESC) AS rn
    FROM `project.dataset.staging_line_items`
  )
  WHERE rn = 1
) AS source
ON target.line_item_id = source.line_item_id
WHEN MATCHED AND source.order_updated_at > target.order_updated_at THEN
  UPDATE SET
    order_id = source.order_id,
    store_id = source.store_id,
    date_key = source.date_key,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_amount = source.discount_amount,
    tax_amount = source.tax_amount,
    total_amount = source.total_amount,
    order_updated_at = source.order_updated_at
WHEN NOT MATCHED THEN
  INSERT (
    line_item_id, order_id, store_id, date_key, quantity, unit_price, discount_amount,
    tax_amount, total_amount, order_updated_at
  )
  VALUES (
    source.line_item_id, source.order_id, source.store_id, source.date_key, source.quantity,
    source.unit_price, source.discount_amount, source.tax_amount, source.total_amount,
    source.order_updated_at
  );
