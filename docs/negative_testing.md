# Negative Testing Plan

This plan verifies that invalid or unapproved records do not populate the Silver reporting tables. Failed records should be written to the quarantine path with their original input values retained for investigation.

| # | Test case | Inject | Expected behavior |
|---|---|---|---|
| 1 | Negative or zero quantity | Set `quantity` to `-5` or `0` | DQDL/PySpark validation rejects the row and sends it to quarantine. |
| 2 | Missing price | Set `price` to null or an invalid number | The converted `unit_price` becomes null; the row is quarantined and the original `price` value is retained. |
| 3 | Fractional quantity | Set `quantity` to a value such as `573.87` | The row remains valid and is written to reporting data with `is_fractional_qty = true`. |
| 4 | Duplicate order ID | Send the same `order_id` more than once in one batch | Invalid duplicates are quarantined first. Among valid duplicates, the batch keeps one record, preferring the latest `order_date`; equal dates use a deterministic hash tie-breaker. |
| 5 | Unrecognized product | Use a product not listed in `allowed_products` | The row is quarantined and does not create a `dim_product` record. |
| 6 | Unknown branch or manager | Use a city/manager pair not listed in `allowed_branches` | The row is quarantined and does not create a `dim_branch` record. |
| 7 | Date format inconsistency | Use an unsupported date format or invalid date text | Supported formats are parsed; unsupported values become null and are quarantined. The original date value remains available in quarantine. |

## Reporting expectation

The `dim_product`, `dim_branch`, and `fact_sales` tables contain only valid records using approved products and approved city/manager combinations. Raw and quarantined data preserve records that were received but rejected.

## Duplicate handling note

Deduplication applies within the current batch after data-quality validation. Invalid records are quarantined before valid duplicates are reduced to one record. The latest `order_date` is preferred; records with equal dates use a deterministic hash tie-breaker.

The demo assumes one row represents one complete order. If the source changes to one row per order line, `order_id` will no longer be unique. The configuration and fact-table merge must then use a stable `order_line_id` instead, so multiple products belonging to the same order are all retained.
