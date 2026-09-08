# External JSON Configuration Specification (`sales_config.json`)

The ETL framework reads a central JSON configuration to execute column transformations, apply data-quality assertions, and map star-schema outputs dynamically.

## Configuration schema

### `columns`

Maps raw incoming column names to Silver target names, data types, and formatting masks.

- `target_name`: Destination Silver column.
- `type`: Target PySpark type such as `string`, `integer`, `double`, `date`, or `timestamp`.
- `format`: Date or timestamp parsing pattern, or an ordered list of patterns.

### `deduplication`

- `primary_keys`: Columns used to determine uniqueness.
- `order_by`: Column used to select the latest record within a duplicate group.

The demo uses `order_id` because the dataset has one row per order. For order-line data, use a stable `order_line_id`.

### `data_quality`

- `not_null`: Columns that must not be null.
- `greater_than_zero`: Numeric columns that must be strictly greater than zero.
- `allowed_products`: Optional list of products permitted in reporting data.
- `allowed_branches`: Optional list of approved `city` and `manager` combinations.

Rejected records retain their original input columns in quarantine. Fractional quantities remain valid and are marked with `is_fractional_qty`.

### `dimensions`

Maps each dimension to the natural-key columns passed to `generate_surrogate_key()`.

### `fact_table_name`, `fact_columns`, and `partition_by`

Define the target fact table, its payload columns, and the Iceberg partition columns.

## Example payload

See [`config/sales_config.json`](../config/sales_config.json) for the complete working configuration used by the workshop.
