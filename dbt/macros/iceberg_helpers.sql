{% macro iceberg_table_properties() %}
  TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'read.split.target-size' = '134217728'
  )
{% endmacro %}

{% macro get_partition_spec(field, granularity='day') %}
  {% if granularity == 'day' %}
    PARTITIONED BY (days({{ field }}))
  {% elif granularity == 'month' %}
    PARTITIONED BY (months({{ field }}))
  {% elif granularity == 'year' %}
    PARTITIONED BY (years({{ field }}))
  {% elif granularity == 'hour' %}
    PARTITIONED BY (hours({{ field }}))
  {% else %}
    PARTITIONED BY ({{ field }})
  {% endif %}
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
  CASE WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
    THEN {{ default }}
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

{% macro classify_trip_distance(distance_col) %}
  CASE
    WHEN {{ distance_col }} < 1   THEN 'short'
    WHEN {{ distance_col }} < 5   THEN 'medium'
    WHEN {{ distance_col }} < 15  THEN 'long'
    ELSE 'very_long'
  END
{% endmacro %}

{% macro business_hours_flag(hour_col) %}
  CASE WHEN {{ hour_col }} BETWEEN 8 AND 18 THEN TRUE ELSE FALSE END
{% endmacro %}
