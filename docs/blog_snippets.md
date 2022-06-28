```yaml
checks for CUSTOMERS:
  - count same as RAW_CUSTOMERS in other_snowflake_data_source
```

```yaml
checks for CUSTOMERS:
  - freshness using row_added_ts < 1h
```

```yaml
for each dataset T1:
  datasets:
    - CUST_%
  checks:
      - anomaly score for row_count < default:
          name: ${T1} row count anomaly detection
```

```yaml
checks for DIM_CUSTOMER:
  - change avg last 7 days for row_count between -10 and +50

checks for DIM_ORDERS:
  - change max last 7 days percent for row_count:
      warn: when < 40%
      fail: when < 50%
```
