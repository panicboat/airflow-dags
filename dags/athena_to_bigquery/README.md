# s3 to BigQuery

## Variable

```json
{
  "s3": {
    "source": "s3://123456789012-data-lake-source/",
    "raw": "s3://123456789012-data-lake-raw/",
    "intermediate": "s3://123456789012-data-lake-intermediate/",
    "output": "s3://123456789012-data-lake-outputs/"
  }
}
```

## config/table_name.yml

```yml
table:
  header: 1
  separator: ","
  encoding: "UTF8"
columns:
  - name: COLUMN_NAME
    type: DATA_TYPE
  - name: COLUMN_NAME
    type: DATA_TYPE
```

### DATA_TYPE

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/data-types.html
