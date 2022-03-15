# s3 to BigQuery

## Variable

### athena_to_bigquery

```json
{
  "s3": {
    "source": "123456789012-data-lake-source",
    "raw": "123456789012-data-lake-raw",
    "intermediate": "123456789012-data-lake-intermediate",
    "output": "123456789012-data-lake-outputs"
  }
}
```

## Connections

### aws_default

```json
{
  "aws_access_key_id": "",
  "aws_secret_access_key": "",
  "region_name": "ap-northeast-1"
}
```


## config/table_name.yml

```yml
table:
  header: 1
  delimiter: ","
  encoding: "UTF8"
columns:
  - name: COLUMN_NAME
    type: DATA_TYPE
  - name: COLUMN_NAME
    type: DATA_TYPE
```

### DATA_TYPE

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/data-types.html
