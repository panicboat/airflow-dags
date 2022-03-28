# s3 to DWH

## Variable

### s3_to_dwh

```json
{
  "s3": {
    "source": "data-lake-123456789012-source",
    "raw": "data-lake-123456789012-raw",
    "intermediate": "data-lake-123456789012-intermediate",
    "output": "data-lake-123456789012-outputs"
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
  partition: []
columns:
  - name: COLUMN_NAME
    type: DATA_TYPE
  - name: COLUMN_NAME
    type: DATA_TYPE
```

### DATA_TYPE

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/data-types.html
