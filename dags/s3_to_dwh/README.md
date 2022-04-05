# s3 to DWH

## Variable

### s3_to_dwh

```json
{
  "s3": {
    "source": "data-lake-123456789012-source",
    "raw": "data-lake-123456789012-raw",
    "intermediate": "data-lake-123456789012-intermediate",
    "structuralization": "data-lake-123456789012-structuralization",
    "finalize": "data-lake-123456789012-finalize",
    "output": "data-lake-123456789012-outputs"
  },
  "gcs": {
    "destination" : ""
  },
  "bigquery": {
    "project": "",
    "dataset": "",
    "location": "asia-northeast1"
  }
}
```

### slack

```json
{
  "webhook_url": "",
  "username": "",
  "channel": ""
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

### google_cloud_default

```json
{
  "type": "service_account",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": ""
}
```

Scope : `https://www.googleapis.com/auth/cloud-platform`

## config/table_name.yml

```yml
table:
  name: table_name
  header: 0
  delimiter: ","
  escape: "\\\\"
  encoding: UTF8
  prefix: prefix/path/
  partition: []
  mode: WRITE_TRUNCATE
columns:
  - name: id
    type: int
    hash: true
  - name: created_at
    type: timestamp
    timezone: Asia/Tokyo
```

### DATA_TYPE

https://docs.aws.amazon.com/ja_jp/athena/latest/ug/data-types.html
