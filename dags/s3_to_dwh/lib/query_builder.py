class QueryBuilder:

  def __init__(self, config: dict):
    self.config = config
    self.database = {
      'r': 'data_lake_raw',
      'i': 'data_lake_intermediate',
      's': 'data_lake_structuralization',
      'f': 'data_lake_finalize',
    }

  def db_name(self, key: str):
      return self.database[key]

  def drop_table(self):
    return (
      'DROP TABLE IF EXISTS {table_name}'.format(table_name=self.config['table']['name'])
    )

  def create_table(self, prefix: str):
    columns = []
    for column in self.config['columns']:
      columns.append(f"{column['name']} string")

    query  = ''
    query += 'CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} '.format(table_name=self.config['table']['name'])
    query += ' ( {columns} ) '.format(columns=','.join(columns))
    query += 'PARTITIONED BY (dt string) '
    query += 'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' '
    query += 'WITH SERDEPROPERTIES ( '
    query += '   "field.delim" = "{delimiter}", '.format(delimiter=self.config['table']['delimiter'])
    query += '   "escapeChar" = "{escapeChar}", '.format(escapeChar=self.config['table']['escape'])
    query += '   "quoteChar" = "\\\"" '
    query += ') '
    query += 'STORED AS TEXTFILE '
    query += 'LOCATION \'{prefix}\' '.format(prefix=prefix)
    query += 'TBLPROPERTIES ('
    query += '   \'has_encrypted_data\'=\'false\', '
    query += '   \'skip.header.line.count\'=\'{header}\', '.format(header=str(self.config['table']['header']))
    query += '   \'serialization.encoding\'=\'{encoding}\' '.format(encoding=self.config['table']['encoding'])
    query += ') '

    print(query)
    return query

  def ctas_parquet(self, source: str, partitions: list, dt: str, prefix: str):
    columns = []
    for column in self.config['columns']:
      columns.extend(self.__try_cast(column))

    query  = ''
    query += 'CREATE TABLE IF NOT EXISTS {table_name} '.format(table_name=self.config['table']['name'])
    query += 'WITH ( '

    if 0 < len(partitions):
      partitioned_by = []
      for col in partitions:
        partitioned_by.append('\'{col}\''.format(col=col))
      query += '  partitioned_by = ARRAY[{partitioned_by}], '.format(partitioned_by=','.join(partitioned_by))

    query += '  format = \'PARQUET\','
    query += '  external_location=\'{prefix}\' '.format(prefix=prefix)
    query += ') '
    query += 'AS '
    query += 'SELECT {columns} '.format(columns=','.join(columns))
    query += 'FROM {source}.{table_name} '.format(source=source, table_name=self.config['table']['name'])

    if len(partitions) < 1:
      query += 'WHERE dt = \'{dt}\''.format(dt=dt)

    print(query)
    return query

  def ctas(self, source: str, location: str):
    query  = ''
    query += 'CREATE TABLE IF NOT EXISTS {table_name} '.format(table_name=self.config['table']['name'])
    query += 'WITH ( '
    query += '  format = \'PARQUET\','
    query += '  external_location=\'{location}\' '.format(location=location)
    query += ') '
    query += 'AS '
    query += 'SELECT * FROM {source}.{table_name} '.format(source=source, table_name=self.config['table']['name'])

    print(query)
    return query

  def __try_cast(self, column: dict):
    if column['type'] == 'timestamp':
      col = 'DATE_FORMAT(FROM_UNIXTIME(TO_UNIXTIME(CAST({name} || \' {timezone}\' AS TIMESTAMP)), \'Asia/Tokyo\'), \'%Y-%m-%d %H:%i:%s\')'.format(name=column['name'], timezone=column['timezone'])
      return [
        'IF(upper({name}) = \'NULL\', {default}, {col}) as {name}'.format(name=column['name'], default='Null', col=col)
      ]

    col = 'IF(upper({name}) = \'NULL\', {default}, {name})'.format(name=column['name'], default='Null')

    if column['type'] == 'string':
      return [
        '{col} as {name}'.format(col=col, name=column['name'])
      ]

    try_cast_columns = [
      'try_cast({col} as {type}) as {name}'.format(col=col, type=column['type'], name=column['name'])
    ]

    if 'hash' in column and column['hash']:
      try_cast_columns.append(
        'IF(upper({name}) = \'NULL\', {default}, TO_HEX(SHA256(TO_UTF8({name})))) as {name}_hash'.format(name=column['name'], default='Null')
      )

    return try_cast_columns
