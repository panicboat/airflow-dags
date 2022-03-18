class QueryBuilder:

  def __init__(self, config: dict):
    self.config = config
    self.database = {
      'r': 'data_lake_raw',
      'i': 'data_lake_intermediate'
    }

  def db_name(self, key: str):
      return self.database[key]

  def drop_table(self):
    return (
      'DROP TABLE IF EXISTS {table_name}'.format(table_name=self.config['table']['name'])
    )

  def create_table_raw(self, prefix: str):
    columns = []
    for column in self.config['columns']:
      columns.append(f"{column['name']} {column['type']}")

    query  = ''
    query += 'CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} '.format(table_name=self.config['table']['name'])
    query += ' ( {columns} ) '.format(columns=','.join(columns))
    query += 'PARTITIONED BY (dt string) '
    query += 'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' '
    query += 'WITH SERDEPROPERTIES ( '
    query += '   "field.delim" = "{delimiter}", '.format(delimiter=self.config['table']['delimiter'])
    query += '   "escapeChar"="\\\\", '
    query += '   "quoteChar"="\\\"" '
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

  def create_table_intermediate(self, partitions: list, dt: str, prefix: str):
    columns = []
    for column in self.config['columns']:
      columns.append(column['name'])

    query  = ''
    query += 'CREATE TABLE IF NOT EXISTS {table_name} '.format(table_name=self.config['table']['name'])
    query += 'WITH ( '

    if 0 < len(partitions):
      partitioned_by = []
      for col in partitions:
        partitioned_by.append('\'{col}\''.format(col=col))
      query += '  partitioned_by = ARRAY[{partitioned_by}], '.format(partitioned_by=','.join(partitioned_by))

    query += '  external_location=\'{prefix}\' '.format(prefix=prefix)
    query += ') '
    query += 'AS '
    query += 'SELECT {columns} '.format(columns=','.join(columns))
    query += 'FROM {database}.{table_name} '.format(database=self.database['r'], table_name=self.config['table']['name'])

    if len(partitions) < 1:
      query += 'WHERE dt = \'{dt}\''.format(dt=dt)

    print(query)
    return query
