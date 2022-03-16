class QueryBuilder:

  def __init__(self, config: dict):
    self.config = config
    self.database = {
      'r': 'data_lake_raw',
      'i': 'data_lake_intermediate'
    }

  def get_db_name(self, key: str):
      return self.database[key]

  def drop_table(self):
    return (
      'DROP TABLE IF EXISTS ' + self.config['table']['name']
    )

  def create_table_raw(self, location: str):
    columns = []
    for column in self.config['columns']:
      columns.append(f"{column['name']} {column['type']}")
    return (
      'CREATE EXTERNAL TABLE IF NOT EXISTS ' + self.config['table']['name'] + ' '
      ' (' + ','.join(columns) + ') '
      'PARTITIONED BY (dt string) '
      'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\' '
      'WITH SERDEPROPERTIES ( '
      '   "field.delim" = "' + self.config['table']['delimiter'] + '", '
      '   "escapeChar"="\\\\", '
      '   "quoteChar"="\\\"" '
      ') '
      'STORED AS TEXTFILE '
      'LOCATION \'' + location + '\' '
      'TBLPROPERTIES ('
      '   \'has_encrypted_data\'=\'false\', '
      '   \'skip.header.line.count\'=\'' + str(self.config['table']['header']) + '\', '
      '   \'serialization.encoding\'=\'' + self.config['table']['encoding'] + '\' '
      ') '
    )

  def create_table_intermediate(self, location: str):
    columns = []
    for column in self.config['columns']:
      columns.append(column['name'])
    query = (
      'CREATE TABLE IF NOT EXISTS ' + self.config['table']['name'] + ' '
      'WITH ( '
      '     format=\'PARQUEST\', '
      '     external_location=\'' + location + '\', '
      '     partitioned_by = ARRAY[\'dt\'] '
      '     ) '
      'AS SELECT ' + ','.join(columns) + ', dt FROM ' + self.database['r'] + '.' + self.config['table']['name'] + ' '
    )
    print(query)
    return query
