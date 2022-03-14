class QueryBuilder:

  def __init__(self, config: dict):
    self.config = config

  def create_table(self, location: str):
    columns = []
    for column in self.config['columns']:
      columns.append(f"{column['name']} {column['type']}")
    return (
      'CREATE EXTERNAL TABLE IF NOT EXISTS ' + self.config['table']['name'] + ' '
      ' (' + ','.join(columns) + ') '
      'ROW FORMAT SerDe \'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\' '
      'WITH SerDeProperties ("field.delim" = ",", "escapeChar"="\\\\",  "quoteChar"="\\"") '
      'STORED AS TEXTFILE '
      'LOCATION \'' + location + '\' '
      'TBLPROPERTIES (\'has_encrypted_data\'=\'false\', \'skip.header.line.count\'=\'1\', \'serialization.encoding\'=\'SJIS\') '
    )
