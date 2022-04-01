import yaml
from pathlib import Path

class ConfigLoader:

  def __init__(self, yml: str):
    self.yml = yml
    self.table = Path(yml).stem
    with open(yml, 'r') as yml:
      self.config = yaml.safe_load(yml)

    self.config['table']['origin'] = self.config['table']['name']

    if 'prefix' in self.config['table'] and 0 < len(self.config['table']['prefix'].split('/')):
      table_name = self.config['table']['name']
      table_prefix = (self.config['table']['prefix'].split('/')[0]).replace('-', '_')
      self.config['table']['name'] = '{table_prefix}_{table_name}'.format(table_prefix=table_prefix, table_name=table_name)

  def load(self):
    return self.config
