import yaml
from pathlib import Path

class ConfigLoader:

  def __init__(self, yml: str):
    self.yml = yml
    self.table = Path(yml).stem
    with open(yml, 'r') as yml:
      self.config = yaml.safe_load(yml)

  def load(self):
    return self.config
