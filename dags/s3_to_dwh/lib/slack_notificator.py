import re, json, requests

class SlackNotificator:

  def __init__(self, webhook_url: str, username: str, channel: str):
    self.webhook_url = webhook_url
    self.webhook_name = username
    self.channel_name = channel

  def successed(status):
      dag_name = re.findall(r'.*\:\s(.*)\>', str(status['dag']))[0]
      data = {
              'username': self.webhook_name,
              'channel': self.channel_name,
              'attachments': [{
                  'fallback': dag_name,
                  'color': '#1e88e5',
                  'title': dag_name,
                  'text': f'{dag_name} was successed!'
                  }]
              }
      requests.post(self.webhook_url, json.dumps(data))

  def failured(status):
      dag_name = re.findall(r'.*\:\s(.*)\>', str(status['dag']))[0]
      task_name = re.findall(r'.*\:\s(.*)\>', str(status['task']))[0]
      data = {
              'username': self.webhook_name,
              'channel': self.channel_name,
              'attachments': [{
                  'fallback': f'{dag_name}:{task_name}',
                  'color': '#e53935',
                  'title': f'{dag_name}:{task_name}',
                  'text': f'{task_name} was failed...'
                  }]
              }
      requests.post(self.webhook_url, json.dumps(data))
