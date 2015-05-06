__author__ = 'jonathan'

import ConfigParser

class Configuration(object):

    def __init__(self, config_path="/etc/rome/rome.conf"):
        self.config_path = config_path
        self.config = None
        self.load()

    def load(self):
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.config_path)

    def host(self):
        return self.config.get('Rome', 'host')

    def port(self):
        return self.config.getint('Rome', 'port')

    def backend(self):
        return self.config.get('Rome', 'backend')

    def redis_cluster_enabled(self):
        return self.config.getboolean('Cluster', 'redis_cluster_enabled')

    def cluster_nodes(self):
        return self.config.get('Cluster', 'nodes').split(",")

config = None

def build_config():
    return Configuration()

def get_config():
    global config
    if config is None:
        config = build_config()
    return config

if __name__ == '__main__':
    conf = Configuration()
    print(conf.host())
    print(conf.port())
    print(conf.backend())
    print(conf.redis_cluster_enabled())
    print(conf.cluster_nodes())
