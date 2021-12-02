from configparser import ConfigParser


def load_config(section):
    config = ConfigParser()
    config.read('config.ini')
    return config[section]
