import json
import os


class ConfigError(RuntimeError):
    pass


class Config:
    def __init__(self, data=None, cores_count=None, system_name = None):
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                try:
                    with open(data, 'r') as f:
                        data = json.loads(f.read())
                except Exception:
                    raise ConfigError("Can't load config!")

        elif data is None:
            data = {}

        self.system_name = system_name or data.get('system_name', 'system')
