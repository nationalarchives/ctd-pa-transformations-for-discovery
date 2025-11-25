import os
import json
import yaml
from pathlib import Path
from dotenv import load_dotenv

class UniversalConfig:
    def __init__(self, env_file=".env", yaml_file=None, json_file=None, base_path=None):
        # If base_path not provided, use current working directory
        self.base_path = Path(base_path) if base_path else Path.cwd()

        # Resolve env_file relative to base_path
        env_path = self.base_path / env_file if not Path(env_file).is_absolute() else Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)

        self.yaml_config = self._load_yaml(yaml_file) if yaml_file else {}
        self.json_config = self._load_json(json_file) if json_file else {}

    def _load_yaml(self, file):
        file_path = self.base_path / file if not Path(file).is_absolute() else Path(file)
        with open(file_path) as f:
            return yaml.safe_load(f)

    def _load_json(self, file):
        file_path = self.base_path / file if not Path(file).is_absolute() else Path(file)
        with open(file_path) as f:
            return json.load(f)

    def get(self, key_path, default=None):
        # Check ENV first
        val = os.getenv(key_path)
        if val:
            return val

        # Check YAML nested keys
        if "." in key_path:
            keys = key_path.split(".")
            value = self.yaml_config
            for k in keys:
                value = value.get(k, {})
            if value != {}:
                return value
        elif key_path in self.yaml_config:
            return self.yaml_config[key_path]

        # Check JSON
        if key_path in self.json_config:
            return self.json_config[key_path]

        return default
