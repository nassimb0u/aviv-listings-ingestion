import yaml
import os
from pathlib import Path
import json
from dotenv import load_dotenv


class PipelineConfig:
    def __init__(self, d: dict, dotenv_path: str = ".env"):
        load_dotenv(dotenv_path, override=False)
        self.raw = d
        self.spark = d.get("spark", {})
        self.input = d.get("input", {})
        self.db = d.get("db", {})
        self.transform = d.get("transform", {})
        self.run = d.get("run", {})
        # inject env secrets
        self.db["host"] = os.environ["DB_HOST"]
        self.db["port"] = os.environ["DB_PORT"]
        self.db["name"] = os.environ["DB_NAME"]
        self.db["user"] = os.environ["DB_USER"]
        self.db["password"] = os.environ["DB_PASSWORD"]

    def __repr__(self):
        return json.dumps(self.raw, indent=4)


def load_config(path: str = "config.yaml") -> PipelineConfig:
    with open(Path(path), "r") as f:
        data = yaml.safe_load(f)
    return PipelineConfig(data)


if __name__ == "__main__":
    config = load_config()
    print(config)
