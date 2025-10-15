from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class Config:
    adapters: list[str]
    macro_output: Path
    scan_project: bool
    model_paths: list[Path]
    project_root: Path


def load_config(config_path: Path) -> Config:
    if not config_path.exists():
        msg = f"Config file not found: {config_path}"
        raise FileNotFoundError(msg)

    with config_path.open() as f:
        data = yaml.safe_load(f)

    if not data:
        msg = "Config file is empty"
        raise ValueError(msg)

    adapters = data.get("adapters", [])
    if not adapters or len(adapters) < 2:
        msg = "At least 2 adapters must be specified"
        raise ValueError(msg)

    project_root = config_path.parent
    macro_output = project_root / data.get("macro_output", "macros/portable_functions.sql")

    model_paths = []
    if data.get("scan_project", True):
        raw_paths = data.get("model_paths", ["models"])
        model_paths = [project_root / p for p in raw_paths]

    return Config(
        adapters=adapters,
        macro_output=macro_output,
        scan_project=data.get("scan_project", True),
        model_paths=model_paths,
        project_root=project_root,
    )
