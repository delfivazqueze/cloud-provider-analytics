"""Lectura de la configuración externalizada (config/pipeline.example.yaml o config/pipeline.yaml)."""
import os
import re

import yaml


def load_config(repo_root, base=None):
    """Carga el YAML y resuelve ${base}. Si existe config/pipeline.yaml, tiene prioridad sobre el ejemplo."""
    path = os.path.join(repo_root, "config", "pipeline.yaml")
    if not os.path.exists(path):
        path = os.path.join(repo_root, "config", "pipeline.example.yaml")
    with open(path) as f:
        cfg = yaml.safe_load(f)

    if base:
        cfg["paths"]["base"] = base
    b = cfg["paths"]["base"]
    cfg["paths"] = {k: re.sub(r"\$\{base\}", b, v) for k, v in cfg["paths"].items()}
    return cfg
