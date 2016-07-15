import os
from egcg_core.config import cfg

default_search_path = [
    os.getenv('PROJECTMANAGEMENTCONFIG'),
    os.path.expanduser('~/.project_management.yaml')
]


def load_config(*config_files):
    if not config_files:
        config_files = default_search_path
    for f in config_files:
        if f and os.path.isfile(f):
            cfg.load_config_file(f)
    # cfg.load_config_file(search_path, env_var='PROJECTMANAGEMENTENV')
