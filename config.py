import os
from egcg_core.config import cfg


def load_config():
    cfg.load_config_file(
        os.getenv('PROJECTMANAGEMENTCONFIG'),
        os.path.expanduser('~/.project_management.yaml'),
        env_var='PROJECTMANAGEMENTENV'
    )
