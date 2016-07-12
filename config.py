import os
from egcg_core.config import EnvConfiguration


default = EnvConfiguration(
    [
        os.getenv('PROJECTMANAGEMENTCONFIG'),
        os.path.expanduser('~/.project_management.yaml'),
        os.path.join(os.path.dirname(__file__), 'etc', 'example_project_management.yaml')
    ],
    env_var='PROJECTMANAGEMENTENV'
)

