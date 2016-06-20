import os
from egcg_core.config import EnvConfiguration


default = EnvConfiguration(
    [
        os.getenv('DATADELETIONCONFIG'),
        os.path.expanduser('~/.data_deletion.yaml'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'example_data_deletion.yaml')
    ]
)
