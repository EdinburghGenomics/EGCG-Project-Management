from os.path import expanduser
import argparse
import logging
from egcg_core.app_logging import logging_default as log_cfg
from data_deletion.raw_data import RawDataDeleter
from data_deletion.fastq import FastqDeleter
from data_deletion.expired_data import DeliveredDataDeleter

from config import load_config



def main():
    deleters = {
        'raw': RawDataDeleter,
        'fastq': FastqDeleter,
        'delivered_data': DeliveredDataDeleter
    }

    p = argparse.ArgumentParser()
    p.add_argument('deleter', type=str, choices=deleters.keys())
    p.add_argument('--debug', action='store_true', default=False)
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--work_dir', default=expanduser('~'))
    p.add_argument('--deletion_limit', type=int, default=None)
    p.add_argument('--project_id', type=str)
    p.add_argument('--manual_delete', type=str, nargs='+')
    p.add_argument('--sample_ids', nargs='+', default=[])
    args = p.parse_args()

    load_config()

    if args.__dict__.pop('debug'):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    log_cfg.set_log_level(log_level)
    log_cfg.add_stdout_handler(log_level)

    deleter_type = args.__dict__.pop('deleter')
    deleter_args = dict([(k, v) for k, v in args.__dict__.items() if v])

    d = deleters[deleter_type](**deleter_args)
    d.delete_data()


if __name__ == '__main__':
    main()
