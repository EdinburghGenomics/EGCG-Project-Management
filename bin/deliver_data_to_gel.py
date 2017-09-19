import argparse
import logging
import sys
import os
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from upload_to_gel.deliver_data_to_gel import GelDataDelivery

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--work_dir', type=str, required=True)
    p.add_argument('--project_id', type=str, required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--sample_id', type=str)
    group.add_argument('--user_sample_id', type=str)
    args = p.parse_args()

    load_config()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
        log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))

    gel_delivery = GelDataDelivery(args.work_dir, args.project_id, args.sample_id, args.user_sample_id, args.dry_run)
    gel_delivery.deliver_data()


if __name__ == '__main__':
    sys.exit(main())
