import os
import sys
import logging
import argparse
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from upload_to_gel.deliver_data_to_gel import GelDataDelivery, check_all_deliveries, report_all


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--work_dir', type=str, required=True)
    group = p.add_mutually_exclusive_group()
    group.add_argument('--sample_id', type=str)
    group.add_argument('--user_sample_id', type=str)
    p.add_argument('--force_new_delivery', action='store_true')
    p.add_argument('--no_cleanup', action='store_true')
    p.add_argument('--check_all_md5sums', action='store_true')
    p.add_argument('--report', action='store_true')

    args = p.parse_args()
    load_config()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
        log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))

    if args.check_all_md5sums:
        check_all_deliveries(args.work_dir)
    elif args.report:
        report_all()
    else:
        gel_delivery = GelDataDelivery(args.work_dir, sample_id=args.sample_id, user_sample_id=args.user_sample_id,
                                       no_cleanup=args.no_cleanup, dry_run=args.dry_run, force_new_delivery=args.force_new_delivery)
        gel_delivery.deliver_data()


if __name__ == '__main__':
    sys.exit(main())
