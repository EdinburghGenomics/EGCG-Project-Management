import os
import sys
import argparse
from egcg_core.app_logging import logging_default as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from upload_to_gel.deliver_data_to_gel import GelDataDelivery, check_all_deliveries, report_all


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--work_dir', type=str)
    group = p.add_mutually_exclusive_group()
    group.add_argument('--sample_id', type=str)
    group.add_argument('--user_sample_id', type=str)
    p.add_argument('--force_new_delivery', action='store_true')
    p.add_argument('--no_cleanup', action='store_true')
    p.add_argument('--check_all_deliveries', action='store_true')
    p.add_argument('--report', action='store_true')

    args = p.parse_args()
    load_config()
    log_cfg.add_stdout_handler()

    if args.debug:
        log_cfg.set_log_level(10)

    if args.check_all_deliveries:
        check_all_deliveries()
    elif args.report:
        report_all()
    else:
        gel_delivery = GelDataDelivery(args.sample_id, user_sample_id=args.user_sample_id, work_dir=args.work_dir,
                                       dry_run=args.dry_run, no_cleanup=args.no_cleanup,
                                       force_new_delivery=args.force_new_delivery)
        gel_delivery.deliver_data()


if __name__ == '__main__':
    sys.exit(main())
