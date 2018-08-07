import argparse
import logging
from egcg_core.app_logging import logging_default as log_cfg

from data_deletion.final_data import FinalDataDeleter
from data_deletion.raw_data import RawDataDeleter
from data_deletion.delivered_data import DeliveredDataDeleter
from config import load_config


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--debug', action='store_true', default=False)
    subparsers = p.add_subparsers()

    for deleter_cls in (RawDataDeleter, DeliveredDataDeleter, FinalDataDeleter):
        subparser = subparsers.add_parser(deleter_cls.alias)
        deleter_cls.add_args(subparser)
        subparser.set_defaults(cls=deleter_cls)

    cmd_args = p.parse_args(argv)
    load_config()
    log_level = logging.DEBUG if cmd_args.debug else logging.INFO
    log_cfg.set_log_level(log_level)
    log_cfg.add_stdout_handler(log_level)

    d = cmd_args.cls(cmd_args)
    d.run()


if __name__ == '__main__':
    main()
