import argparse
from shutil import disk_usage
from egcg_core import rest_communication, archive_management as am
from egcg_core.app_logging import logging_default
from egcg_core.exceptions import EGCGError
from config import cfg, load_config
from data_deletion import ProcessedSample, get_file_list_size

logging_default.add_stdout_handler()
logger = logging_default.get_logger(__name__)


def main(argv=None):
    a = argparse.ArgumentParser()
    a.add_argument('action', choices=('check', 'restore'))
    a.add_argument('sample_id')
    args = a.parse_args(argv)
    load_config()

    if args.action == 'check':
        check(args.sample_id)
    elif args.action == 'restore':
        restore(args.sample_id)


def file_states(sample_id):
    s = ProcessedSample(rest_communication.get_document('aggregate/samples', match={'sample_id': sample_id}))
    return {f: sorted(am.archive_states(f)) for f in s.processed_data_files}


def check(sample_id):
    fstates = file_states(sample_id)
    logger.debug('Found %s files', len(fstates))

    restorable_files = []
    unarchived_files = []
    dirty_files = []
    for f in sorted(fstates):
        states = fstates[f]
        if am.is_dirty(f, states):
            target_list = dirty_files
        elif am.is_released(f, states):
            target_list = restorable_files
        else:
            target_list = unarchived_files
        target_list.append(f)

    logger.info(
        'Found %s files: %s restorable (%s Gb), %s dirty (%s Gb), %s unarchived (%s Gb)',
        len(fstates),
        len(dirty_files),
        get_file_list_size(restorable_files) / 1000000000,
        len(unarchived_files),
        get_file_list_size(dirty_files) / 1000000000,
        len(restorable_files),
        get_file_list_size(unarchived_files) / 1000000000
    )
    return restorable_files, unarchived_files, dirty_files


def restore(sample_id):
    if disk_usage(cfg['data_deletion']['delivered_data']).free < 50000000000000:  # TODO: refactor config
        raise EGCGError('Unsafe to recall: less than 50Tb free')

    files_to_restore, files_not_archived, dirty_files = check(sample_id)

    if dirty_files:
        raise EGCGError('Found %s dirty files: %s' % (len(dirty_files), dirty_files))

    if files_not_archived:
        logger.warning(
            'Found %s files not archived. Have they already been restored? %s',
            len(files_not_archived),
            files_not_archived
        )
    if not files_to_restore:
        logger.info('No files to restore found - nothing to do')
        return None

    logger.info('Recalling %s files: %s', len(files_to_restore), files_to_restore)
    for f in files_to_restore:
        am.recall_from_tape(f)

    rest_communication.patch_entry('samples', {'data_deleted': 'none'}, 'sample_id', sample_id)


if __name__ == '__main__':
    main()
