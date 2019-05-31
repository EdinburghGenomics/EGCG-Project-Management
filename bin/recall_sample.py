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
    s = ProcessedSample(rest_communication.get_document('samples', quiet=True,  where={'sample_id': sample_id}))
    return {f: sorted(am.archive_states(f)) for f in s.raw_data_files + s.processed_data_files}


def check(sample_id):
    fstates = file_states(sample_id)
    logger.debug('Found %s files', len(fstates))

    restorable_files = []
    unreleased_files = []
    unarchived_files = []
    dirty_files = []
    for f in sorted(fstates):
        states = fstates[f]
        if am.is_dirty(f, states):
            dirty_files.append(f)
        elif am.is_released(f, states):
            restorable_files.append(f)
        elif am.is_archived(f, states):
            unreleased_files.append(f)
        else:
            unarchived_files.append(f)

    msg_parts = [
        '%s %s (%s Gb)' % (len(l), name, get_file_list_size(l) / 1000000000)
        for name, l in (
            ('restorable', restorable_files), ('unreleased', unreleased_files),
            ('unarchived', unarchived_files), ('dirty', dirty_files)
        )


    ]
    logger.info('Found %s files: %s', len(fstates), ', '.join(msg_parts))
    return restorable_files, unreleased_files, unarchived_files, dirty_files


def restore(sample_id):
    if disk_usage(cfg['delivery']['dest']).free < 50000000000000:
        raise EGCGError('Unsafe to recall: less than 50Tb free')

    files_to_restore, files_not_released, files_not_archived, dirty_files = check(sample_id)

    if dirty_files or files_not_archived:
        logger.error('Found %s dirty files: %s', len(dirty_files), dirty_files)
        logger.error('Found %s files not archived: %s', len(files_not_archived), files_not_archived)
        raise EGCGError('Found %s dirty, %s unarchived files' % (len(dirty_files), len(files_not_archived)))

    if files_not_released:
        logger.warning('Found %s files not released: %s', len(files_not_released), files_not_released)

    if not files_to_restore:
        logger.info('No files to restore found - nothing to do')
        return None

    logger.info('Recalling %s files for sample %s', len(files_to_restore), sample_id)
    for f in files_to_restore:
        am.recall_from_tape(f)

    rest_communication.patch_entry('samples', {'data_deleted': 'none'}, 'sample_id', sample_id)


if __name__ == '__main__':
    main()
