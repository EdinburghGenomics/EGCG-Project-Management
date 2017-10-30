import os
import sys
import argparse
from egcg_core.archive_management import is_released, is_archived

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


class SpaceUsed():
    def __init__(self, directory):
        self.dir_to_explore = directory
        self.data_on_lustre = {}
        self.data_on_tape = {}
        self.dig_through(self.dir_to_explore)

    def dig_through(self, directory):
        for d in os.listdir(directory):
            p = os.path.join(directory, d)
            if os.path.islink(p):
                continue
            if os.path.isdir(p):
                self.dig_through(p)
            elif os.path.isfile(p):
                stat = os.stat(p)
                if is_archived(p) and is_released(p):
                    self.data_on_tape[p] = stat.st_size
                else:
                    self.data_on_lustre[p] = stat.st_size

    def summarise(self):
        print('For directory ' + self.dir_to_explore)
        print('Data on lustre takes: %s (%s)' % (sizeof_fmt(sum(self.data_on_lustre.values())), sum(self.data_on_lustre.values())))
        print('Data on tape takes: %s (%s)' % (sizeof_fmt(sum(self.data_on_tape.values())), sum(self.data_on_tape.values())))


def main():
    args = _parse_args()
    sp = SpaceUsed(args.directory)
    sp.summarise()

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-d', '--directory', dest='directory', type=str)
    p.add_argument('--debug', action='store_true', help='override pipeline log level to debug')
    return p.parse_args()


if __name__ == '__main__':
    sys.exit(main())
