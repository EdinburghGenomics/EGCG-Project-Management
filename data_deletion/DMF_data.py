import os
import errno
import subprocess

from egcg_core.config import cfg

from data_deletion import Deleter


class DMFDtatDeleter(Deleter):
    alias = 'dmf_deletion'

    def __init__(self, cmd_args):
        super().__init__(cmd_args)
        self.dmf_file_system = cfg['data_deletion']['dmf_filesystem']
        self.lustre_file_system = cfg['data_deletion']['lustre_file_system']
        self.file_checked = 0
        if not os.path.exists(self.dmf_file_system):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.dmf_file_system)
        if not os.path.exists(self.lustre_file_system):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.lustre_file_system)

    @staticmethod
    def _get_cmd_output(cmd):
        p = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        exit_status = p.wait()
        o, e = p.stdout.read(), p.stderr.read()
        p.stdout.close()
        p.stderr.close()
        return exit_status, o, e

    def _has_no_lustre_path(self, fid):
        exit_status, stdout, stderr = self._get_cmd_output('lfs fid2path %s %s' % (self.lustre_file_system, fid))
        if exit_status == 2 and stdout == b'' and b'No such file or directory' in stderr:
            return True
        return False

    def find_files_to_delete(self):
        file_to_delete = []
        for path, subdirs, files in os.walk(self.dmf_file_system):
            for name in files:
                self.file_checked += 1
                # the name of files in DMF filesystem are fids
                if self._has_no_lustre_path(name):
                    file_to_delete.append(os.path.join(path, name))
        return file_to_delete

    def delete_data(self):
        files_to_delete = self.find_files_to_delete()
        self.info('Checked %s files and found %s orphan files for deletion in %s',
                  self.file_checked, len(files_to_delete), self.dmf_file_system)
        if not files_to_delete or self.dry_run:
            return 0

        # Actually remove the files
        for f in files_to_delete:
            self.debug('Remove %s', f)
            os.remove(f)
