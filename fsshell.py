import cmd
import re
import shlex
import argparse
import file_system


class FsShell(cmd.Cmd):
    def __init__(self, fs):
        super().__init__()
        self._fs = fs
        self._current_inode = self._fs.root_inode
        self._last_inode = self._fs.root_inode
        self.change_directory(self._fs.root_inode)

        self._ls_arg_parser = argparse.ArgumentParser(
            add_help=False, prog='ls', description='list directory contents')
        self._ls_arg_parser.add_argument(
            '-l', action='store_true', dest='long', default=False, help='list files in the long format')
        self._ls_arg_parser.add_argument(
            '-h', action='store_true', dest='human', default=False, help='show human-readable size')
        self._ls_arg_parser.add_argument(
            '-a', action='store_true', dest='hidden', default=False, help='list hidden files')
        self._ls_arg_parser.add_argument(
            '--help', action='help', help='show this help')
        self._ls_arg_parser.add_argument(
            'paths', nargs='*', help='paths to list')

    def change_directory(self, new_dir_inode):
        if self._current_inode == new_dir_inode and new_dir_inode != self._fs.root_inode:
            return False

        if not self._fs.is_dir(new_dir_inode):
            print('This is not a directory')
            return False

        self._last_inode = self._current_inode
        self._current_inode = new_dir_inode
        self._current_children = self._fs.list_inode(new_dir_inode)
        self.prompt = F'{self._fs.get_full_path(new_dir_inode)} > '

        return True

    def resolve(self, paths):
        '''
        Todo: resolve relative / absolute / glob into inodes
        '''
        return []

    def do_ls(self, arg):
        '''
        List directory content

        -l list files in the long format
        -a list hidden files
        -h show human-readable size
        '''

        try:
            args = self._ls_arg_parser.parse_args(shlex.split(arg))
        except SystemExit:
            return

        if not args.paths:
            inodes = self._current_children
        else:
            inodes = self.resolve(args.paths)

        for inode in sorted(inodes, key=lambda n: n.name):
            if inode.name.startswith('.') and not args.hidden:
                continue

            print(inode.name)

    def complete_ls(self, text, line, beginidx, endidx):
        completion = []

        if not text:
            completion = [shlex.quote(inode.name)
                          for inode in self._current_children]
        else:
            completion = [shlex.quote(
                inode.name) for inode in self._current_children if inode.name.lower().startswith(text.lower())]

        return completion

    def do_cd(self, arg):
        'Change directory'

        if not arg:
            self.change_directory(self._fs.root_inode)

        try:
            args = shlex.split(arg)
        except ValueError as ve:
            print(F'Invalid parameter: {ve}')
            return

        if len(args) > 1:
            print('Too many arguments')
            return

        path = args[0].split('/')
        if path[0] == '-':
            if len(path) == 1:
                if self._last_inode != file_system.INVALID_INODE:
                    self.change_directory(self._last_inode)
            else:
                print('Too many arguments')

            return

        last_inode = self._last_inode
        current_inode = self._current_inode
        succ = True
        for dir_name in path:
            if not self.cd_from_current_dir(dir_name):
                succ = False
                break

        if not succ:
            self.change_directory(current_inode)

        self._last_inode = last_inode

    def cd_from_current_dir(self, directory):
        if not directory:
            # empty directory
            return True

        if directory == '.':
            # cd pwd
            return True
        elif directory == '..':
            # cd parent
            parent = self._fs.get_parent_inode_id(self._current_inode)
            if parent != file_system.INVALID_INODE:
                self.change_directory(parent)
                return True

            return False

        found = False
        children = [
            child for child in self._current_children if child.name == directory]
        if children:
            self.change_directory(children[0].id)

            return True
        else:
            print(F"cd: The directory '{directory}' doesn't exist")

            return False

    def complete_cd(self, text, line, beginidx, endidx):
        completion = []

        if '/' not in line:
            if not text:
                completion = [shlex.quote(
                    inode.name) for inode in self._current_children if inode.node_type == file_system.InodeType.DIRECTORY]
            else:
                completion = [shlex.quote(inode.name) for inode in self._current_children if inode.node_type ==
                              file_system.InodeType.DIRECTORY and inode.name.lower().startswith(text.lower())]
        else:
            path = [s for s in re.sub(
                r'''^\s*cd\s+''', '', line).split('/') if s][:-1]
            completion = self.get_path_completion(path)

        return completion

    def get_path_completion(self, path):
        '''
        TODO: This doesn't work. Fix it.
        '''
        current_inode = self._current_inode
        while path:
            children = [inode for inode in self._fs.list_inode(
                current_inode) if inode.name == path]
            if not children:
                break
            current_inode = children[0].id
            path = path[1:]

        completion = [
            inode.name for inode in self._fs.list_inode(current_inode)]

    def do_q(self, arg):
        'Quit shell'
        return True

    def do_exit(self, arg):
        'Quit shell'
        return True
