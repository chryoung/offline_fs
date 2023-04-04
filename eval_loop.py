class EvalLoop:
    def __init__(self, fs):
        self._fs = fs
        self._current_inode = fs.root_inode
        self._pwd = fs.get_full_path(self._current_inode)

    def run(self):
        while True:
            command = input(F'{self._pwd} > ').strip()
            if command == 'q':
                break
            elif command == 'h':
                self.show_help()
            elif command == 'ls':
                self.show_files(command)

    def show_help(self):
        print('ls List files')
        print('q Exit')
        print('h Help')

    def show_files(self, command):
        inodes = self._fs.list_inode(self._current_inode)
        if inodes:
            for inode in inodes:
                print(inode.name)
