import argparse
import file_system
import fsshell

parser = argparse.ArgumentParser(prog='Offline Filesystem')
parser.add_argument('-d', '--db', default='file_system.db',
                    help='Specify the database for storing offline filesystem')

subparsers = parser.add_subparsers(dest='sub_command', help='Sub commands')

parser_index = subparsers.add_parser(
    'index', aliases=['idx'], help='Build index for paths')
parser_index.add_argument('paths', nargs='+', help='Paths to index')

parser_interactive = subparsers.add_parser(
    'interactive', aliases=['i'], help='Browse files interactively')

args = parser.parse_args()

fs = file_system.FileSystem(args.db)

if args.sub_command in ('index', 'idx'):
    for path in args.paths:
        fs.index(path)
elif args.sub_command in ('interactive', 'i'):
    shell = fsshell.FsShell(fs)
    shell.cmdloop()
