import file_system
import argparse

parser = argparse.ArgumentParser(prog='Offline Filesystem')
parser.add_argument('-d', '--db', default='file_system.db', help='Specify the database for storing offline filesystem.')

subparsers = parser.add_subparsers(dest='sub_command', help='Sub commands')

parser_index = subparsers.add_parser('index', help='Build index for path.')
parser_index.add_argument('paths', nargs='+', help='Paths to index')

args = parser.parse_args()

fs = file_system.FileSystem(args.db)

if args.sub_command == 'index':
    for path in args.paths:
        fs.index(path)
