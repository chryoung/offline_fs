import os
from datetime import datetime, timezone
from exception import *
from enum import IntFlag, auto
from peewee import *
import pwd
import grp
import stat
from functools import lru_cache
from pathlib import PurePath

INVALID_INODE = -1
_db = SqliteDatabase(None)


class InodeType(IntFlag):
    FILE = auto()
    DIRECTORY = auto()
    DEVICE = auto()
    SOFTLINK = auto()
    HARDLINK = auto()


class Permission(IntFlag):
    EXECUTABLE = auto()
    WRITE = auto()
    READ = auto()


class User(Model):
    id = IntegerField(primary_key=True)
    username = TextField(index=True)

    class Meta:
        database = _db


class Group(Model):
    id = IntegerField(primary_key=True)
    group_name = TextField(index=True)

    class Meta:
        database = _db


class Inode(Model):
    id = AutoField(primary_key=True)
    node_type = IntegerField(index=True)
    name = TextField(index=True)
    size = IntegerField(index=True)
    created_time = DateTimeField(index=True)
    modified_time = DateTimeField(index=True)
    owner = ForeignKeyField(User, backref='owner')
    group = ForeignKeyField(Group, backref='groups')
    owner_permission = IntegerField()
    group_permission = IntegerField()

    class Meta:
        database = _db


class Directory(Model):
    inode = ForeignKeyField(Inode, backref='parent')
    child = ForeignKeyField(Inode, backref='children')

    class Meta:
        database = _db
        indexes = (
            (('inode', 'child'), True),
        )


class Ancestor(Model):
    ancestor = ForeignKeyField(Inode, backref='ancestors')
    inode = ForeignKeyField(Inode, backref='inodes')

    class Meta:
        database = _db
        indexes = (
            (('ancestor', 'inode'), True),
        )


class FullPath(Model):
    inode = ForeignKeyField(Inode, index=True, backref='inode')
    path = TextField()

    class Meta:
        database = _db


class Tag(Model):
    id = AutoField(primary_key=True)
    name = TextField()

    class Meta:
        database = _db


class InodeTag(Model):
    inode = ForeignKeyField(Inode, index=True, backref='inode')
    tag = ForeignKeyField(Tag, index=True, backref='tags')

    class Meta:
        database = _db


class FileSystem:
    def __init__(self, db_path):
        if not db_path.strip():
            raise ParameterException(
                F'{db_path} is required to create a FileSystem')

        self._db_path = db_path
        self._db = _db
        self._db.init(self._db_path)
        self._db.connect()
        self._db.create_tables(
            [User, Group, Inode, Directory, Ancestor, FullPath, Tag, InodeTag])

        # get or create root node
        root = 1
        if not Inode.select().where(Inode.id == root):
            root = Inode.create(
                node_type=int(InodeType.DIRECTORY),
                name='/',
                size=0,
                created_time=datetime.now(),
                modified_time=datetime.now(),
                owner=0,
                group=0,
                owner_permission=0,
                group_permission=0,
            )

            FullPath.create(inode=root, path='/')

        self._root_inode = root

    @property
    def db(self):
        return self._db

    def index(self, path):
        if not os.path.isdir(path):
            raise ParameterException(F'{path} is not a directory')

        path = PurePath(os.path.abspath(path))

        exist_index = Inode.select()\
            .join(Directory, on=Directory.inode)\
            .join(FullPath, on=(Directory.child == FullPath.inode))\
            .where((Inode.id == self._root_inode) & (FullPath.path == str(path)))

        if exist_index:
            raise DuplicateIndexException(F'{path} is already indexed')

        new_root_path = PurePath(F'/{path.name}')
        root_inode = self.create_inode(
            path.name, str(path), InodeType.DIRECTORY, new_root_path)
        self.link_parent(self._root_inode, root_inode)
        stack = [(path, root_inode, [])]

        with self._db.atomic() as tx:
            while stack:
                # get the top element
                visiting, visiting_inode, ancestors = stack[-1]
                ancestors = ancestors.copy()
                ancestors.append(visiting_inode)
                # pop the top element
                stack = stack[:-1]

                try:
                    for entry in os.scandir(visiting):
                        inode_type = InodeType(0)

                        if entry.is_dir():
                            inode_type = InodeType.DIRECTORY
                        elif entry.is_file():
                            inode_type = InodeType.FILE
                        elif entry.is_symlink():
                            inode_type = InodeType.SOFTLINK

                        if inode_type != InodeType(0):
                            entry_path = PurePath(entry.path)
                            inode = self.create_inode(
                                entry.name, entry.path, inode_type, str(PurePath(new_root_path / entry_path.relative_to(path))))
                            self.link_parent(visiting_inode, inode)
                            self.link_ancestors(ancestors, inode)

                            if inode_type == InodeType.DIRECTORY:
                                stack.append((entry.path, inode, ancestors))
                except PermissionError as perm_ex:
                    print(
                        F'Insufficient permission to visit {visiting}: {perm_ex}')

    def link_parent(self, parent, child):
        Directory.create(inode=parent, child=child)

    def link_ancestors(self, ancestors, inode):
        for ancestor in ancestors:
            Ancestor.create(ancestor=ancestor, inode=inode)

    def create_inode(self, name, path, inode_type, full_path=''):
        stat = self.get_stat(path)
        inode = Inode.create(
            node_type=int(inode_type),
            name=name,
            size=stat['size'],
            created_time=stat['created_time'],
            modified_time=stat['modified_time'],
            owner=self.get_or_create_user(stat['owner']),
            group=self.get_or_create_group(stat['group']),
            owner_permission=int(stat['owner_permission']),
            group_permission=int(stat['group_permission']),
        )

        if full_path:
            FullPath.create(inode=inode, path=full_path)
        else:
            FullPath.create(inode=inode, path=os.path.abspath(path))

        return inode

    @lru_cache
    def get_or_create_user(self, uid):
        users = User.select(User.id).where(User.id == uid)
        if users:
            return uid

        username = pwd.getpwuid(uid).pw_name
        User.create(id=uid, username=username)

        return uid

    @lru_cache
    def get_or_create_group(self, gid):
        groups = Group.select(Group.id).where(Group.id == gid)
        if groups:
            return gid

        name = grp.getgrgid(gid).gr_name
        Group.create(id=gid, group_name=name)

        return gid

    def get_stat(self, path):
        stat = os.stat(path)
        return {
            'size': stat.st_size,
            'owner': stat.st_uid,
            'group': stat.st_gid,
            'owner_permission': self.parse_owner_permission(stat.st_mode),
            'group_permission': self.parse_group_permission(stat.st_mode),
            'created_time': self.to_datetime(stat.st_ctime),
            'modified_time': self.to_datetime(stat.st_mtime),
        }

    def to_datetime(self, st_time):
        return datetime.fromtimestamp(st_time, tz=timezone.utc)

    @lru_cache
    def parse_owner_permission(self, mode):
        perm = Permission(0)
        if stat.S_IXUSR & mode:
            perm |= Permission.EXECUTABLE
        if stat.S_IWUSR & mode:
            perm |= Permission.WRITE
        if stat.S_IRUSR & mode:
            perm |= Permission.READ

        return perm

    @lru_cache
    def parse_group_permission(self, mode):
        perm = Permission(0)
        if stat.S_IXGRP & mode:
            perm |= Permission.EXECUTABLE
        if stat.S_IWGRP & mode:
            perm |= Permission.WRITE
        if stat.S_IRGRP & mode:
            perm |= Permission.READ

        return perm

    @property
    def root_inode(self):
        return self._root_inode

    @lru_cache
    def get_full_path(self, inode_id):
        result = FullPath.select(FullPath.path).where(
            FullPath.inode == inode_id)
        if result:
            return result[0].path

        return None

    @lru_cache
    def list_inode(self, inode_id, get_self=False):
        inode = Inode.select().where(Inode.id == inode_id)

        if not inode:
            return None

        inode = inode[0]

        if inode.node_type == InodeType.DIRECTORY and not get_self:
            return Inode\
                .select()\
                .join(Directory, on=(Directory.child == Inode.id))\
                .where(Directory.inode == inode_id)

        return [inode]

    @lru_cache
    def list_pattern(self, inode_id, pattern):
        return Inode\
            .select()\
            .join(Directory, on=(Directory.child == Inode.id))\
            .where((Directory.inode == inode_id) & (Inode.name ** pattern))

    @lru_cache
    def get_parent_inode_id(self, inode_id):
        result = Directory.select(Directory.inode).where(
            Directory.child == inode_id)
        if result:
            return result[0].inode

        return INVALID_INODE

    def is_dir(self, inode_id):
        if Inode.select(Inode.id).where((Inode.id == inode_id) & (Inode.node_type == int(InodeType.DIRECTORY))):
            return True

        return False
