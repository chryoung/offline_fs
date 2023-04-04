import os
from datetime import datetime, timezone
from exception import *
from enum import IntFlag, auto
from peewee import *
import pwd
import grp
import stat
from functools import lru_cache


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
    node_type = IntegerField()
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
        indexes = (
            (('inode', 'child'), True),
        )
        database = _db


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
    def __init__(self, db_path='file_system.db'):
        self._db_path = db_path
        self._db = _db
        self._db.init(self._db_path)
        self._db.connect()
        self._db.create_tables([User, Group, Inode, Directory, FullPath, Tag, InodeTag])

        # get or create root node
        if not Inode.select().where(Inode.id == 1):
            Inode.create(
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

        self._root_inode = 1


    @property
    def db(self):
        return self._db

    def index(self, path):
        if not os.path.isdir(path):
            raise ParameterException(F'{path} is not a directory')

        path = os.path.abspath(path)

        exist_index = Inode.select()\
                .join(Directory, on=Directory.inode)\
                .join(FullPath, on=(Directory.child == FullPath.inode))\
                .where((Inode.id == self._root_inode) & (FullPath.path == path))
        if exist_index:
            raise DuplicateIndexException(F'{path} is already indexed')

        root_inode = self.create_inode(os.path.basename(path), path, InodeType.DIRECTORY)
        self.link_parent(self._root_inode, root_inode)
        stack = [(os.path.abspath(path), root_inode)] # stack of str

        with self._db.atomic() as tx:
            while stack:
                # get top element
                visiting, visiting_inode = stack[-1]
                # pop top element
                stack = stack[:-2]

                for entry in os.scandir(visiting):
                    inode_type = InodeType(0)

                    if entry.is_dir():
                        inode_type = InodeType.DIRECTORY
                    elif entry.is_file():
                        inode_type = InodeType.FILE
                    elif entry.is_symlink():
                        inode_type = InodeType.SOFTLINK

                    if inode_type != InodeType(0):
                        inode = self.create_inode(entry.name, entry.path, inode_type)
                        self.link_parent(visiting_inode, inode)

                        if inode_type == InodeType.DIRECTORY:
                            stack.append((entry.path, inode))


    def link_parent(self, parent, child):
        Directory.create(inode=parent, child=child)

    def create_inode(self, name, path, inode_type):
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
    
    def parse_owner_permission(self, mode):
        perm = Permission(0)
        if stat.S_IXUSR & mode:
            perm |= Permission.EXECUTABLE
        if stat.S_IWUSR & mode:
            perm |= Permission.WRITE
        if stat.S_IRUSR & mode:
            perm |= Permission.READ

        return perm

    def parse_group_permission(self, mode):
        perm = Permission(0)
        if stat.S_IXGRP & mode:
            perm |= Permission.EXECUTABLE
        if stat.S_IWGRP & mode:
            perm |= Permission.WRITE
        if stat.S_IRGRP & mode:
            perm |= Permission.READ

        return perm
