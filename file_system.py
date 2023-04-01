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
    owner = ForeignKeyField(User, backref='name')
    group = ForeignKeyField(Group, backref='name')
    owner_permission = IntegerField()
    group_permission = IntegerField()

    class Meta:
        database = _db


class Directory(Model):
    inode = ForeignKeyField(Inode, index=True)
    child = ForeignKeyField(Inode)

    class Meta:
        database = _db


class FullPath(Model):
    inode = ForeignKeyField(Inode, index=True)
    path = TextField()

    class Meta:
        database = _db


class Tag(Model):
    id = AutoField(primary_key=True)
    name = TextField()

    class Meta:
        database = _db


class InodeTag(Model):
    inode = ForeignKeyField(Inode)
    tag = ForeignKeyField(Tag)

    class Meta:
        database = _db


class FileSystem:
    def __init__(self, db_path='file_system.db'):
        self._db_path = db_path
        self._db = _db
        self._db.init(self._db_path)
        self._db.connect()
        self._db.create_tables([User, Group, Inode, Directory, FullPath, Tag, InodeTag])

    @property
    def db(self):
        return self._db

    def index(self, path):
        if not os.path.isdir(path):
            raise ParameterException(F'{path} is not a directory')

        with self._db.atomic() as tx:
            for root, dirs, files in os.walk(path):
                for dir in dirs:
                    self.create_inode(dir, os.path.join(root, dir), InodeType.DIRECTORY)
                for file in files:
                    self.create_inode(file, os.path.join(root, file), InodeType.FILE)


    def create_inode(self, name, path, node_type):
        stat = self.get_stat(path)
        inode = Inode.create(
            node_type=int(node_type),
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
