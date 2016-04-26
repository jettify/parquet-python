import abc
import os


class BaseFileSystem(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def open(self, path, mode='rb'):
        pass

    @abc.abstractmethod
    def is_dir(self, path):
        pass


class LocalFileSystem(BaseFileSystem):
    def open(self, path, mode='rb'):
        return open(path, mode=mode)

    def is_dir(self, path):
        return os.path.isdir(path)
