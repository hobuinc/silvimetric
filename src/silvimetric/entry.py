import numpy as np
from typing import Self
from abc import ABC, abstractmethod
from tiledb import Attr

class Entry(ABC):

    @abstractmethod
    def __init__(self, name: str, dtype: np.dtype, deps: list[Self]):
        self._name = name
        self._dtype = dtype
        self._dependencies = deps

    @property
    @abstractmethod
    def name(self):
        return self._name

    @property
    @abstractmethod
    def dependencies(self):
        return self._dependencies

    @abstractmethod
    def entry_name(self):
        return self._name

    @abstractmethod
    def schema(self):
        return Attr(name=self.name, dtype=self.dtype, var=True)
