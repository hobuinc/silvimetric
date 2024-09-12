import json
import numpy as np
import pdal
from abc import ABC, abstractmethod
from tiledb import Attr

from .array_extensions import AttributeArray, AttributeDtype

class Entry(ABC):
    """Base class for Attribute and Metric. These represent entries into the
    database."""

    def __eq__(self, other):
        return self.name == other.name and \
            self.dtype == other.dtype

    @abstractmethod
    def entry_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def schema(self) -> Attr:
        raise NotImplementedError

    @abstractmethod
    def to_json(self) -> object:
        raise NotImplementedError

    def toJSON(self) -> object:
        return self.to_json()

    @staticmethod
    @abstractmethod
    def from_dict(data: dict):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def from_string(data: str):
        raise NotImplementedError

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError


class Attribute(Entry):
    """Represents point data from a PDAL execution that has been binned, and
    provides the information necessary to transfer that data to the database."""

    def __init__(self, name: str, dtype: np.dtype):
        super().__init__()
        self.name = name
        """Name of the attribute, eg. Intensity."""
        self.dtype: AttributeDtype
        """SilviMetric representation of array of numpy dtype"""
        if isinstance(dtype, AttributeDtype):
            self.dtype = dtype
        else:
            # AttributeDtype takes any dtype that can be passed to np.dtype
            try:
                self.dtype = AttributeDtype(subtype=dtype)
            except Exception as e:
                raise AttributeError(f"Invalid dtype passed to Attribute: {dtype}") from e

    def make_array(self, data, copy=False):
        return AttributeArray(data=data, copy=copy)

    def entry_name(self) -> str:
        """Return TileDB attribute name."""
        return self.name

    def schema(self) -> Attr:
        """
        Create the tiledb schema for this attribute.
        :return: TileDB attribute schema
        """
        return Attr(name=self.name, dtype=self.dtype.subtype, var=True)

    def to_json(self) -> object:
        return {
            'name': self.name,
            'dtype': np.dtype(self.dtype.subtype).str
        }

    @staticmethod
    def from_dict(data: dict):
        """
        Make an Attribute from a JSON like object
        """
        name = data['name']
        dtype = data['dtype']
        return Attribute(name, dtype)

    @staticmethod
    def from_string(data: str):
        """
        Create Attribute from string or dict version of it.

        :param data: Stringified or json object of attribute
        :raises TypeError: Incorrect type of incoming data, must be string or dict
        :return: Return derived Attribute
        """
        j = json.loads(data)
        return Attribute.from_dict(j)


    def __repr__(self):
        return json.dumps(self.to_json())

# A list of pdal dimensions can be found here https://pdal.io/en/2.6.0/dimensions.html
Pdal_Attributes = { d['name']: Attribute(d['name'], d['dtype']) for d in pdal.dimensions }
Attributes = Pdal_Attributes