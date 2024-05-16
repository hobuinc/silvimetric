from __future__ import annotations

from typing import Sequence

import re

import numpy as np
import pandas as pd

@pd.api.extensions.register_extension_dtype
class AttributeDtype(pd.api.extensions.ExtensionDtype):
    type = np.generic

    def __init__(self, subtype=np.float32):
        self._subtype = np.dtype(subtype)

    def __str__(self) -> str:
        return f'Attribute[{self.subtype}]'

    def __repr__(self) -> str:
        return f'Attribute[{self.subtype}]'

    # TestDtypeTests
    def __hash__(self) -> int:
        return hash(str(self))


    @property
    def subtype(self):
        return self._subtype

    @property
    def name(self):
        return f"Attribute[{self.subtype}]"

    @classmethod
    def construct_array_type(cls):
        return AttributeArray

    @classmethod
    def construct_from_string(cls, string):
        if string.lower() == "attribute":
            return cls()
        match = re.match(r"^attribute\[(\w+)\]$", string, re.IGNORECASE)
        if match:
            return cls(match.group(1))
        raise TypeError(f"Cannot construct a 'AttributeDtype' from '{string}'")


class AttributeArray(pd.api.extensions.ExtensionArray):
    def __init__(self, arrays, dtype):
        assert isinstance(dtype, AttributeDtype)
        self._dtype = dtype
        # self._data = np.array([np.array(array, dtype=dtype.subtype) for array in arrays], dtype=dtype.subtype)
        self._data = [np.asarray(array, dtype=dtype.subtype) for array in arrays]

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        if dtype is None and bool(scalars.dtype):
            return cls(scalars, AttributeDtype(scalars.dtype))
        elif dtype is None:
            cls(scalars, AttributeDtype(np.generic))
        return cls(scalars, dtype)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, i):
        return self._data[i]

    @property
    def dtype(self):
        return self._dtype

    def copy(self):
        return type(self)(self._data, self._dtype)

    def take(self, indices, allow_fill=False, fill_value=None):
        """ Take elements from an array.  """

        if allow_fill and fill_value is None:
            fill_value = self.dtype.na_value

        result = pd.core.algorithms.take(self._data, indices, allow_fill=allow_fill,
                                         fill_value=fill_value)
        return self._from_sequence(result)

    @classmethod
    def _from_factorized(cls, uniques: np.ndarray, original: AttributeArray):
        """ Reconstruct an AttributeArray after factorization.  """
        return cls(uniques, AttributeDtype(uniques.dtype))

    @pd.core.ops.unpack_zerodim_and_defer('__eq__')
    def __eq__(self, other):
        return self._apply_operator('__eq__', other, recast=False)

    def nbytes(self):
        return self._data.nbytes

    def isna(self):
        """ A 1-D array indicating if each value is missing.  """
        return pd.isnull(self._data)

    def copy(self):
        """ Return a copy of the array.  """
        copied = self._data.copy()
        return type(self)(copied, self.dtype)

    @classmethod
    def _concat_same_type(cls, to_concat: Sequence[AttributeArray]) -> AttributeArray:
        """
        Concatenate multiple AngleArrays.
        """
        return cls(np.concatenate(to_concat))


# @pd.api.extensions.register_extension_dtype
# class AttributeDtype(pd.core.dtypes.dtypes.PandasExtensionDtype):
#     """
#     A PandasExtensionDtype specific to SilviMetric Attributes
#     """
#     # Required for all parameterized dtypes
#     _metadata = ('attr',)
#     # _match = re.compile(r'(A|a)ngle\[(?P<unit>.+)\]')

#     def __init__(self, attr: str, dtype=np.generic):
#         self._attr = attr
#         self._type = dtype

#     def __str__(self) -> str:
#         return f'Attribute[{self._attr}]'

#     # TestDtypeTests
#     def __hash__(self) -> int:
#         return hash(str(self))

#     # TestDtypeTests
#     def __eq__(self, other: Any) -> bool:
#         if isinstance(other, str):
#             return self.name == other
#         else:
#             return isinstance(other, type(self)) and \
#                 self.name == other.name and \
#                 self.type == other.type

#     # Required for pickle compat (see GH26067)
#     def __setstate__(self, state) -> None:
#         self._unit = state['unit']

#     # Required for all ExtensionDtype subclasses
#     @classmethod
#     def construct_array_type(cls):
#         """
#         Return the array type associated with this dtype.
#         """
#         return AttributeArray

#     # Recommended for parameterized dtypes
#     @classmethod
#     def construct_from_string(cls, string: str) -> AttributeDtype:
#         """ Construct an AttributeDtype from a string.  """
#         if not isinstance(string, str):
#             msg = f"'construct_from_string' expects a string, got {type(string)}"
#             raise TypeError(msg)

#         msg = f"Cannot construct a '{cls.__name__}' from '{string}'"
#         match = cls._match.match(string)

#         if match:
#             d = match.groupdict()
#             try:
#                 return cls(attr=d['attr'], dtype=np.dtype(d['type']))
#             except (KeyError, TypeError, ValueError) as err:
#                 raise TypeError(msg) from err
#         else:
#             raise TypeError(msg)

#     # Required for all ExtensionDtype subclasses
#     @property
#     def type(self):
#         """
#         The scalar type for the array (e.g., int).
#         """
#         return self._type

#     # Required for all ExtensionDtype subclasses
#     @property
#     def name(self) -> str:
#         """
#         A string representation of the dtype.
#         """
#         return str(self)

#     @property
#     def attr(self) -> str:
#         """
#         The angle unit.
#         """
#         return self._attr

# class AttributeArray(pd.api.extensions.ExtensionArray):

#     def __init__(self, data, attr, copy: bool=False):
#         self._data = np.array(data, copy=copy)
#         self._attr = attr

#     def __getitem__(self, index: int) -> AttributeArray | Any:
#         """
#         Select a subset of self.
#         """
#         if isinstance(index, int):
#             return self._data[index]
#         else:
#             # Check index for TestGetitemTests
#             index = pd.core.indexers.check_array_indexer(self, index)
#             return type(self)(self._data[index])

#     def __len__(self):
#         return len(self._data)

#     # Required for all ExtensionArray subclasses
#     @pd.core.ops.unpack_zerodim_and_defer('__eq__')
#     def __eq__(self, other):
#         return self._apply_operator('__eq__', other, recast=False)

#     def __setstate__(self, state) -> None:
#         self._attr = state['attr']

#     @classmethod
#     def _from_factorized(cls, uniques: np.ndarray, original: AttributeArray):
#         """ Reconstruct an AttributeArray after factorization.  """
#         return cls(uniques, attr=original.dtype.attr)

#     @property
#     def attr(self):
#         return self._attr

#     @property
#     def dtype(self):
#         return AttributeDtype(self._attr, self._data.dtype)

#     def nbytes(self):
#         return self._data.nbytes

#     def isna(self):
#         """ A 1-D array indicating if each value is missing.  """
#         return pd.isnull(self._data)

#     def take(self, indices, allow_fill=False, fill_value=None):
#         """ Take elements from an array.  """

#         if allow_fill and fill_value is None:
#             fill_value = self.dtype.na_value

#         result = pd.core.algorithms.take(self._data, indices, allow_fill=allow_fill,
#                                          fill_value=fill_value)
#         return self._from_sequence(result)

#     def copy(self):
#         """ Return a copy of the array.  """
#         copied = self._data.copy()
#         return type(self)(copied, unit=self.unit)

#     @classmethod
#     def _concat_same_type(cls, to_concat: Sequence[AttributeArray]) -> AttributeArray:
#         """
#         Concatenate multiple AngleArrays.
#         """

#         # ensure same attribute name
#         counts = pd.value_counts([array.dtype.attr for array in to_concat])
#         attr = counts.index[0]

#         if counts.size > 1:
#             to_concat = [a.asunit(attr) for a in to_concat]

#         return cls(np.concatenate(to_concat), attr=attr)
