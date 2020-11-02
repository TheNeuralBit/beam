#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import inspect
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Tuple
from typing import Union
import weakref

from apache_beam import pvalue
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import schemas
from apache_beam.dataframe import transforms

if TYPE_CHECKING:
  # pylint: disable=ungrouped-imports
  from typing import Optional
  import pandas


# TODO: Or should this be called as_dataframe?
def to_dataframe(
    pcoll,  # type: pvalue.PCollection
    proxy=None,  # type: Optional[pandas.core.generic.NDFrame]
    label=None,  # type: Optional[str]
):
  # type: (...) -> frame_base.DeferredFrame

  """Converts a PCollection to a deferred dataframe-like object, which can
  manipulated with pandas methods like `filter` and `groupby`.

  For example, one might write::

    pcoll = ...
    df = to_dataframe(pcoll, proxy=...)
    result = df.groupby('col').sum()
    pcoll_result = to_pcollection(result)

  A proxy object must be given if the schema for the PCollection is not known.
  """
  if proxy is None:
    if pcoll.element_type is None:
      raise ValueError(
          "Cannot infer a proxy because the input PCollection does not have a "
          "schema defined. Please make sure a schema type is specified for "
          "the input PCollection, or provide a proxy.")
    # If no proxy is given, assume this is an element-wise schema-aware
    # PCollection that needs to be batched.
    if label is None:
      # Attempt to come up with a reasonable, stable label by retrieving
      # the name of these variables in the calling context.
      label = 'BatchElements(%s)' % _var_name(pcoll, 2)
    proxy = schemas.generate_proxy(pcoll.element_type)
    pcoll = pcoll | label >> schemas.BatchRowsAsDataFrame(proxy=proxy)
  return frame_base.DeferredFrame.wrap(
      expressions.PlaceholderExpression(proxy, pcoll))


# PCollections generated by to_pcollection are memoized, keyed by expression id.
# WeakValueDictionary is used so the caches are cleaned up with the parent
# pipelines
# Note that the pipeline (indirectly) holds references to the transforms which
# keeps both the PCollections and expressions alive. This ensures the
# expression's ids are never accidentally re-used.
TO_PCOLLECTION_CACHE = weakref.WeakValueDictionary()
UNBATCHED_CACHE = weakref.WeakValueDictionary()


def _make_unbatched_pcoll(pc, expr, include_indexes):
  label = f"Unbatch '{expr._id}'"
  if include_indexes:
    label += " with indexes"

  if label not in UNBATCHED_CACHE:
    UNBATCHED_CACHE[label] = pc | label >> schemas.UnbatchPandas(
        expr.proxy(), include_indexes=include_indexes)

  # Note unbatched cache is keyed by the expression id as well as parameters
  # for the unbatching (i.e. include_indexes)
  return UNBATCHED_CACHE[label]


# TODO: Or should this be called from_dataframe?


def to_pcollection(
    *dataframes,  # type: frame_base.DeferredFrame
    **kwargs):
  # type: (...) -> Union[pvalue.PCollection, Tuple[pvalue.PCollection, ...]]

  """Converts one or more deferred dataframe-like objects back to a PCollection.

  This method creates and applies the actual Beam operations that compute
  the given deferred dataframes, returning a PCollection of their results. By
  default the resulting PCollections are schema-aware PCollections where each
  element is one row from the output dataframes, excluding indexes. This
  behavior can be modified with the `yield_elements` and `include_indexes`
  arguments.

  If more than one (related) result is desired, it can be more efficient to
  pass them all at the same time to this method.

  Args:
    always_return_tuple: (optional, default: False) If true, always return
        a tuple of PCollections, even if there's only one output.
    yield_elements: (optional, default: "schemas") If set to "pandas", return
        PCollections containing the raw Pandas objects (DataFrames or Series),
        if set to "schemas", return an element-wise PCollection, where DataFrame
        and Series instances are expanded to one element per row. DataFrames are
        converted to schema-aware PCollections, where column values can be
        accessed by attribute.
    include_indexes: (optional, default: False) When yield_elements="schemas",
        if include_indexes=True, attempt to include index columns in the output
        schema for expanded DataFrames. Raises an error if any of the index
        levels are unnamed (name=None), or if any of the names are not unique
        among all column and index names.
  """
  label = kwargs.pop('label', None)
  always_return_tuple = kwargs.pop('always_return_tuple', False)
  yield_elements = kwargs.pop('yield_elements', 'schemas')
  if not yield_elements in ("pandas", "schemas"):
    raise ValueError(
        "Invalid value for yield_elements argument, '%s'. "
        "Allowed values are 'pandas' and 'schemas'" % yield_elements)
  include_indexes = kwargs.pop('include_indexes', False)
  assert not kwargs  # TODO(BEAM-7372): Use PEP 3102
  if label is None:
    # Attempt to come up with a reasonable, stable label by retrieving the name
    # of these variables in the calling context.
    label = 'ToPCollection(%s)' % ', '.join(_var_name(e, 3) for e in dataframes)

  def extract_input(placeholder):
    if not isinstance(placeholder._reference, pvalue.PCollection):
      raise TypeError(
          'Expression roots must have been created with to_dataframe.')
    return placeholder._reference

  placeholders = frozenset.union(
      frozenset(), *[df._expr.placeholders() for df in dataframes])

  # Exclude any dataframes that have already been converted to PCollections.
  # We only want to convert each DF expression once, then re-use.
  new_dataframes = [
      df for df in dataframes if df._expr._id not in TO_PCOLLECTION_CACHE
  ]
  if len(new_dataframes):
    new_results = {p: extract_input(p)
                   for p in placeholders
                   } | label >> transforms._DataframeExpressionsTransform({
                       ix: df._expr
                       for (ix, df) in enumerate(new_dataframes)
                   })  # type: Dict[Any, pvalue.PCollection]

    TO_PCOLLECTION_CACHE.update(
        {new_dataframes[ix]._expr._id: pc
         for ix, pc in new_results.items()})

  raw_results = {
      ix: TO_PCOLLECTION_CACHE[df._expr._id]
      for ix,
      df in enumerate(dataframes)
  }

  if yield_elements == "schemas":

    def maybe_unbatch(pc, value):
      if isinstance(value, frame_base._DeferredScalar):
        return pc
      else:
        return _make_unbatched_pcoll(pc, value._expr, include_indexes)

    results = {
        ix: maybe_unbatch(pc, dataframes[ix])
        for (ix, pc) in raw_results.items()
    }
  else:
    results = raw_results

  if len(results) == 1 and not always_return_tuple:
    return results[0]
  else:
    return tuple(value for key, value in sorted(results.items()))


def _var_name(obj, level):
  frame = inspect.currentframe()
  for _ in range(level):
    if frame is None:
      return '...'
    frame = frame.f_back
  for key, value in frame.f_locals.items():
    if obj is value:
      return key
  for key, value in frame.f_globals.items():
    if obj is value:
      return key
  return '...'
