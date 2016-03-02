from .main import (_validate_parquet_file, _read_footer, _get_offset, _read_page_header,
                   read_dictionary_page, read_data_page)
from .ttypes import PageType, Type
from .converted_types import convert_column
from .schema import SchemaHelper

from collections import defaultdict
import pandas as pd


class CurrentLocation(object):
    def __init__(self):
        self._page_index = 0
        self._row_index = 0

    def __repr__(self):
        return "page_index={} row_index={}".format(self._page_index,
                                                   self._row_index)


class ParquetReader(object):
    def __init__(self, binary_stream):
        self._fo = binary_stream
        _validate_parquet_file(self._fo)
        self._footer = _read_footer(self._fo)
        self._schema_helper = SchemaHelper(self._footer.schema)
        self._rg = self._footer.row_groups
        self._cg = self._rg[0].columns
        self._schema = [s for s in self._footer.schema if s.num_children is None]
        self._cols = [".".join(x for x in c.meta_data.path_in_schema) for c in
                      self._cg]
        self._rows = self._footer.num_rows
        self._row_group_index = 0
        self._column_group_locations = defaultdict(CurrentLocation)
        self._rows_read = 0

    def _get_column_info(self, col):
        name = ".".join(x for x in col.meta_data.path_in_schema)
        ind = [s for s in self._schema if s.name == name]
        width = ind[0].type_length
        return (name, width)

    def _read_rows_in_group(self, col, name, width, rg, remaining_rows):
        offset = _get_offset(col.meta_data)
        self._fo.seek(offset, 0)
        cmd = col.meta_data
        cmd.width = width
        location_in_group = self._column_group_locations[name]
        total_rows_in_group = rg.num_rows
        values_seen = 0
        page_index = 0
        column = []
        dict_items = []
        while values_seen < total_rows_in_group:
            ph = _read_page_header(self._fo)
            if page_index < location_in_group._page_index:
                # skip
                if ph.type == PageType.DATA_PAGE:
                    self._fo.seek(ph.compressed_page_size, 1)
                    daph = ph.data_page_header
                    values_seen += daph.num_values
                elif ph.type == PageType.DICTIONARY_PAGE:
                    dict_items = read_dictionary_page(self._fo, ph, cmd)
            else:
                # start reading rows.
                if ph.type == PageType.DATA_PAGE:
                    values = read_data_page(self._fo,
                                            self._schema_helper, ph,
                                            cmd, dict_items)

                    # Need to check which values to keep
                    if location_in_group._row_index != 0:
                        values = values[location_in_group._row_index:]

                    done = False
                    if remaining_rows is not None:
                        if len(values) + len(column) >= remaining_rows:
                            done = True
                            needed = remaining_rows - len(column)
                            if needed != len(values):
                                values = values[:needed]
                                location_in_group._page_index = page_index
                                location_in_group._row_index += needed
                            else:
                                location_in_group._page_index += 1
                                location_in_group._row_index = 0
                    column += values
                    if done:
                        return column

                    values_seen += ph.data_page_header.num_values
                elif ph.type == PageType.DICTIONARY_PAGE:
                    dict_items = read_dictionary_page(self._fo, ph, cmd)
            if page_index < location_in_group._page_index:
                page_index += 1
            else:
                location_in_group._page_index = page_index
                location_in_group._row_index = 0
                page_index += 1

        return column

    def read(self, columns=None, rows=None):
        columns = columns or self._cols
        res = defaultdict(list)

        remaining_rows = rows
        while self._row_group_index < len(self._rg):
            rg = self._rg[self._row_group_index]
            cg = rg.columns
            rows_read = 0
            for col in cg:
                name, width = self._get_column_info(col)
                if name not in columns:
                    continue
                row_data = self._read_rows_in_group(col, name, width,
                                                    rg, remaining_rows)
                res[name] += row_data
                if rows_read == 0 and len(row_data):
                    rows_read = len(row_data)
            if remaining_rows is not None:
                remaining_rows -= rows_read
                if remaining_rows == 0:
                    break

            self._row_group_index += 1

        if len(res) == 0:
            for name in columns:
                res[name] = []

        out = pd.DataFrame(res)
        for col in columns:
            schema = [s for s in self._schema if col == s.name][0]
            if schema.converted_type:
                out[col] = convert_column(out[col], schema)
            elif schema.type in [Type.BYTE_ARRAY,
                                 Type.FIXED_LEN_BYTE_ARRAY]:
                def _conv(x):
                    if x is not None:
                        return x.decode('utf-8')
                out[col] = out[col].apply(_conv)
        return out
