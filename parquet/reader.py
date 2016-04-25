from .main import ParquetMain
from .ttypes import PageType, Type
from .converted_types import convert_column
from .schema import SchemaHelper

from collections import defaultdict
import pandas as pd
import os.path


class CurrentLocation(object):
    def __init__(self):
        self._page_index = 0
        self._row_index = 0

    def __repr__(self):
        return "page_index={} row_index={}".format(self._page_index,
                                                   self._row_index)


class ParquetReader(object):
    def __init__(self, binary_stream):
        self._main_file = None
        self._directory = None
        self._main_filename = None
        self._files = {}
        self._main = ParquetMain()
        self._open_main(binary_stream)
        self._footer = self._main.read_footer(self._main_filename, self._main_file)
        self._schema_helper = SchemaHelper(self._footer.schema)
        self._rg = self._footer.row_groups
        self._cg = self._rg[0].columns
        self._schema = [s for s in self._footer.schema if s.num_children is None]
        self._cols = []
        for c in self._cg:
            self._cols.append(".".join([x for x in c.meta_data.path_in_schema]))
        self._rows = self._footer.num_rows
        self._row_group_index = 0
        self._column_group_locations = defaultdict(CurrentLocation)
        self._rows_read = 0

    def __del__(self):
        self.close()

    def _open_main(self, binary_stream_or_name):
        if isinstance(binary_stream_or_name, str):
            if self._is_directory(binary_stream_or_name):
                self._directory = binary_stream_or_name
                self._main_filename = os.path.join(binary_stream_or_name, "_metadata")
            else:
                self._main_filename = binary_stream_or_name
            self._main_file = self._open_file(self._main_filename)
        else:
            self._main_file = binary_stream_or_name

    def _is_directory(self, file_name):
        """ For non local files (ie HDFS), this will need to be overridden
        """
        return os.path.isdir(file_name)

    def _open_file(self, file_name):
        """ For non local files (ie HDFS), this will need to be overridden
        """
        fileobj = open(file_name, 'rb')
        self._files[file_name] = fileobj
        return fileobj

    def _close_file(self, fileobj):
        """ For non local files (ie HDFS), this will need to be overridden
        """
        fileobj.close()

    def _get_file(self, file_name):
        if self._directory is not None:
            file_name = os.path.join(self._directory, file_name)
        if file_name in self._files:
            return self._files[file_name]
        return self._open_file(file_name)

    def close(self):
        for fileobj in self._files.values():
            self._close_file(fileobj)
        self._files = {}

    def _get_column_info(self, col):
        name = ".".join(x for x in col.meta_data.path_in_schema)
        ind = [s for s in self._schema if s.name == name]
        width = ind[0].type_length
        return (name, width)

    def _read_rows_in_group(self, col, name, width, rg, remaining_rows,
                            natural):
        file_name = col.file_path

        if file_name is not None:
            fileobj = self._get_file(file_name)
        else:
            fileobj = self._main_file
        offset = self._main._get_offset(col.meta_data)
        fileobj.seek(offset, 0)
        cmd = col.meta_data
        cmd.width = width
        location_in_group = self._column_group_locations[name]
        total_rows_in_group = rg.num_rows
        values_seen = 0
        page_index = 0
        column = []
        dict_items = []

        while values_seen < total_rows_in_group:
            ph = self._main._read_page_header(fileobj)
            if page_index < location_in_group._page_index and not natural:
                # skip
                if ph.type == PageType.DATA_PAGE:
                    fileobj.seek(ph.compressed_page_size, 1)
                    daph = ph.data_page_header
                    values_seen += daph.num_values
                elif ph.type == PageType.DICTIONARY_PAGE:
                    dict_items = self._main.read_dictionary_page(fileobj, ph, cmd)
            else:
                # start reading rows.
                if ph.type == PageType.DATA_PAGE:
                    values = self._main.read_data_page(fileobj,
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
                    dict_items = self._main.read_dictionary_page(fileobj, ph, cmd)
            if page_index < location_in_group._page_index:
                page_index += 1
            else:
                location_in_group._page_index = page_index
                location_in_group._row_index = 0
                page_index += 1

        return column

    def read(self, columns=None, rows=None, natural=False):
        if columns:
            for c in columns:
                if c not in self._cols:
                    raise ValueError("Unknown column {}".format(c))

        columns = columns or self._cols
        res = defaultdict(list)

        if natural and rows is not None:
            raise ValueError("Cannot specify rows with natural")
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
                                                    rg, remaining_rows, natural)
                res[name] += row_data
                if rows_read == 0 and len(row_data):
                    rows_read = len(row_data)

            if natural and rows_read != 0:
                self._row_group_index += 1
                break
            if remaining_rows is not None:
                remaining_rows -= rows_read
                if remaining_rows == 0:
                    break

            self._row_group_index += 1

        return self._make_dataframe(res, columns)

    def _make_dataframe(self, res, columns):
        if len(res) == 0:
            for name in columns:
                res[name] = []

        out = pd.DataFrame(res, columns=columns)

        for col in columns:
            match = [s for s in self._schema if col == s.name]
            if len(match):
                schema = match[0]
                if schema.converted_type:
                    out[col] = convert_column(out[col], schema)
        return out
