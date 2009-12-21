from cassandra.ttypes import Column, ColumnOrSuperColumn, ColumnParent, \
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate, SliceRange

import time

__all__ = ['gm_timestamp', 'ColumnFamily']

def gm_timestamp():
    """
    Returns
    -------
    int : UNIX epoch time in GMT
    """
    return int(time.mktime(time.gmtime()))

def Column2base(column, include_timestamp):
    if include_timestamp:
        return (column.value, column.timestamp)
    return column.value

def ColumnOrSuperColumns2dict(list_col_or_super, include_timestamp):
    ret = {}
    for col_or_super in list_col_or_super:
        col = col_or_super.column
        ret[col.name] = Column2base(col, include_timestamp)
    return ret

def create_SlicePredicate(columns, column_start, column_finish, column_reversed, column_count):
    if columns is not None:
        return SlicePredicate(column_names=columns)
    sr = SliceRange(start=column_start, finish=column_finish,
                    reversed=column_reversed, count=column_count)
    return SlicePredicate(slice_range=sr)

class ColumnFamily(object):
    def __init__(self, client, keyspace, column_family,
                 buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ZERO,
                 timestamp=gm_timestamp):
        """
        Construct a ColumnFamily

        Parameters
        ----------
        client   : cassandra.Cassandra.Client
            Cassandra client with thrift API
        keyspace : str
            The Keyspace this ColumnFamily belongs to
        column_family : str
            The name of this ColumnFamily
        buffer_size : int
            When calling get_range(), the intermediate results need to be
            buffered if we are fetching many rows, otherwise the Cassandra
            server will overallocate memory and fail.  This is the size of
            that buffer.
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation
        write_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any write operation
        timestamp : function
            The default timestamp function returns:
            int(time.mktime(time.gmtime()))
            Or the number of seconds since Unix epoch in GMT.
            Set timestamp to replace the default timestamp function with your
            own.
        """
        self.client = client
        self.keyspace = keyspace
        self.column_family = column_family
        self.buffer_size = buffer_size
        self.read_consistency_level = read_consistency_level
        self.write_consistency_level = write_consistency_level
        self.timestamp = timestamp

    def get(self, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False):
        """
        Fetch a key from a Cassandra server
        
        Parameters
        ----------
        key : str
            The key to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count : int
            Limit the number of columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        if include_timestamp == True: {'column': ('value', timestamp)}
        else: {'column': 'value'}
        """
        if columns is not None and len(columns) == 1:
            column = columns[0]
            cp = ColumnPath(column_family=self.column_family, column=column)
            col = self.client.get(self.keyspace, key, cp,
                                  self.read_consistency_level).column
            return {col.name: Column2base(col, include_timestamp)}

        cp = ColumnParent(column_family=self.column_family)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        lst_col_or_super = self.client.get_slice(self.keyspace, key, cp, sp,
                                                 self.read_consistency_level)

        ret = {}
        for col_or_super in lst_col_or_super:
            col = col_or_super.column
            ret[col.name] = Column2base(col, include_timestamp)
        if len(ret) == 0:
            raise NotFoundException()
        return ret

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False):
        """
        Fetch multiple key from a Cassandra server
        
        Parameters
        ----------
        keys : [str]
            A list of keys to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count : int
            Limit the number of columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        if include_timestamp == True: {'key': {'column': ('value', timestamp)}}
        else: {'key': {'column': 'value'}}
        """
        if len(keys) == 1:
            key = keys[0]
            cols = self.get(key, columns=columns, column_start=column_start,
                            column_finish=column_finish,
                            include_timestamp=include_timestamp)
            return {key: cols}

        if columns is not None and len(columns) == 1:
            column = columns[0]
            cp = ColumnPath(column_family=self.column_family, column=column)
            keymap = self.client.multiget(self.keyspace, keys, cp,
                                       self.read_consistency_level)

            ret = {}
            for key, col_or_sp in keymap.iteritems():
                col = col_or_sp.column
                ret[key] = {col.name: Column2base(col, include_timestamp)}
            return ret

        cp = ColumnParent(column_family=self.column_family)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        keymap = self.client.multiget_slice(self.keyspace, keys, cp, sp,
                                            self.read_consistency_level)

        ret = {}
        for key, columns in keymap.iteritems():
            ret[key] = ColumnOrSuperColumns2dict(columns, include_timestamp)
        return ret

    def get_count(self, key):
        """
        Count the number of columns for a key

        Parameters
        ----------
        key : str
            The key with which to count columns

        Returns
        -------
        int Count of columns
        """
        cp = ColumnParent(column_family=self.column_family)
        return self.client.get_count(self.keyspace, key, cp,
                                     self.read_consistency_level)

    def get_range(self, start="", finish="", columns=None, column_start="",
                       column_finish="", column_reversed=False, column_count=100,
                       row_count=None, include_timestamp=False):
        """
        Get an iterator over keys in a specified range
        
        Parameters
        ----------
        start : str
            Start from this key (inclusive)
        finish : str
            End at this key (inclusive)
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count : int
            Limit the number of columns fetched per key
        row_count : int
            Limit the number of rows fetched
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        iterator over ('key', {'column': 'value'})
        """
        cp = ColumnParent(column_family=self.column_family)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        last_key = start
        ignore_first = False
        i = -1
        while True:
            key_slices = self.client.get_range_slice(self.keyspace, cp, sp, last_key,
                                                     finish, self.buffer_size,
                                                     self.read_consistency_level)

            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if i > 0 and j == 0:
                    continue
                i += 1
                if row_count is not None and i >= row_count:
                    return
                yield (key_slice.key,
                       ColumnOrSuperColumns2dict(key_slice.columns, include_timestamp))

            if len(key_slices) > 0:
                last_key = key_slices[-1].key
                ignore_first = True
            if len(key_slices) != self.buffer_size:
                return

    def insert(self, key, columns):
        """
        Insert or update columns for a key

        Parameters
        ----------
        key : str
            The key to insert or update the columns at
        columns : {'column': 'value'}
            The columns to insert or update

        Returns
        -------
        int timestamp
        """
        timestamp = self.timestamp()
        if len(columns) == 1:
            col, val = columns.items()[0]
            cp = ColumnPath(column_family=self.column_family, column=col)
            self.client.insert(self.keyspace, key, cp, val,
                               timestamp, self.write_consistency_level)
            return timestamp

        cols = []
        for c, v in columns.iteritems():
            column = Column(name=c, value=v, timestamp=timestamp)
            cols.append(ColumnOrSuperColumn(column=column))
        self.client.batch_insert(self.keyspace, key,
                                 {self.column_family: cols},
                                 self.write_consistency_level)
        return timestamp

    def remove(self, key, column=None):
        """
        Remove a specified key or column

        Parameters
        ----------
        key : str
            The key to remove. If column is not set, remove all columns
        column : str
            If set, remove only this column

        Returns
        -------
        int timestamp
        """
        cp = ColumnPath(column_family=self.column_family, column=column)
        timestamp = self.timestamp()
        self.client.remove(self.keyspace, key, cp, timestamp,
                           self.write_consistency_level)
        return timestamp
