import pandas as pd
from abc import ABC
## TODO

class genericDB(ABC):
    """
    Generic database structure to be inherited to specific database structures
    """
    ##Class Fields ##

    ##Init##
    def __init__(self, *args, **kwargs):
        # Handle args == 1
        if len(args) > 1:
            raise AttributeError('Too many arguments')
        for arg in args:
            kwargs['data'] = arg

        # Handle data in kwargs
        if 'data' in kwargs:
            data = kwargs.pop('data')
            self.setData(data, **kwargs)
        if 'load' in kwargs:
            kwargs.pop('load')
            fileName = kwargs.pop('fileName')
            self.loadDB(fileName, **kwargs)


    def setData(self, data, **kwargs):
        if kwargs.pop('dataFrame', True) and data == type(pd.DataFrame()):
            self._data = data
        else:
            self._data = pd.DataFrame(data=data, **kwargs)

    def getData(self):
        return self._data

    def addDataEntry(self, dataPoint):
        #TODO - not working
        """Add dataPoint to DB"""
        idx = len(self._data) + 1
        while idx not in self._data.index:
            idx += 1
        self._data.loc[idx] = dataPoint

    def catDB(self, other):
        #TODO - not working
        """Concatenate DB to current DB"""
        idx = len(self._data) + 1
        for odx in other.index:
            while idx not in self._data.index:
                idx += 1
            self._data.loc[idx] = other.loc[odx]

    def saveDB(self, **kwargs):
        """Save Database using pandas tools"""
        fileType = kwargs.pop('fileType', 'pickle').lower()
        fileName = kwargs.pop('fileName', './cardDB.pickle')
        if 'pickle' in fileType:
            return self._data.to_pickle(fileName)
        elif 'csv' in fileType:
            return self._data.to_csv(fileName, **kwargs)
        if 'xls' in fileType:
            return self._data.to_excel(fileName, **kwargs)

    def loadDB(self, fileName, **kwargs):
        """Load Database using pandas tools"""
        fileType = kwargs.pop('fileType', 'pickle').lower()
        if 'pickle' in fileType:
            self._data = pd.read_pickle(fileName)
        elif 'csv' in fileType:
            self._data = pd.read_csv(fileName, **kwargs)
        if 'xls' in fileType:
            self._data = pd.read_excel(fileName, **kwargs)

    def __getattr__(self, item):
        return getattr(self._data, item)
