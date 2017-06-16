import pandas as pd
from abc import ABC
## TODO
# 1. make a field instantiatior
# 2. use class._fields to act on self._data
# 3.

class genericDB(ABC):
    """
    Generic database structure to be inherited to specific database structures
    """
    ##Class Fields ##
    _fields = []

    ##Init##
    def __init__(self, *args, **kwargs):
        self._data = pd.DataFrame()
        if 'fields' in kwargs:
            self.setFields(*kwargs['fields'])
        if 'data' in kwargs:
            data = kwargs.pop('data')
            self.setData(data, **kwargs)
        if 'load' in kwargs:
            kwargs.pop('load')
            fileName = kwargs.pop('fileName')
            self.loadDB(fileName, **kwargs)


    def setData(self, data, **kwargs):
        self._data = pd.DataFrame(data=data, **kwargs)

    def getData(self):
        return self._data

    def setFields(self, *args):
        """Set class-based fields"""
        genericDB._fields = [arg for arg in args]

    def addToFields(self, *args):
        """Add field to class-based fields"""
        for arg in args:
            genericDB._fields.append(arg)

    def addDataEntry(self, *args, **kwargs):
        print('Not Instantiated!')
        pass

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
