import pandas as pd
from abc import ABC
# TODO
# 1. Issue with a db of 1, getData is doubling the index
# 2. issue with db values that are not lists -> must force a list
# 3. test NoneType dicts
# 4. indexing wrong getting key len not val len
# 5. catDb not working

# Load and save not tested


class genericDB(ABC):
    """
    Generic database structure to be inherited to specific database structures
    """
    def __init__(self, data=None, load=None, keyValues=None, **kwargs):
        self._data = {}
        self._index = []
        self._keyValues = tuple()

        if keyValues is not None:
            self.setData({key: [] for key in keyValues})
        else:
            self.setData(data=data)
        # Handle data in kwargs
        if load is not None:
            fileName = kwargs.pop('fileName')
            self.loadDB(fileName, **kwargs)

    def setData(self, data=None, **kwargs):
        if kwargs.pop('DataFrame', False):
            self._data = pd.DataFrame(data=data, **kwargs)
        else:
            self._data = data
            self._setIndex()
            self._setKeyValues()

    def getData(self, dataFrame=True):
        if dataFrame:
            return pd.DataFrame(self._data, index=self._index)
        else:
            return self._data

    def _setIndex(self):
        try:
            self._index = range(len(self.getData(False)))
        except TypeError:
            self._index = range(0)

    def _setKeyValues(self):
        try:
            self._keyValues = tuple(self.getData(False).keys())
        except AttributeError:
            self._keyValues = tuple()

    def getKeyValues(self):
        return self._keyValues

    def addDataEntry(self, dataPoint):
        """Add dataPoint to DB"""
        for k, v in dataPoint.items():
            if k not in self.getKeyValues():
                raise AttributeError('Key {key} not found!'.format(key=k))
            self._data[k].append(v)
            self._setIndex()

    def catDB(self, other):
        """Concatenate DB to current DB"""
        otherData = other.getData(dataFrame=False)
        otherKeys = other.getKeyValues()
        if otherKeys != self.getKeyValues():
            raise AttributeError('KeyValue mismatch!')
        while otherData[otherKeys[0]] > 0:
            newDataPoint = {}
            for k in otherKeys:
                newDataPoint[k] = otherData[k].pop(0)
            self.addDataEntry(newDataPoint)

    def saveDB(self, fileName='./cardDB.pickle', fileType='pickle', **kwargs):
        """Save Database using pandas tools"""
        fileType = fileType.lower()
        if 'pickle' in fileType:
            return self.getData(False).to_pickle(fileName)
        elif 'csv' in fileType:
            return self.getData().to_csv(fileName, **kwargs)
        if 'xls' in fileType:
            return self.getData().to_excel(fileName, **kwargs)

    def loadDB(self, fileName, fileType='pickle', **kwargs):
        """Load Database using pandas tools"""
        fileType = fileType.lower()
        if 'pickle' in fileType:
            self.setData(pd.read_pickle(fileName))
        elif 'csv' in fileType:
            self.setData(pd.read_csv(fileName, **kwargs))
        if 'xls' in fileType:
            self.setData(pd.read_excel(fileName, **kwargs))

    def __getattr__(self, item):
        return getattr(self._data, item)
