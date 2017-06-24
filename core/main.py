import pandas as pd
from abc import ABC
## TODO

class genericDB(ABC):
    """
    Generic database structure to be inherited to specific database structures
    """
    ##Class Fields ##

    ##Init##
    def __init__(self, data=None, load=None, keyValues=None, **kwargs):
        self._data = {}
        self._index = []
        self._keyValues = tuple()
        # Handle data in kwargs
        if load is not None:
            fileName = kwargs.pop('fileName')
            self.loadDB(fileName, **kwargs)
        if keyValues is not None:
            self.setData({key: [] for key in keyValues})

    def setData(self, data=None, **kwargs):
        if kwargs.pop('DataFrame',False):
            self._data = pd.DataFrame(data=data, **kwargs)
        else:
            self._data = data
            self._setIndex()
            self._setKeyValues()

    def getData(self, dataFrame=True):
        if DataFrame:
            return pd.DataFrame(self._data, index=self._index)
        else:
            return self._data

    def _setIndex(self):
        self._index = range(len(self.getData(False)))

    def _setKeyValues(self):
        self._keyValues = tuple(self.getData(Flase).keys())

    def getKeyValues(self):
        return self._keyValues

    def addDataEntry(self, dataPoint):
        """Add dataPoint to DB"""
        for k,v in dataPoint.items():
            if k not in data.getKeyValues():
                raise AttributeError('Key {key} not found!'.format(key=k)
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
        fileType = filetype.lower()
        if 'pickle' in fileType:
            return self.getData(False).to_pickle(fileName)
        elif 'csv' in fileType:
            return self.getData().to_csv(fileName, **kwargs)
        if 'xls' in fileType:
            return self.getData().to_excel(fileName, **kwargs)

    def loadDB(self, fileName, filetype='pickle', **kwargs):
        """Load Database using pandas tools"""
        fileType = filetype.lower()
        if 'pickle' in fileType:
            self.setData(pd.read_pickle(fileName))
        elif 'csv' in fileType:
            self.setData(pd.read_csv(fileName, **kwargs))
        if 'xls' in fileType:
            self.setData(pd.read_excel(fileName, **kwargs))

    def __getattr__(self, item):
        return getattr(self._data, item)
