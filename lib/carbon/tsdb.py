from abc import ABCMeta, abstractmethod

# class TSDB is a generic DB layer to support graphite. Plugins can provide an implementation satisfying the following functions
# by configuring DB_MODULE, DB_INIT_FUNC and DB_INIT_ARG

# the global variable APP_DB will be initialized as the return value of DB_MODULE.DB_INIT_FUNC(DB_INIT_ARG)
# we will throw an error if the provided value does not implement our abstract class DB below

class TSDB:
    __metaclass__ = ABCMeta

    # returns info for the underlying db (including 'aggregationMethod')

    # info returned in the format
    #info = {
    # 'aggregationMethod' : aggregationTypeToMethod.get(aggregationType, 'average'),
    # 'maxRetention' : maxRetention,
    # 'xFilesFactor' : xff,
    # 'archives' : archives,
    #}
    # where archives is a list of
    # archiveInfo = {
    # 'offset' : offset,
    # 'secondsPerPoint' : secondsPerPoint,
    # 'points' : points,
    # 'retention' : secondsPerPoint * points,
    # 'size' : points * pointSize,
    #}
    #
    @abstractmethod
    def info(self, metric):
        pass

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur. If None, the existing xFilesFactor in path will not be changed
    @abstractmethod
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        pass

    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    @abstractmethod
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        pass


    # datapoints is a list of (timestamp,value) points
    @abstractmethod
    def update_many(self, metric, datapoints):
        pass

    # returns True or False
    @abstractmethod
    def exists(self, metric):
        pass

    # yields a generator of graphite.node.Node, either BranchNode or LeafNode. You'll need to make a Reader implementation to return with BranchNodes.
    # query is a is a graphite.storage.FindQuery
    @abstractmethod
    def find_nodes(self, query):
        pass
