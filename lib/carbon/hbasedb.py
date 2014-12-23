from json import loads as json_loads
from json import dumps as json_dumps
from time import time, sleep
from fnmatch import filter as fnmatch_filter
from struct import pack as struct_pack
from struct import unpack as struct_unpack
from socket import gethostbyname
from carbon.tsdb import TSDB
from carbon import util, log
from random import choice
from random import randrange
import happybase

try:
    from redis import StrictRedis
except Exception, e:
    redis = False

try:
    from msgpack import packb, unpackb
except Exception, e:
    msgpack = False
# we manage a namespace table (NS) and then a data table (data)

# the NS table is organized to mimic a tree structure, with a ROOT node containing links to its children.
# Nodes are either a BRANCH node which contains multiple child columns prefixed with c_,
# or a LEAF node containing a single NODE column so it has data

# IDCTR
#   - unique id counter

# ROOT
#   - c_branch1 -> m_branch1
#   - c_leaf1 -> m_leaf1

# branch1
#   - c_leaf2 -> m_branch1.leaf2

# leaf1
#    - NODE -> bool

# <leafname>_<floored hourly timestamp>
#    - t:<timestamp> -> "timestamp, value, retention seconds"

META_CF_NAME = 'tree'
DATA_CF_NAME = 'timestamp'
TAG_CF_NAME = 'tag'
META_TABLE_SUFFIX = 'meta'
DATA_TABLE_SUFFIX = 'data'

class HbaseTSDB(TSDB):
    def __init__(self, host_list, port, table_prefix, batch_size=1000,
                 transport='buffered', send_interval=60,
                 reset_interval=1800, protocol='binary'):
        self.host_list = [gethostbyname(host) for host in host_list]
        self.thrift_port = port
        self.transport_type = transport
        self.batch_size = batch_size
        self.send_interval = send_interval
        self.reset_interval = reset_interval + randrange(120)
        self.connection_retries = 3
        self.protocol = protocol
        self.table_prefix = table_prefix
        self.cache_ttl = 3600
        #use the reset function only for consistent connection creation
        self.__reset_conn(send_batch=False)
        if redis:
            self.redis_conn = redis.ConnectionPool(unix_socket_path = '/tmp/redis.sock')

    def __make_conn(self):
        try:
            del self.client
        except Exception, e:
            pass
        if self.host_list < 1:
            log.msg("Empty host list. Very bad! Very bad!")
            exit(2)
        self.thrift_host = choice(self.host_list)
        log.msg("Reconnecting to %s::%s" % (self.thrift_host, self.thrift_port))
        self.client = happybase.Connection(
            host=self.thrift_host,
            port=self.thrift_port,
            table_prefix=self.table_prefix,
            transport=self.transport_type,
            protocol=self.protocol,
            autoconnect=False
        )
        self.client.open()
        sleep(0.25)
        e = None
        try:
            res = len(self.client.tables())
        except Exception, e:
            res = 0
        #should remove non-responsive hosts
        if res < 1:
            try:
                del self.host_list[self.host_list.index(self.thrift_host)]
            except Exception, e:
                pass
        return res > 0, e

    def __reset_conn(self, send_batch=True):
        if send_batch:
            try:
                self.data_batch.send()
            except Exception, e:
                log.msg('Failed to send batch %s because %s' % (self.data_batch, e))
                pass

        for conn in xrange(self.connection_retries):
            res, e = self.__make_conn()
            if res:
                break
        else:
            log.msg('Cannot get connection to HBase because %s.' % e)
            exit(2)
        self.meta_table = self.client.table(META_TABLE_SUFFIX)
        self.data_table = self.client.table(DATA_TABLE_SUFFIX)
        self.data_batch = self.data_table.batch()
        self.send_time = time()
        self.reset_time = time()

    def __refresh_conn(self, wait_time=60):
        self.client.close()
        #try and refresh for 1 minute
        give_up_time = time() + wait_time
        log.msg('Connection failed to %s' % self.thrift_host)
        while time() < give_up_time:
            try:
                log.msg('Retrying connection to %s' % self.thrift_host)
                sleep(1)
                self.client.open()
                log.msg('Connection resumed...')
            except Exception, e:
                pass
            else:
                break
        else:
            self.__reset_conn()

    def __send(self):
        cur_time = time()
        if cur_time - self.reset_time > self.reset_interval:
            self.__reset_conn()
        elif cur_time - self.send_time > self.send_interval or \
                len(self.data_batch._mutations) > self.batch_size:
            try:
                self.data_batch.send()
            except Exception:
                self.__refresh_conn()
                self.data_batch.send()
            self.send_time = cur_time

    def __get_cached_row(self, row):
        res = None
        red_con = Redis(ConnectionPool=self.redis_conn)
        try:
            res = red_con.get(row)
        except Exception, e:
            log.exception("Can't connect to redis...skipping %s" % row)
        else:
            if msgpack:
                res = unpackb(res)
        return res

    def __put_cached_row(self, row, val):
        red_con = Redis(ConnectionPool=self.redis_conn)
        if msgpack:
            val = packb(val)
        try:
            red_con.set(row, val)
            red_con.expire(row, self.cache_ttl)
        except Exception, e:
            log.exception("Can't connect to redis...skipping %s" % row)

    def __get_row(self, row, column=None, send_batch=True):
        if time() - self.reset_time > self.reset_interval:
            self.__reset_conn(send_batch=send_batch)
        res = None
        if redis:
            res = self.__get_cached_row(row)
        if not res:
            try:
                if column:
                    res = self.meta_table.row(row, column)
                else:
                    res = self.meta_table.row(row)
            except Exception:
                self.__refresh_conn()

                if column:
                    res = self.meta_table.row(row, column)
                else:
                    res = self.meta_table.row(row)
            finally:
                if redis:
                    self.__put_cached_row(row, res)

        return res

    # archiveList, aggregationMethod, isSparse, doFallocate are silently dropped
    def create(self, metric, archiveList, xFilesFactor, aggregationMethod, 
               isSparse, doFallocate, tags=[]):
        column_name = "%s:NODE" % META_CF_NAME
        values = {column_name: 'True'}
        for tag in tags:
            tag_key = "%s:%s" % (TAG_CF_NAME, tag)
            values[tag_key] = 'True'
        self.meta_table.put(metric, values)
        counter_row = "%s:CTR" % META_CF_NAME
        self.meta_table.counter_inc('CTR', counter_row)
        metric_parts = metric.split('.')
        metric_key = ""
        metric_prefix = "%s:c_" % (META_CF_NAME)
        prefix_len = len(metric_prefix)
        for part in metric_parts:
            # if parent is empty, special case for root
            if metric_key == "":
                prior_key = "ROOT"
                metric_key = part
            else:
                prior_key = metric_key
                metric_key = "%s.%s" % (metric_key, part)

            # make sure parent of this node exists and is linked to us
            metric_name = "%s%s" % (metric_prefix, part)
            if metric_name == metric_prefix:
                continue
            parentLink = self.__get_row(prior_key,
                                        column=[metric_name])
            if len(parentLink) == 0:
                self.meta_table.put(prior_key, {metric_name:
                                                metric_key})

    def update_many(self, metric, points, retention_config):
        """Update many datapoints.

        Keyword arguments:
        metric -- the name of the metric to process
        points  -- Is a list of (timestamp,value) points
        retention_config -- the storage retentions (time per point, number of points) for this metric

        """
        current_points = []
        now = time()

        for point in points:
            (timestamp, value) = point
            hour = (int(timestamp) / 3600) * 3600
            rowkey = "%s:%d" % (metric, hour)
            age = now - timestamp
            # get retention seconds and set to correct retention amount
            try:
                reten = map(min, zip(*[r for r in retention_config if age < r[1]]))
            except ValueError, e:
                reten = retention_config[-1]
            colkey = "%s:%d" % (DATA_CF_NAME, timestamp)
            colval = "%d %f %d:%d" % (timestamp, value, reten[0], reten[1])
            self.data_batch.put(rowkey, {colkey: colval})
        self.__send()

    """
    def propagate(self, metric, hours, retention_config, rollup='average'):
        # make sure the write buffer is flushed
        self.__reset_conn()

        now = time()
        #number of seconds to look through
        deg_time = hours * 3600
        oldest_time = now - deg_time
        oldest_floor = (int(oldest_time) / 3600) * 3600
        try:
            reten = map(min, zip(*[r for r in retention_config if age < r[1]]))
        except ValueError, e:
            reten = retention_config[-1]
        startkey = "%s:%d" % (metric, oldest_floor)
        endkey = "%s:%d" % (metric, now + 10)
        scan = self.data_table.scan(row_start=startkey, row_stop=endkey,
                                    batch_size = self.batch_size)
        delete_cols = {}
        for row in scan:
            rollup_val = []
            #Hacky, but it should work
            #Basically, we assume the first timestamp we get 
            #fits the new retention because we need *a* value
            floored_timestamp = int(row[0].split(':')[1])
            new_timestamp = floored_timestamp
            #look through each column
            for col in row[1].values():
                (timestamp, value, reten) = col.split()
                timestamp = int(timestamp)
                value = float(value)
                step, num_points = [int(obj) for obj in reten.split(':')]
                ret_diff = num_points / step
                if timestamp % num_points == 0:
                    new_timestamp = timestamp
                if len(new_val) >= ret_diff:
                    rollup_val = util.aggregate(rollup, rollup_val)
                    val = "%d %f %d:%d" % (new_timestamp, rollup_val, points, num_points)
                    colkey = "%s:%d" % (DATA_CF_NAME, new_timestamp)
                    self.data_batch.put(row[0], {colkey, val})
                    rollup_val = [value] 
                    new_timestamp = floored_timestamp
                else:
                    rollup_val.append(value)
                    #According to devs, sending deletes and updates in the 
                    #same batch can be bad. We'll just hang on the "to be deleted"
                    #columns for later
                    delete_cols[row[0]] = col
            self.__send()
        #force a final send to empty batch
        self.__reset_conn()
        #Remove columns that were rolled up
        for row, col in delete_cols.iteritems():
            self.data_batch.delete(row, columns=[col])
            self.__send()
        self.__reset_conn()
    """

    def exists(self, metric):
        column_name = "%s:NODE" % META_CF_NAME
        try:
            res = self.__get_row(metric, column=[column_name])
    
            metric_exists = bool(res[column_name])
        except Exception, e:
            return False
        else:
            return metric_exists

    def delete(self, metric):
        #Make sure write buffer is empty
        self.__reset_conn()
        #Get data on metric
        if not self.exists(metric):
            return
        #we'll first get a scan object of all the data table rows associated
        scan = self.data_table.scan(row_prefix = "%s:" % metric)
        #then batch delete
        for row in scan:
            self.data_batch.delete(row[0])
            #since we only actually send when required, this is fine
            self.__send()
        #make sure there aren't any left over deletes
        self.__reset_conn()
        #now we remove the meta table row
        self.meta_table.delete(metric)
        counter_row = "%s:CTR" % META_CF_NAME
        self.meta_table.counter_dec('CTR', counter_row)

    def build_index(self, tmp_index):
        column_name = "%s:NODE" % META_CF_NAME
        scan = self.meta_table.scan(columns=[column_name])
        t = time()
        total_entries = 0
        for row in scan:
            if bool(row[1][column_name]):
                tmp_index.write('%s\n' % row[0])
                total_entries += 1
        tmp_index.flush()

        log.msg("[IndexSearcher] index rebuild took %.6f seconds (%d entries)" %
             (time() - t, total_entries))

def create_tables(host, port, table_prefix, transport, protocol):
    client = happybase.Connection(
        host=host,
        port=port,
        table_prefix=table_prefix,
        transport=transport,
        protocol=protocol
    )
    sleep(0.25)
    try:
        tables = client.tables()
    except Exception as e:
        print("HBase tables can't be retrieved. Cluster offline?")
        exit(1)

    meta_families = {META_CF_NAME: dict(compression="Snappy",
                                        block_cache_enabled=True,
                                        bloom_filter_type="ROWCOL",
                                        max_versions=1),
                     TAG_CF_NAME: dict(compression="Snappy",
                                       block_cache_enabled=True,
                                       bloom_filter_type="ROW",
                                       max_versions=1)}
    data_families = {DATA_CF_NAME: dict(compression="Snappy",
                                        block_cache_enabled=True,
                                        bloom_filter_type="ROW",
                                        max_versions=1)}
    if META_TABLE_SUFFIX in tables and DATA_TABLE_SUFFIX in tables:
        print('Both Graphite tables available!')
        return
    if META_TABLE_SUFFIX not in tables:
        client.create_table(META_TABLE_SUFFIX, meta_families)
        store_table = client.table(META_TABLE_SUFFIX)
        # add counter record
        counter_row = "%s:CTR" % META_CF_NAME
        store_table.counter_set("CTR", counter_row, 0)
        print('Created %s_%s!' % (table_prefix, META_TABLE_SUFFIX))
    if DATA_TABLE_SUFFIX not in tables:
        client.create_table(DATA_TABLE_SUFFIX, data_families)
        store_table = client.table(DATA_TABLE_SUFFIX)
        print('Created %s_%s!' % (table_prefix, DATA_TABLE_SUFFIX))

