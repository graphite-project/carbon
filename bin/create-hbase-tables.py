#!/usr/local/rnt/bin/python

from carbon import hbasedb
import argparse
from configobj import ConfigObj
from random import choice

def main(options):                      
    config = ConfigObj(options.config)
    config = config['HbaseDB']
    hbasedb.create_tables(host=choice(config['THRIFT_HOST_LIST']),
                          port=config['THRIFT_PORT'],
                          table_prefix=config['GRAPHITE_PREFIX'], 
                          transport=config['THRIFT_TRANSPORT_TYPE'],
                          protocol=config['THRIFT_PROTOCOL'])
 
def cli_opts():
    parser = argparse.ArgumentParser("Create HBase tables for Graphite")
    parser.add_argument('-c', '--config',                  
                        help='The config file to pull settings from',
                        action='store',
                        dest='config',
                        default='/usr/local/rnt/conf/graphite-db.conf')
    return parser.parse_args()
    
if __name__ == '__main__':                                                
    main(cli_opts())                                

