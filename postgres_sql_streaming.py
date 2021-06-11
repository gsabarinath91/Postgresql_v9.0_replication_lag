# !/usr/bin/python
# Author  : Sabarinath Gnanasekar
# This script produce the result of replication lag between master postgresql and replica in measurable of bytes.And the sole purpose of this code bound with
# postgresql version 9.0 running with streaming replication which doesn't have any in-built function to measure and alert the replication lag as human unders
# tandable
 
 
import logging
import sys
import math
import psycopg2 as pgsql
 
###Logging function to produce verbose information about script
 
LEVELS={'debug':logging.DEBUG,'info':logging.INFO,'warning':logging.WARNING,'error':logging.ERROR,'critical':logging.CRITICAL}
 
def Logging(level,data,mod_name):
    Level = LEVELS.get(level)
    logger = logging.getLogger(mod_name)
    if not len(logger.handlers):
        logger.setLevel(Level)
        file_handler = logging.FileHandler('replication-lag.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s -%(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    logger.log(Level,data)
 
 
###Math Functions to convert Hex to decimal and convert to unit of bytes
 
def getDecimalEquivalent(hexadigit) :
        hexaDict = {'0' : 0, '1' : 1, '2' : 2 , '3' : 3, '4' : 4, '5' : 5 , '6' : 6 , '7' : 7, '8' : 8 , '9' : 9, 'A' : 10, \
                   'B' : 11, 'C' : 12, 'D' : 13, 'E' : 14, 'F' : 15}
 
        return hexaDict.get( str(hexadigit) )
 
 
def convertHex2Dec (hexa) :
        dec = 0
        counter = 1
        try:
            for hexadigit in str(hexa).upper() :
                    dec = dec + getDecimalEquivalent(hexadigit) * 16 ** ( len(hexa) - counter )
                    counter += 1
        except TypeError:
            dec = 0
        return dec
 
def computeMegaByteDiff (master , replica ) :
        multiplier = convertHex2Dec('FF000000')
        master_val = multiplier * convertHex2Dec( str(master[0][0]).split('/')[0] )  + convertHex2Dec( str(master[0][0]).split('/')[1])
        Logging('info','Master postgresql current transaction log write location %s in bytes'%(master_val),'postgres')
        try:
            slave_val  = multiplier * convertHex2Dec( str(replica[0][0]).split('/')[0] )  + convertHex2Dec( str(replica[0][0]).split('/')[1] )
        except IndexError:
            slave_val = 0
        Logging('info','Slave postgresql latest transaction replay log write location %s in bytes'%(slave_val),'postgres')
        byte = master_val - slave_val
        Logging('info','Replica postgresql is lag by %s in bytes'%(byte),'postgres')
        return byte
 
###Database Functions to connect with MAster and slave Postgres to grab query output
 
def pgsql_query_get( conn_str,query):
        pgsql_conn = pgsql.connect(conn_str)
        Logging('info','postgres connection established with %s'%(conn_str),'postgres')
        pgsql_cur = pgsql_conn.cursor()
        pgsql_cur.execute(query)
        Logging('info','%s Query has been executed against %s'%(query,conn_str),'postgres')
        result = pgsql_cur.fetchall()
        Logging('info','Data has been fetched and stored in the variable for further process','postgres')
        Logging('info','postgres connection closed with %s'%(conn_str),'postgres')
        return result
        pgsql_conn.close()
        pgsql_cur.close()
        #return result
 
 
###Calling All above said function
 
conn_mas = "dbname='postgres' user='postgres' host='127.0.0.1'"
conn_sla = "dbname='postgres' user='postgres' host='10.12.41.95' port='5433'"
query_mas = "SELECT pg_current_xlog_location()"
query_sla = "SELECT pg_last_xlog_replay_location()"
 
master = pgsql_query_get(conn_mas ,query_mas)
slave = pgsql_query_get(conn_sla ,query_sla)
 
print computeMegaByteDiff(master , slave)
