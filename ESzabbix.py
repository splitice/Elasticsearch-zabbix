#!/usr/bin/env python
# Created by Aaron Mildenstein on 19 SEP 2012
# Updated by Bjoern Puttmann to use the official python client library.

import elasticsearch
import sys
import logging

# Define the fail message
def zbx_fail():
    print "ZBX_NOTSUPPORTED"
    sys.exit(2)

logger = logging.getLogger()
ch = logging.StreamHandler(sys.stdout)
form = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
ch.setFormatter(form)
logger.addHandler(ch)
# If not in an interactive shell set log level to silent
if not sys.stdout.isatty():
    logger.setLevel(logging.CRITICAL)

es_node = "localhost"
es_port = 9200
es_use_ssl = False
es_http_auth = None

searchkeys = ['query_total', 'fetch_time_in_millis', 'fetch_total', 'fetch_time', 'query_current', 'fetch_current', 'query_time_in_millis']
getkeys = ['missing_total', 'exists_total', 'current', 'time_in_millis', 'missing_time_in_millis', 'exists_time_in_millis', 'total']
docskeys = ['count', 'deleted']
indexingkeys = ['delete_time_in_millis', 'index_total', 'index_current', 'delete_total', 'index_time_in_millis', 'delete_current']
storekeys = ['size_in_bytes', 'throttle_time_in_millis']
cachekeys = ['filter_size_in_bytes', 'field_size_in_bytes', 'field_evictions']
clusterkeys = searchkeys + getkeys + docskeys + indexingkeys + storekeys
returnval = None

# __main__

# We need to have two command-line args: 
# sys.argv[1]: The node name or "cluster"
# sys.argv[2]: The "key" (status, filter_size_in_bytes, etc)

if len(sys.argv) < 3:
    logger.error("Wrong number of arguments.")
    zbx_fail()

clients = {}
try:
    clients['default'] = elasticsearch.Elasticsearch( host=es_node,
                                        port=es_port,
                                        sniff_on_start=True,
                                        sniff_on_connection_fail=True,
                                        sniff_timeout=10,
                                        maxsize=20,
                                        use_ssl=es_use_ssl,
                                        http_auth=es_http_auth)
    clients['cluster'] = elasticsearch.client.ClusterClient(clients['default'])
    clients['node'] = elasticsearch.client.NodesClient(clients['default'])
except Exception, e:
    if sys.argv[2] == 'status':
	returnval = 0
    else:
        etype, evalue, etb = sys.exc_info()
        logger.error("Error while connection to %s. Exception: %s, Error: %s." % (es_node, etype, evalue))
        zbx_fail()

if sys.argv[1] == 'cluster':
    if sys.argv[2] in clusterkeys:
        nodestats = clients['node'].stats()
        subtotal = 0
        for node_key, node_data in nodestats['nodes'].iteritems():
            nodename = node_data['name']
            if sys.argv[2] in indexingkeys:
                indexstats = node_data['indices']['indexing']
            elif sys.argv[2] in storekeys:
                indexstats = node_data['indices']['store']
            elif sys.argv[2] in getkeys:
                indexstats = node_data['indices']['get']
            elif sys.argv[2] in docskeys:
                indexstats = node_data['indices']['docs']
            elif sys.argv[2] in searchkeys:
                indexstats = node_data['indices']['search']
            try:
                subtotal += indexstats[sys.argv[2]]
            except Exception, e:
                pass
        returnval = subtotal
    else:
        # Try to get a value to match the key provided
        try:
            returnval = clients['cluster'].health()[sys.argv[2]]
        except Exception, e:
            zbx_fail()
        # If the key is "status" then we need to map that to an integer
        if sys.argv[2] == 'status':
            if returnval == 'green':
                returnval = 0
            elif returnval == 'yellow':
                returnval = 1
            elif returnval == 'red':
                returnval = 2
            else:
                zbx_fail()

# Mod to check if ES service is up
elif sys.argv[1] == 'service':
    if sys.argv[2] == 'status':
        returnval = 1
    elif sys.argv[2] == 'master':
        state = clients['cluster'].state()
        master = state["master_node"]
        master_node = state["nodes"][master]
        returnval = master_node["name"]


else: # Get node specific data.
    nodestats = clients['node'].stats()
    for node_key, node_data in nodestats['nodes'].iteritems():
        if sys.argv[1] != node_data['name']:
            continue
        if sys.argv[2] in indexingkeys:
            stats = node_data['indices']['indexing']
        elif sys.argv[2] in storekeys:
            stats = node_data['indices']['store']
        elif sys.argv[2] in getkeys:
            stats = node_data['indices']['get']
        elif sys.argv[2] in docskeys:
            stats = node_data['indices']['docs']
        elif sys.argv[2] in searchkeys:
            stats = node_data['indices']['search']
        elif sys.argv[2] in cachekeys:
            stats = node_data['indices']['cache']
        try:
            returnval = stats[sys.argv[2]]
        except Exception, e:
            pass

# If we somehow did not get a value here, that's a problem.  Send back the standard
# ZBX_NOTSUPPORTED
if returnval is None:
    zbx_fail()
else:
    print returnval
