#!/usr/bin/python2.6

"""
Reads stats from cfstats and push them to graphite

====
Usage
====

.. code-block:: bash
    cassandra2graphite host prefix graphite_host graphite_port

The metrics have the following format: 'prefix.host.cassandra.Keyspace.ColumnFamily.Key value timestamp'

"""

import socket
import subprocess
import sys
import time


def is_digit(d):
    """
    Check if the value is a digit

    Args:
        @param d: The value to check
        @type d: mixed

    Returns:
        bool

    """
    try:
        float(d)
    except ValueError:
        return False
    return True


def get_cfstats(host):
    """
    Get the nodetool cfstat output

    Args:
        @param host: the host to connect to
        @type host: string

    Returns:
        The cfstats output
    """
    p = subprocess.Popen(["nodetool", "-h", host, "cfstats"], stdout=subprocess.PIPE)
    return p.stdout


def parse(f):
    """
    Parse the cfstats output

    Args:
        @param f: The cfstats output
        @type f: string or anything that accept readline()

    Returns:
        dict

    """
    values = {}
    keyspace = None
    while True:
        line = f.readline()
        if not line:
            break
        s = line.split()
        if not s:
            continue
        if s[0].startswith('Keyspace'):
            keyspace = s[1]
            parse_keyspace(f, values, keyspace)
        if s[0].startswith('Column'):
            cf = s[2]
            parse_cf(f, values, keyspace, cf)
    return values


def parse_keyspace(f, values, keyspace):
    """
    Parse the 'Keyspace' section of the output

    Args:
        @param f: The cfstats output
        @type f: string or anything that accept readline()
        @param values: A dict where the result will go
        @type values: dict
        @param keyspace: The keyspace being parsed
        @type keyspace: string

    Returns:
        void

    """
    values[keyspace] = {'global': {}}
    for i in xrange(0, 5):
        line = f.readline()
        s = line.split()
        add_value(s, values[keyspace]['global'])


def parse_cf(f, values, keyspace, cf):
    """
    Parse the ColumnFamily section of the output

    Args:
        @param f: The cfstats output
        @type f: string or anything that accept readline()
        @param values: A dict where the result will go
        @type values: dict
        @param keyspace: The keyspace being parsed
        @type keyspace: string
        @param cf: The column family being parsed
        @type cf: string

    Returns:
        void

    """
    values[keyspace][cf] = {}
    while True:
        line = f.readline()
        if not line:
            break
        s = line.split()
        if not s:
            break
        s = line.split()
        add_value(s, values[keyspace][cf])


def add_value(s, values):
    """
    Add a value to the final result. This takes care of values ending with 'ms', 'NaN', lines containing parenthesis

    Args:
        @param s: The line being processed, splitted
        @param s: array
        @param values: A dict where the result will go
        @type values: dict

    Returns:
        void

    """
    for i, k in enumerate(s):
        s[i] = k.replace('(', '').replace(')', '')

    if s[-1] == 'ms.':
        s = s[:-1]

    if s[-1] == 'NaN':
        s[-1] = '0'

    if is_digit(s[-1]):
        k = '_'.join(s[0:-1]).replace(':', '')
        values[k] = s[-1]


def to_graphite(values, prefix, namespace=''):
    """
    Creates an array of graphite metric strings

    Args:
        @param values: Parsed output to send to graphite
        @type values: dict
        @param prefix: The prefix for the graphite metric
        @type prefix: string
        @param namespace: The current namespace. This accumulates Keyspace / ColumnFamily during the recursion.
        @type namespace: string

    Returns:
        array
    """
    results = []
    now = time.time()
    for k, v in values.iteritems():
        if type(v) is dict:
            r = to_graphite(v, prefix, namespace + '.' + k)
            results += r
        else:
            if namespace:
                results.append("%s.cassandra%s.%s %s %d" % (prefix, namespace, k, v, now))
            else:
                results.append("%s.cassandra%s %s %d" % (prefix, k, v, now))
    return results


def send_to_graphite(host, port, results):
    """
    Sends to graphite

    Args:
        @param host: The graphite host
        @type host: string
        @param port: The graphite port
        @type port: string or int
        @param results: The output of :func:`to_graphite`
        @type results: array

    Returns:
        void
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, int(port)))
    for r in results:
        s.send(r + '\n')


def main(argv=None):
    """
    Main
    """
    f = get_cfstats(argv[0])

    values = parse(f)
    prefix = argv[1] + '.' + socket.gethostbyaddr(argv[0])[0].replace('.', '_')
    results = to_graphite(values, prefix)
    send_to_graphite(argv[2], argv[3], results)


if __name__ == "__main__":
    main(sys.argv[1:])
