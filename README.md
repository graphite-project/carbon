# Carbon

[![Build Status](https://secure.travis-ci.org/graphite-project/carbon.png?branch=master)](http://travis-ci.org/graphite-project/carbon)

Carbon is one of the components of [Graphite][], and is responsible for
receiving metrics over the network and writing them down to disk using a
storage backend. Currently [Whisper][] is our stable, supported backend and
[Ceres][] is the work-in-progress future replacement for Whisper.

[Graphite]: https://github.com/graphite-project
[Graphite Web]: https://github.com/graphite-project/graphite-web
[Whisper]: https://github.com/graphite-project/whisper
[Ceres]: https://github.com/graphite-project/ceres

## Overview

Client applications can connect to the running carbon-cache.py daemon on port
2003 (default) and send it lines of text of the following format:

    my.metric.name value unix_timestamp

For example:

    performance.servers.www01.cpuUsage 42.5 1208815315

- The metric name is like a filesystem path that uses a dot as a separator instead of
a forward-slash.

- The value is some scalar integer or floating point value 

- The unix_timestamp is unix epoch time, as an integer.

Each line like this corresponds to one data point for one metric.

Alternatively, they can send pickle-formatted messages to port 2004 (default)
which is considered faster than the line-based format.

Once you've got some clients sending data to carbon-cache, you can view
graphs of that data through the frontend [Graphite Web][] application.

## Running carbon-cache.py

First you must tell carbon-cache what user it should run as.  This must be a
user with write privileges to `$GRAPHITE_ROOT/storage/whisper`. Specify the
user account in `$GRAPHITE_ROOT/conf/carbon.conf`. This user must also have
write privileges to `$GRAPHITE_ROOT/storage/log/carbon-cache`

Alternatively, you can run `carbon-cache`/`carbon-relay`/`carbon-aggregator` as
[Twistd plugins][], for example:

    Usage: twistd [options] carbon-cache [options]
    Options:
          --debug       Run in debug mode.
      -c, --config=     Use the given config file.
          --instance=   Manage a specific carbon instance. [default: a]
          --logdir=     Write logs to the given directory.
          --whitelist=  List of metric patterns to allow.
          --blacklist=  List of metric patterns to disallow.
          --version     Display Twisted version and exit.
          --help        Display this help and exit.

Common options to `twistd(1)`, like `--pidfile`, `--logfile`, `--uid`, `--gid`,
`--syslog` and `--prefix` are fully supported and have precedence over
`carbon-*`'s own options. Please refer to `twistd --help` for the full list of
supported `twistd` options.

[Twistd plugins]: http://twistedmatrix.com/documents/current/core/howto/plugin.html

## Writing a client

First you obviously need to decide what data it is you want to graph with
graphite. The script [examples/example-client.py] demonstrates a simple client
that sends `loadavg` data for your local machine to carbon on a minutely basis.

The default storage schema stores data in one-minute intervals for 2 hours.
This is probably not what you want so you should create a custom storage schema
according to the docs on the [Graphite wiki][].

[Graphite wiki]: http://graphite.wikidot.com
[examples/example-client.py]: https://github.com/graphite-project/carbon/blob/master/examples/example-client.py


## Troubleshooting

Carbon uses SQLite as its persistence layer. SQLite is meant to be a lightweight database, and thus can’t support a high level of concurrency. OperationalError: database is locked errors indicate that your application is experiencing more concurrency than sqlite can handle in default configuration. This error means that one thread or process has an exclusive lock on the database connection and another thread timed out waiting for the lock the be released.  If you see “Database is locked” errors

Python’s SQLite wrapper has a default timeout value that determines how long the second thread is allowed to wait on the lock before it times out and raises the OperationalError: database is locked error.

If you’re getting this error, you can solve it by:

* Switching to another database backend. At a certain point SQLite becomes too “lite” for real-world applications, and these sorts of concurrency errors indicate you’ve reached that point.

* Rewriting your code to reduce concurrency and ensure that database transactions are short-lived.

* Increase the default timeout value by setting the timeout database option option:

         'OPTIONS': {
	         ...
             'timeout': 20,
             ...
          }

This will simply make SQLite wait a bit longer before throwing “database is locked” errors; it won’t really do anything to solve 
