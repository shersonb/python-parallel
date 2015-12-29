#!/usr/bin/env python
import iqueue as queue
import sys
import threading
import socket
import pickle
import os
import sys
import traceback
import time

__version__ = "1.0"

def timestamp(t=None):
    t = time.time() if t is None else t
    ms = 1000 * t % 1000
    ymdhms = time.localtime(t)
    tz = time.altzone if ymdhms.tm_isdst else time.timezone
    sgn = "-" if tz >= 0 else "+"
    return "%04d-%02d-%02d %02d:%02d:%02d.%03d%s%02d:%02d" % (ymdhms[:6] + (1000 * t % 1000, sgn, abs(tz) / 3600, abs(tz) / 60 % 60))


def detectCPUs():
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if "SC_NPROCESSORS_ONLN" in os.sysconf_names.keys():
            # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
        else:  # OSX:
            return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    # Windows:
    if "NUMBER_OF_PROCESSORS" in os.environ.keys():
        ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
        if ncpus > 0:
            return ncpus
    return 1  # Default


class BaseTaskClass(object):

    def __init__(self, **args):
        self.args = args
        self.results = {}
        self.lock = threading.RLock()
        self.status = threading.Condition(self.lock)
        self.jobsdone = 0
        self.jobcount = 1

    def postproc(self, v):
        (x, y) = v[:2]
        self.results[x] = y
        with self.status:
            self.jobsdone += 1
            self.status.notifyAll()

    def __iter__(self):
        return range(0)

    def f(self, **kwargs):
        return None


class ComputationThread(threading.Thread):

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        self.jobmanager.logwrite("%s started." % self)
        try:
            while True:
                x = self.jobmanager.inqueue.get()
                with self.jobmanager.lock:
                    # print(x)
                    count = self.jobmanager.pending.count(x)
                    if count == 0:
                        self.jobmanager.pending.append(x)
                try:
                    if type(x) == tuple:
                        y = self.jobmanager.currentjob.f(*x)
                    elif type(x) == dict:
                        y = self.jobmanager.currentjob.f(**x)
                    else:
                        y = self.jobmanager.currentjob.f(x)
                except:
                    self.jobmanager.logerror("Exception in %s thread!" % self)
                    self.jobmanager.interrupt()
                    raise
                self.jobmanager.outqueue.put((x, y))
                self.jobmanager.inqueue.task_done()
        finally:
            with self.jobmanager.lock:
                self.jobmanager.procthreads.remove(self)
            self.jobmanager.logwrite("%s terminated." % self)


class JobManager(threading.Thread):

    def __init__(self, logfile=sys.stderr, threads=-1):
        self.jobqueue = queue.Queue()
        self.inqueue = queue.Queue()
        self.outqueue = queue.Queue()
        self.errqueue = queue.Queue()
        self.logfile = logfile
        self.loglock = threading.Lock()
        self.pending = []
        self.currentjob = None
        self.lock = threading.Lock()
        self.procthreads = [ComputationThread(self) for k in range(
            detectCPUs() if threads == -1 else threads)]
        self.connections = []
        self.postprocthread = threading.Thread(target=self.postproc)
        self.postprocthread.daemon = True
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()
        self.postprocthread.start()

    def logwrite(self, *lines):
        """logwrite(*lines)

        Writes one or more line to the log file, signed with a timestamp."""
        with self.loglock:
            ts = timestamp()
            for line in lines:
                print >>self.logfile, u"%s %s" % (ts, line)
            self.logfile.flush()

    def logerror(self, *lines):
        """logerror(*lines)

        Prints lines and traceback sys.stderr and to the log file."""
        exc, excmsg, tb = sys.exc_info()
        lines = lines + tuple(traceback.format_exc().split("\n"))

        # Print to log AND stderr
        self.logwrite(*[u"!!! {line}".format(**vars()) for line in lines])
        for line in lines:
            print >>sys.stderr, line

    def run(self):
        while True:
            self.currentjob = self.jobqueue.get()
            self.logwrite("*** Accepted job: %s" % self.currentjob)
            with self.currentjob.status:
                self.currentjob.running = True
                self.currentjob.status.notifyAll()
            self.currentjob.populatequeue(self.inqueue)
            self.logwrite("*** Queue populated: %s" % self.currentjob)
            self.inqueue.join()
            self.outqueue.join()

    def postproc(self):
        while True:
            (x, y) = self.outqueue.get()
            try:
                self.currentjob.postproc((x, y))
            except:
                self.logerror("Exception in postproc thread!")
                self.interrupt()
                raise
            with self.lock:
                if x in self.pending:
                    self.pending.remove(x)
            self.outqueue.task_done()

    def interrupt(self):
        self.jobqueue.interrupt()
        self.inqueue.interrupt()
        self.outqueue.interrupt()
        with self.currentjob.status:
            self.currentjob.running = False
            self.currentjob.status.notifyAll()
