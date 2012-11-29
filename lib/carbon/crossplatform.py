import platform



def daemonize():
    if platform.system() == "Windows":
        pass
    else:
        from twisted.scripts._twistd_unix import daemonize
        twisted.scripts._twistd_unix.daemonize()


def getpwnam(user):
    if platform.system() == "Windows":
        pass
    else:
        pwd.getpwnam(user)[2:4]


class rusage_struct:
    ru_utime = 0
    ru_stime = 0

    def __init(self):
        pass
    

def getrusage(who):
    if platform.system() == "Windows":
        r = rusage_struct
        return r
    else:
        import resource
        return resource.getrusage(who)


RUSAGE_SELF = 0

def sysconf(str):
    if platform.system() == "Windows":
        if str == "SC_PAGESIZE":
            return 16384
    else:
        return os.sysconf(str)


def log_to_syslog(self, prefix):
    if platform.system() == "Windows":
        pass
    else:
        from twisted.python.syslog import SyslogObserver
        observer = SyslogObserver(prefix).emit
        def syslog_observer(event):
            event["system"] = event.get("type", "console")
            observer(event)
            self.observer = syslog_observer
