import platform


# global code for platform determination.
# caches platform information for next call
class Platform:
    (Unknown, Linux, Windows, Other) = range(0, 4)
_platform = Platform.Unknown


def getPlatform():
    global _platform
    if _platform == Platform.Unknown:
        if platform.system() == "Windows":
            _platform = Platform.Windows
        elif platform.system() == "Linux":
            _platform = Platform.Linux
        else:
            _platform = Platform.Other
            
    return _platform

        

def daemonize():
    if getPlatform() == Platform.Windows:
        pass
    else:
        from twisted.scripts._twistd_unix import daemonize
        twisted.scripts._twistd_unix.daemonize()


def getpwnam(user):
    if getPlatform() == Platform.Windows:
        return "w"
    else:
        return pwd.getpwnam(user)[2:4]


class rusage_struct:
    ru_utime = 0
    ru_stime = 0

    def __init(self):
        pass
    

def getrusage(who):
    if getPlatform() == Platform.Windows:
        r = rusage_struct
        return r
    else:
        import resource
        return resource.getrusage(who)


def getCpuUsage():
    if getPlatform() == Platform.Windows:
        return 1
    else:
        global lastUsage, lastUsageTime

        rusage = getrusage(RUSAGE_SELF)
        currentUsage = rusage.ru_utime + rusage.ru_stime
        currentTime = time.time()

        usageDiff = currentUsage - lastUsage
        timeDiff = currentTime - lastUsageTime

        if timeDiff == 0: #shouldn't be possible, but I've actually seen a ZeroDivisionError from this
            timeDiff = 0.000001

        cpuUsagePercent = (usageDiff / timeDiff) * 100.0

        lastUsage = currentUsage
        lastUsageTime = currentTime

        return cpuUsagePercent

RUSAGE_SELF = 0

def sysconf(str):
    if getPlatform() == Platform.Windows:
        if str == "SC_PAGESIZE":
            return 16384
    else:
        return os.sysconf(str)


def log_to_syslog(self, prefix):
    if getPlatform() == Platform.Windows:
        pass
    else:
        from twisted.python.syslog import SyslogObserver
        observer = SyslogObserver(prefix).emit
        def syslog_observer(event):
            event["system"] = event.get("type", "console")
            observer(event)
            self.observer = syslog_observer
