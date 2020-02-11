from twisted.cred import portal, checkers
from twisted.conch.ssh import keys
from twisted.conch.checkers import SSHPublicKeyDatabase
from twisted.conch.manhole import Manhole
from twisted.conch.manhole_ssh import TerminalRealm, ConchFactory
from twisted.conch.openssh_compat.factory import OpenSSHFactory
from twisted.internet import reactor
from twisted.application.internet import TCPServer

from carbon.protocols import CarbonServerProtocol
from carbon.conf import settings
import carbon

from carbon.exceptions import CarbonConfigException

namespace = {'carbon': carbon}


class PublicKeyChecker(SSHPublicKeyDatabase):
  def __init__(self, userKeys):
    self.userKeys = {}
    for username, keyData in userKeys.items():
      self.userKeys[username] = keys.Key.fromString(data=keyData).blob()

  def checkKey(self, credentials):
    if credentials.username in self.userKeys:
      keyBlob = self.userKeys[credentials.username]
      return keyBlob == credentials.blob


def createManholeListener():
  sshRealm = TerminalRealm()
  sshRealm.chainedProtocolFactory.protocolFactory = lambda _: Manhole(namespace)

  if settings.MANHOLE_PUBLIC_KEY == 'None':
    credChecker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
    credChecker.addUser(settings.MANHOLE_USER.encode('utf-8'),
                        ''.encode('utf-8'))
  else:
    userKeys = {
        settings.MANHOLE_USER.encode('utf-8'):
            settings.MANHOLE_PUBLIC_KEY.encode('utf-8'),
    }
    credChecker = PublicKeyChecker(userKeys)

  sshPortal = portal.Portal(sshRealm)
  sshPortal.registerChecker(credChecker)
  sessionFactory = ConchFactory(sshPortal)

  # set ssh host keys
  if settings.MANHOLE_HOST_KEY_DIR == "":
    raise CarbonConfigException("MANHOLE_HOST_KEY_DIR not defined")
  openSSHFactory = OpenSSHFactory()
  openSSHFactory.dataRoot = settings.MANHOLE_HOST_KEY_DIR
  sessionFactory.publicKeys = openSSHFactory.getPublicKeys()
  sessionFactory.privateKeys = openSSHFactory.getPrivateKeys()

  return sessionFactory


def start():
    sessionFactory = createManholeListener()
    reactor.listenTCP(settings.MANHOLE_PORT, sessionFactory, interface=settings.MANHOLE_INTERFACE)


class ManholeProtocol(CarbonServerProtocol):
  plugin_name = "manhole"

  @classmethod
  def build(cls, root_service):
    if not settings.ENABLE_MANHOLE:
      return

    factory = createManholeListener()
    service = TCPServer(
      settings.MANHOLE_PORT,
      factory,
      interface=settings.MANHOLE_INTERFACE)
    service.setServiceParent(root_service)
