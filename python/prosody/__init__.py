import logging

from prosody.prosody import ProsodyClient

from prosody.context import Context
from prosody.errors import EventHandlerError, PermanentError, TransientError, permanent, transient
from prosody.handler import EventHandler, ProsodyHandler
from prosody.message import Message
from prosody.timer import Timer

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
