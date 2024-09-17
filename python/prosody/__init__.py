import logging

from .errors import EventHandlerError, PermanentError, TransientError, permanent, transient
from .handler import EventHandler, ProsodyHandler
from .message import Message
from .prosody import Context, ProsodyClient

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
