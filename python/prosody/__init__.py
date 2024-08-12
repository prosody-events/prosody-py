import logging

from .handler import EventHandler, ProsodyHandler
from .message import Message
from .prosody import Context, ProsodyClient

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
