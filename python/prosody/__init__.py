import logging

from .handler import EventHandler, ProsodyHandler
from .prosody import Context, Message, ProsodyClient

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
