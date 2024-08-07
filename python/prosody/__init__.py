import logging

from .handler import AbstractMessageHandler, TracingHandler
from .prosody import Context, Message, ProsodyClient

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
