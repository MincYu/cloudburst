from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.serializer import Serializer

import logging
import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os

from cloudburst.server.benchmarks import utils
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

f_elb = 'af80c307e28174ab1b288fcf99b7f5f7-113220057.us-east-1.elb.amazonaws.com'
my_ip = '54.196.208.3'
timeout = 5

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)