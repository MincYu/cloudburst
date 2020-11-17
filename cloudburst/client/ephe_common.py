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

f_elb = 'a47076cce314c4f679ce15e54400e656-831255056.us-east-1.elb.amazonaws.com'
my_ip = '54.145.198.45'
timeout = 5

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)