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

f_elb = 'a8e920957d21549a694d6d2fd1e1b714-2061365145.us-east-1.elb.amazonaws.com'
my_ip = '34.224.41.88'
timeout = 10

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)
