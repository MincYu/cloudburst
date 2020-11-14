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

f_elb = 'a62eeb33f6b3b43779ea9c09796b7c2e-1388118431.us-east-1.elb.amazonaws.com'
my_ip = '18.212.101.17'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)