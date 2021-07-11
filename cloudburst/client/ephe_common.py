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

f_elb = 'abfb3d2ffc99042128b5021106d4f94b-212926308.us-east-1.elb.amazonaws.com'
my_ip = '3.80.60.167'
timeout = 10

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)
