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

f_elb = 'a3555f3d301f14851b1ff7010b90ffc1-1062765033.us-east-1.elb.amazonaws.com'
my_ip = '54.226.1.250'
timeout = 10

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)
