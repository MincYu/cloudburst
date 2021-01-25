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

f_elb = 'ac89ac36528324a3f959ac8e36d6992a-28381125.us-east-1.elb.amazonaws.com'
my_ip = '52.206.7.72'
timeout = 5

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)