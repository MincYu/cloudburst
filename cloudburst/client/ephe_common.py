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

f_elb = 'ac487ffb575ab4bb983e05215e756bd1-1931834692.us-east-1.elb.amazonaws.com'
my_ip = '50.17.47.24'
timeout = 10

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)
