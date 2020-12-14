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

f_elb = 'ab2f39babc74c43f5ba8fc20e374139b-1989531381.us-east-1.elb.amazonaws.com'
my_ip = '100.26.22.108'
timeout = 5

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)