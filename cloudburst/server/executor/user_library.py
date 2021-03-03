#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import zmq

import cloudburst.server.utils as sutils
from cloudburst.shared.serializer import Serializer
# from ephekvs.client import KVSClient
import cloudburst.libPyCpp as libPyCpp

serializer = Serializer()


class AbstractCloudburstUserLibrary:
    # Stores a lattice value at ref.
    def put(self, ref, ltc):
        raise NotImplementedError

    # Retrives the lattice value at ref.
    def get(self, ref, deserialize=True):
        raise NotImplementedError

    # Sends a bytestring message to the specified destination.
    # TODO: type and format for destination ID?
    def send(self, dest, bytestr):
        raise NotImplementedError

    # Receives messages sent by send() to this function.
    # Receives all outstanding messages as a list [(sender id,
    # bytestring message), ...]
    def recv(self):
        raise NotImplementedError

class CloudburstUserLibrary(AbstractCloudburstUserLibrary):

    # ip: Executor IP.
    # tid: Executor thread ID.
    # anna_client: The Anna client, used for interfacing with the kvs.
    def __init__(self, context, pusher_cache, ip, tid, anna_client, has_ephe=False):
        self.executor_ip = ip
        self.executor_tid = tid
        self.anna_client = anna_client

        self.pusher_cache = pusher_cache

        self.address = sutils.BIND_ADDR_TEMPLATE % (sutils.RECV_INBOX_PORT +
                                                    self.executor_tid)

        # Socket on which inbound messages, if any, will be received.
        self.recv_inbox_socket = context.socket(zmq.PULL)
        self.recv_inbox_socket.bind(self.address)

        self.has_ephe = has_ephe
        # if has_ephe:
        #     self.ephe_client = KVSClient(thread_id=tid, context=context)
        #     self.session = None
    
    def gen_test_str(self, size):
        test_str = libPyCpp.gen_str(size)
        return test_str
    
    def put_test_str(self, size):
        res = libPyCpp.kvs_put(self.executor_tid, size)
        return
    
    def get_test_str(self, size):
        res = libPyCpp.kvs_get_str(self.executor_tid, size)
        return res

    def put(self, ref, value, use_session=False, durable=True):
        if durable or not self.has_ephe:
            return self.put_anna(ref, value, use_session=use_session)
        else:
            return self.put_ephe(ref, value, use_session=use_session)

    def get(self, ref, deserialize=True, durable=True):
        if durable or not self.has_ephe:
            return self.get_anna(ref, deserialize=deserialize)
        else:
            return self.get_ephe(ref)

    def put_ephe(self, bucket_key, value, use_session=False):
        # ref should be (bucket, key, session) tuple if it is a bucket key, otherwise it is a list
        # if type(bucket_key) == list and type(value) == list:
        #     results = []
        #     for b_k_s, v in zip(bucket_key, value):
        #         cur_session = self.session if use_session else b_k_s[2]
        #         results.append(self.ephe_client.put(b_k_s[0], b_k_s[1], v, session=cur_session))
        #     return results
        # else:
        #     cur_session = self.session if use_session else bucket_key[2]
        #     return self.ephe_client.put(bucket_key[0], bucket_key[1], value, session=cur_session)
        return

    def get_ephe(self, bucket_key):
        # if type(bucket_key) == list:
        #     return [self.ephe_client.get(b_k_s[0], b_k_s[1], session=b_k_s[2]) for b_k_s in bucket_key]
        # else:
        #     return self.ephe_client.get(bucket_key[0], bucket_key[1], session=bucket_key[2])
        return

    def put_anna(self, ref, value, use_session=False):
        if use_session:
            if type(ref) == list:
                ref = ['{}_{}'.format(r, self.session) for r in ref]
            else:
                ref = '{}_{}'.format(ref, self.session)
        return self.anna_client.put(ref, serializer.dump_lattice(value))

    def get_anna(self, ref, deserialize=True):
        if type(ref) != list:
            refs = [ref]
        else:
            refs = ref

        kv_pairs = self.anna_client.get(refs)
        result = {}

        # Deserialize each of the lattice objects and return them to the
        # client.
        for key in kv_pairs:
            if kv_pairs[key] is None:
                # If the key is not in the kvs, we can just return None.
                result[key] = None
            else:
                if deserialize:
                    result[key] = serializer.load_lattice(kv_pairs[key])
                else:
                    result[key] = kv_pairs[key].reveal()

        if type(ref) == list:
            return result
        else:
            return result[ref]

    def getid(self):
        return (self.executor_ip, self.executor_tid)

    # dest is currently (IP string, thread id int) of destination executor.
    def send(self, dest, bytestr):
        ip, tid = dest
        dest_addr = sutils.get_user_msg_inbox_addr(ip, tid)
        sender = (self.executor_ip, self.executor_tid)

        socket = self.pusher_cache.get(dest_addr)
        socket.send_pyobj((sender, bytestr))

    # We see if any messages have been sent to this thread. We return an empty
    # list if there are none.
    def recv(self):
        res = []
        while True:
            try:
                # We pass in zmq.NOBLOCK here so that we only check for
                # messages that have already been received.
                msg = self.recv_inbox_socket.recv_pyobj(zmq.NOBLOCK)
                res.append(msg)
            except zmq.ZMQError as e:
                # ZMQ will throw an EAGAIN error with a timeout if there are no
                # pending messages. If that's the case, that means that there
                # are no more messages to be received.
                if e.errno == zmq.EAGAIN:
                    break
                else:
                    raise e

        return res

    def close(self):
        # Closes the context for this request by clearing any outstanding
        # messages.
        self.recv()