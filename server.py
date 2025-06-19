import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value):
        self.key = key
        self.value = value
        # id and seq is used for identify unique requests
        self.client_id = None
        self.seq = None

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key
        # id and seq is used for identify unique requests
        self.client_id = None
        self.seq = None

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.
        self.store = {} # in memory key -> value
        self.cache = {} # track all the replies ever sent 

    def Get(self, args: GetArgs):
        reply = GetReply(None)

        # Your code here.
        key = (args.client_id, args.seq)
        with self.mu: # lock once per request 
            if key in self.cache: # if seen the same id and seq before, just return cached
                return self.cache[key]
            val = self.store.get(args.key, "") # else cache args.key then return it
            reply = GetReply(val)
            self.cache[key] = reply

        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply("")

        # Your code here.
        key = (args.client_id, args.seq)
        with self.mu:
            if key in self.cache:
                return self.cache[key]
            self.store[args.key] = args.value # overwrite local value
            reply = PutAppendReply("")
            self.cache[key] = reply

        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.
        key = (args.client_id, args.seq)
        with self.mu:
            if key in self.cache:
                return self.cache[key]
            old = self.store.get(args.key, "") # read exisiting
            new = old + args.value # add provided 
            self.store[args.key] = new 
            reply = PutAppendReply(old) 
            self.cache[key] = reply

        return reply
