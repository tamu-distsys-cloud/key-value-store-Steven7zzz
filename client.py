import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.
        self.client_id = nrand()
        self.seq = 0
        self.mu = threading.Lock() # for concurrency 

        self.total_shards   = cfg.nservers
        self.replicas_per_shard = cfg.nreplicas

        # build each shard’s replica group with wrap around
        self.servers_by_shard: List[List[ClientEnd]] = []
        n = len(self.servers)
        for shard in range(self.total_shards):
            grp = []
            for r in range(self.replicas_per_shard):
                idx = (shard + r) % n
                grp.append(self.servers[idx])
            self.servers_by_shard.append(grp)

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # You will have to modify this function.
        args = GetArgs(key)
        with self.mu:
            args.client_id = self.client_id
            args.seq = self.seq
            self.seq += 1

        # signle or multi shard
        if self.total_shards == 1:
            shard = 0
        else:
            shard = int(key) % self.total_shards

        # retry forever over all known servers, move to next server when timeout
        while True:
            for srv in self.servers_by_shard[shard]:
                try:
                    reply: GetReply = srv.call("KVServer.Get", args)
                    return reply.value
                except TimeoutError:
                    continue

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.
        args = PutAppendArgs(key, value)
        with self.mu:
            args.client_id = self.client_id
            args.seq = self.seq
            self.seq += 1

        # signle or multi shard
        if self.total_shards == 1:
            shard = 0
        else:
            shard = int(key) % self.total_shards

        # similar to get, return whatever server returns
        for srv in self.servers_by_shard[shard]:
            while True:
                try:
                    reply: PutAppendReply = srv.call(f"KVServer.{op}", args)
                    break
                except TimeoutError:
                    continue
        # only return when all replicas are apllied
        return reply.value

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
