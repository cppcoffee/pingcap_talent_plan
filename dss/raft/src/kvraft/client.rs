use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crate::proto::kvraftpb::*;

use futures::prelude::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    seq_num: AtomicU64,
    last_leader: AtomicUsize,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        //crate::your_code_here((name, servers))
        Clerk {
            name,
            servers,
            seq_num: AtomicU64::new(1),
            last_leader: AtomicUsize::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        //crate::your_code_here(key)
        let args = GetRequest {
            key,
            seq: self.get_next_seq_num(),
            name: self.name.clone(),
        };

        let len = self.servers.len();
        let mut last_leader = self.last_leader.load(Ordering::Relaxed);

        loop {
            match self.servers[last_leader].get(&args).wait() {
                Ok(reply) => {
                    if !reply.wrong_leader {
                        if reply.ready {
                            self.last_leader.store(last_leader, Ordering::Relaxed);
                            return reply.value;
                        } else {
                            // wait for ready.
                            thread::sleep(Duration::from_millis(200));
                        }
                    } else {
                        last_leader = (last_leader + 1) % len;
                    }
                }
                Err(_) => {
                    last_leader = (last_leader + 1) % len;
                }
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        //crate::your_code_here(op)
        let len = self.servers.len();
        let args = self.make_put_append_request(op);
        let mut last_leader = self.last_leader.load(Ordering::Relaxed);

        loop {
            match self.servers[last_leader].put_append(&args).wait() {
                Ok(reply) => {
                    if !reply.wrong_leader {
                        if reply.ready {
                            self.last_leader.store(last_leader, Ordering::Relaxed);
                            return;
                        } else {
                            // wait for ready.
                            thread::sleep(Duration::from_millis(200));
                        }
                    } else {
                        last_leader = (last_leader + 1) % len;
                    }
                }
                Err(_) => {
                    last_leader = (last_leader + 1) % len;
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }

    fn get_next_seq_num(&self) -> u64 {
        self.seq_num.fetch_add(1, Ordering::Relaxed)
    }

    fn make_put_append_request(&self, op: Op) -> PutAppendRequest {
        let mut args = PutAppendRequest {
            key: String::new(),
            value: String::new(),
            op: 0,
            seq: self.get_next_seq_num(),
            name: self.name.clone(),
        };

        match op {
            Op::Put(k, v) => {
                args.key = k;
                args.value = v;
                args.op = 2; // put
            }
            Op::Append(k, v) => {
                args.key = k;
                args.value = v;
                args.op = 3; // append
            }
        }

        args
    }
}
