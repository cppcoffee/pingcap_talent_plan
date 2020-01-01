use crate::proto::kvraftpb::*;
use crate::raft;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::prelude::*;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use labrpc::RpcFuture;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    apply_ch: Option<UnboundedReceiver<raft::ApplyMsg>>,
    db: HashMap<String, String>,
    ready_reply: HashMap<String, ReadyReply>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.
        let snapshot = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        //crate::your_code_here((rf, maxraftstate, apply_ch))

        let mut server = KvServer {
            rf: raft::Node::new(rf),
            me,
            maxraftstate,
            apply_ch: Some(apply_ch),
            db: HashMap::new(),
            ready_reply: HashMap::new(),
        };

        server.restore_snapshot(&snapshot);

        server
    }

    fn persist_snapshot(&self) {
        let snapshot = Snapshot {
            db: self.db.clone(),
            ready_reply: self.ready_reply.clone(),
        };

        let mut buf = vec![];
        labcodec::encode(&snapshot, &mut buf).unwrap();
        self.rf.save_state_and_snapshot(buf);
    }

    fn restore_snapshot(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        let snapshot: Snapshot = labcodec::decode(data).unwrap();
        self.db = snapshot.db.clone();
        self.ready_reply = snapshot.ready_reply.clone();
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    id: usize,
    server: Arc<Mutex<KvServer>>,
    join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        //crate::your_code_here(kv);
        let node = Node {
            id: kv.me,
            server: Arc::new(Mutex::new(kv)),
            join_handle: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false)),
        };

        // take apply_ch.
        let mut server = node.server.lock().unwrap();
        let apply_ch = server.apply_ch.take().unwrap();
        drop(server);

        // run apply_ch task.
        let node_clone = node.clone();
        *node.join_handle.lock().unwrap() = Some(thread::spawn(move || {
            node_clone.apply_notify_task(apply_ch)
        }));

        node
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        let server = self.server.lock().unwrap();
        server.rf.kill();
        drop(server);

        // shutdown apply_notify_task.
        *self.shutdown.lock().unwrap() = true;
        if let Some(handle) = self.join_handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.lock().unwrap().rf.get_state()
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().unwrap()
    }

    /// listen apply_ch notify.
    fn apply_notify_task(&self, apply_ch: UnboundedReceiver<raft::ApplyMsg>) {
        let mut apply_ch = apply_ch;

        loop {
            if self.is_shutdown() {
                info!("server{} task shutdown.", self.id);
                return;
            }

            if let Ok(Async::Ready(Some(cmd))) =
                futures::executor::spawn(futures::lazy(|| apply_ch.poll())).wait_future()
            {
                if !cmd.command_valid {
                    continue;
                }

                let mut server = self.server.lock().unwrap();
                let entry: KvEntry = labcodec::decode(&cmd.command).unwrap();

                if let Some(reply) = server.ready_reply.get(&entry.name) {
                    if reply.seq >= entry.seq {
                        continue;
                    }
                }

                let mut reply = ReadyReply {
                    seq: entry.seq,
                    value: String::new(),
                };

                match entry.op {
                    1 => {
                        // get
                        if let Some(value) = server.db.get(&entry.key) {
                            reply.value = value.clone();
                        }

                        info!(
                            "server{} ready {}-{} get({})={}",
                            self.id, entry.name, entry.seq, entry.key, reply.value
                        );

                        server.ready_reply.insert(entry.name, reply);
                    }
                    2 => {
                        // put
                        info!(
                            "server{} ready {}-{} put({},{})",
                            self.id, entry.name, entry.seq, entry.key, entry.value
                        );

                        server.db.insert(entry.key, entry.value);
                        server.ready_reply.insert(entry.name, reply);
                        server.persist_snapshot();
                    }
                    3 => {
                        // append
                        info!(
                            "server{} ready {}-{} append({},{})",
                            self.id, entry.name, entry.seq, entry.key, entry.value
                        );

                        if let Some(mut_ref) = server.db.get_mut(&entry.key) {
                            mut_ref.push_str(&entry.value);
                        } else {
                            // perform as put
                            server.db.insert(entry.key, entry.value);
                        }
                        server.ready_reply.insert(entry.name, reply);
                        server.persist_snapshot();
                    }
                    _ => {
                        panic!("unknown op type: {}", entry.op);
                    }
                }
            }

            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        //crate::your_code_here(arg)
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
            ready: false,
            seq: 0,
        };

        if !self.is_leader() {
            reply.err = String::from("wrong leader");
            return Box::new(futures::future::ok(reply));
        }

        let server = self.server.lock().unwrap();

        if let Some(rep) = server.ready_reply.get(&arg.name) {
            reply.seq = rep.seq;
            if arg.seq == rep.seq {
                // ready
                reply.wrong_leader = false;
                reply.ready = true;
                reply.value = rep.value.clone();
                return Box::new(futures::future::ok(reply));
            } else if arg.seq < rep.seq {
                // not ready (expired request)
                reply.wrong_leader = false;
                reply.err = String::from("expired request");
                return Box::new(futures::future::ok(reply));
            }
        }

        info!(
            "server{} {}-{} get({})",
            self.id, arg.name, arg.seq, arg.key
        );

        let cmd = KvEntry {
            name: arg.name,
            key: arg.key,
            seq: arg.seq,
            op: 1, // get
            value: String::new(),
        };

        match server.rf.start(&cmd) {
            Ok((index, term)) => {
                reply.wrong_leader = false;
                reply.err = format!("not redy, index={}, term={}", index, term);
            }
            Err(_) => {
                reply.err = String::from("start cmd failed");
            }
        }

        Box::new(futures::future::ok(reply))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        //crate::your_code_here(arg)
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
            ready: false,
            seq: 0,
        };

        if !self.is_leader() {
            reply.err = String::from("wrong leader");
            return Box::new(futures::future::ok(reply));
        }

        let server = self.server.lock().unwrap();

        if let Some(rep) = server.ready_reply.get(&arg.name) {
            reply.seq = rep.seq;
            if arg.seq == rep.seq {
                // ready
                reply.wrong_leader = false;
                reply.ready = true;
                return Box::new(futures::future::result(Ok(reply)));
            } else if arg.seq < rep.seq {
                // not ready (expired request)
                reply.wrong_leader = false;
                reply.err = String::from("expired request");
                return Box::new(futures::future::ok(reply));
            }
        }

        info!(
            "server{} {}-{} put_append({}, {})",
            self.id, arg.name, arg.seq, arg.key, arg.value
        );

        let cmd = KvEntry {
            name: arg.name,
            key: arg.key,
            seq: arg.seq,
            op: arg.op,
            value: arg.value,
        };

        match server.rf.start(&cmd) {
            Ok((index, term)) => {
                reply.wrong_leader = false;
                reply.err = format!("not ready, index={}, term={}", index, term);
            }
            Err(_) => {
                reply.err = String::from("start cmd failed");
            }
        }

        Box::new(futures::future::ok(reply))
    }
}
