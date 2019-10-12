use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::UnboundedSender;
use labrpc::RpcFuture;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const ELECTION_TIMEOUT_MIN: u64 = 500;
const ELECTION_TIMEOUT_MAX: u64 = 600;
const HEARTBEAT_INTERVAL: u64 = 200;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[derive(Clone, PartialEq, Debug)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

impl Default for RaftState {
    fn default() -> Self {
        RaftState::Follower
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub inner: RaftState,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.inner == RaftState::Leader
    }
    // Whether this peer believes it is the candidate.
    fn is_candidate(&self) -> bool {
        self.inner == RaftState::Candidate
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bytes, tag = "2")]
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new(command: Vec<u8>, term: u64) -> LogEntry {
        LogEntry { command, term }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    /*
    // apply_ch is a channel on which the tester or service
    // expects Raft to send ApplyMsg messages.
    apply_ch: UnboundedSender<ApplyMsg>,
    */
    // candidateId that received vote in current
    // term (or null if none)
    voted_for: Option<usize>,
    // log entries; each entry contains command
    // for state machine, and term when entry
    // was received by leader (first index is 1)
    //log: Vec<LogEntry>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            //apply_ch,
            voted_for: None,
            //log: Vec::new(),
        };

        let _ = apply_ch;

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        //crate::your_code_here((rf, apply_ch))
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn set_state(&mut self, inner: RaftState) {
        self.state.inner = inner;
    }

    // FIXME
    fn get_last_log_index_and_term(&self) -> (u64, u64) {
        (0, 0)
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    peer: RaftClient,
    id: usize,
    heartbeat_instant: Arc<Mutex<Instant>>,
    // state change channel.
    sc_tx: SyncSender<RaftState>,
    join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        //crate::your_code_here(raft)
        let id = raft.me;
        let peer = raft.peers[raft.me].clone();
        let (tx, rx) = sync_channel(0);

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            peer,
            id,
            heartbeat_instant: Arc::new(Mutex::new(Instant::now())),
            sc_tx: tx,
            join_handle: Arc::new(Mutex::new(None)),
        };

        let node_clone = node.clone();
        let handle = thread::spawn(move || node_clone.state_machine(rx));
        *node.join_handle.lock().unwrap() = Some(handle);

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        //crate::your_code_here(())
        let rf = self.raft.lock().unwrap();
        rf.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        //crate::your_code_here(())
        let rf = self.raft.lock().unwrap();
        rf.state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let rf = self.raft.lock().unwrap();
        rf.state.clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        {
            self.raft.lock().unwrap().set_state(RaftState::Shutdown);
            self.sc_tx.send(RaftState::Shutdown).unwrap();
        }

        if let Some(handle) = self.join_handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }

    fn convert_to_follower(&self, term: u64) {
        let mut rf = self.raft.lock().unwrap();
        rf.set_state(RaftState::Follower);
        rf.state.term = term;
        rf.voted_for = None;
    }

    fn spawn_future<F>(&self, f: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.peer.spawn(f);
    }

    fn state_machine(&self, rx: Receiver<RaftState>) {
        loop {
            match self.get_state().inner {
                RaftState::Follower => {
                    self.start_follow(&rx);
                }
                RaftState::Candidate => {
                    self.election(&rx);
                }
                RaftState::Leader => {
                    self.leader_heartbeat(&rx);
                }
                RaftState::Shutdown => {
                    return;
                }
            }
        }
    }

    fn gen_election_timeout(&self) -> Duration {
        let timeout = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
        Duration::from_millis(timeout)
    }

    // If election timeout elapses without receiving AppendEntries
    // RPC from current leader or granting vote to candidate:
    // convert to candidate
    fn start_follow(&self, rx: &Receiver<RaftState>) {
        let id = self.id;
        let dur = self.gen_election_timeout();
        let dur_copy = dur;

        match rx.recv_timeout(dur_copy) {
            Ok(ns) => {
                info!("[FOLLOWER] peer{} discover change to {:?}.", id, ns);
            }
            Err(_) => {
                let heartbeat = self.heartbeat_instant.lock().unwrap();
                if heartbeat.elapsed() < dur {
                    return;
                }

                info!("[FOLLOWER] peer{} timeout({:?}) start election.", id, dur);
                self.raft.lock().unwrap().set_state(RaftState::Candidate);
            }
        }
    }

    fn election(&self, rx: &Receiver<RaftState>) {
        self.do_vote();

        let id = self.id;
        let dur = self.gen_election_timeout();

        match rx.recv_timeout(dur) {
            Ok(ns) => {
                if ns == RaftState::Leader {
                    info!("[LEADER] peer{} become leader.", id);
                } else {
                    info!("[CANDIDATE] peer{} discover change to {:?}.", id, ns);
                }
            }
            Err(_) => {
                // If election timeout elapses: start new election
                info!("[CANDIDATE] peer{} timeout, new election.", id);
            }
        }
    }

    fn do_vote(&self) {
        let (term, peers) = {
            let mut rf = self.raft.lock().unwrap();
            rf.voted_for = Some(rf.me);
            rf.state.term += 1;

            (rf.state.term, rf.peers.clone())
        };

        let id = self.id;
        let half_votes = peers.len() / 2;
        let votes = Arc::new(AtomicUsize::new(1));
        let (last_log_index, last_log_term) =
            { self.raft.lock().unwrap().get_last_log_index_and_term() };
        let args = RequestVoteArgs {
            term,
            candidate_id: id as u64,
            last_log_index,
            last_log_term,
        };

        info!("[CANDIDATE] peer{} request vote term {}.", id, term);

        for (i, peer) in peers.iter().enumerate() {
            if i == id {
                continue;
            }

            let votes_clone = votes.clone();
            let node_clone = self.clone();

            self.spawn_future(
                peer.request_vote(&args)
                    .map_err(move |e| warn!("[CANDIDATE] peer{} request_vote fail, {:?}", id, e))
                    .map(move |resp| {
                        // If RPC request or response contains term T > currentTerm:
                        // set currentTerm = T, convert to follower (§5.1)
                        if resp.term > node_clone.term() {
                            node_clone.convert_to_follower(resp.term);
                        }

                        if resp.vote_granted {
                            let mut rf = node_clone.raft.lock().unwrap();
                            let count = votes_clone.fetch_add(1, Ordering::SeqCst);

                            info!("[CANDIDATE] peer{} => peer{} granted.", i, id);

                            // If votes received from majority of servers: become leader
                            if count + 1 > half_votes && !rf.state.is_leader() {
                                rf.set_state(RaftState::Leader);
                                node_clone.sc_tx.send(RaftState::Leader).unwrap();
                            }
                        } else {
                            info!("[CANDIDATE] peer{} deny peer{} vote request.", i, id);
                        }
                    }),
            );
        }
    }

    // Upon election: send initial empty AppendEntries RPCs
    // (heartbeat) to each server; repeat during idle periods to
    // prevent election timeouts (§5.2)
    fn leader_heartbeat(&self, rx: &Receiver<RaftState>) {
        let (term, peers) = {
            let rf = self.raft.lock().unwrap();
            (rf.state.term, rf.peers.clone())
        };

        let id = self.id;
        let args = AppendEntriesArgs {
            term,
            leader_id: id as u64,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        for (i, peer) in peers.iter().enumerate() {
            if i == id {
                continue;
            }

            let node_clone = self.clone();

            self.spawn_future(
                peer.append_entries(&args)
                    .map_err(move |e| {
                        warn!(
                            "[LEADER] peer{} term({}) heartbeat to peer{} fail, {:?}",
                            id, term, i, e
                        )
                    })
                    .map(move |resp| {
                        // If RPC request or response contains term T > currentTerm:
                        // set currentTerm = T, convert to follower (§5.1)
                        if resp.term > node_clone.term() {
                            node_clone.convert_to_follower(resp.term);
                            node_clone.sc_tx.send(RaftState::Follower).unwrap();
                        }
                    }),
            );
        }

        if let Ok(ns) = rx.recv_timeout(Duration::from_millis(HEARTBEAT_INTERVAL)) {
            info!("[LEADER] peer{} discover state change {:?}.", id, ns);
        }
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        //crate::your_code_here(args)
        let current_term = self.term();
        let mut vote_granted = false;

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if args.term > current_term {
            self.convert_to_follower(args.term);
            self.sc_tx.send(RaftState::Follower).unwrap();
        }

        // Reply false if term < currentTerm (§5.1)
        if args.term < current_term {
            vote_granted = false;
        } else {
            // If votedFor is null or candidateId, and candidate’s log is at
            // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
            let mut rf = self.raft.lock().unwrap();
            let (last_log_index, last_log_term) = rf.get_last_log_index_and_term();

            if (rf.voted_for.is_none() || rf.voted_for == Some(args.candidate_id as usize))
                && (args.last_log_term > last_log_term
                    || (args.last_log_term == last_log_term
                        && args.last_log_index >= last_log_index))
            {
                vote_granted = true;
                rf.voted_for = Some(args.candidate_id as usize);
                rf.set_state(RaftState::Follower);
            }
        }

        Box::new(future::ok(RequestVoteReply {
            term: current_term,
            vote_granted,
        }))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        //crate::your_code_here(args)
        *self.heartbeat_instant.lock().unwrap() = Instant::now();

        let current_term = self.term();
        let mut success = true;
        let voted_for = { self.raft.lock().unwrap().voted_for };

        // If AppendEntries RPC received from new leader: convert to
        // follower
        if self.get_state().is_candidate() && voted_for != Some(args.leader_id as usize) {
            self.convert_to_follower(current_term);
            self.sc_tx.send(RaftState::Follower).unwrap();
        }

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if args.term > current_term {
            self.convert_to_follower(args.term);
            self.sc_tx.send(RaftState::Follower).unwrap();
        }

        // Reply false if term < currentTerm (§5.1)
        if args.term < current_term {
            success = false;
        } else {
            // FIXME
            // Reply false if log doesn’t contain an entry at prevLogIndex
            // whose term matches prevLogTerm (§5.3)

            // FIXME
            // If an existing entry conflicts with a new one (same index
            // but different terms), delete the existing entry and all that
            // follow it (§5.3)

            // FIXME
            // Append any new entries not already in the log

            // FIXME
            // If leaderCommit > commitIndex, set commitIndex =
            // min(leaderCommit, index of last new entry)
        }

        Box::new(future::ok(AppendEntriesReply {
            term: current_term,
            success,
        }))
    }
}
