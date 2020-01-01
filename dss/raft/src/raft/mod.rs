use std::cmp::{max, min};
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

    // apply_ch is a channel on which the tester or service
    // expects Raft to send ApplyMsg messages.
    apply_ch: UnboundedSender<ApplyMsg>,

    // candidateId that received vote in current
    // term (or null if none)
    voted_for: Option<usize>,
    // log entries; each entry contains command
    // for state machine, and term when entry
    // was received by leader (first index is 1)
    log: Vec<LogEntry>,
    // index of highest log entry known to be
    // committed (initialized to 0, increases
    // monotonically)
    commit_index: usize,
    //index of highest log entry applied to state
    // machine (initialized to 0, increases
    // monotonically)
    last_applied: usize,
    // for each server, index of the next log entry
    // to send to that server (initialized to leader
    // last log index + 1)
    next_index: Vec<u64>,
    // for each server, index of highest log entry
    // known to be replicated on server
    // (initialized to 0, increases monotonically)
    match_index: Vec<u64>,
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
        let n = peers.len();
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            apply_ch,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; n],
            match_index: vec![0; n],
        };

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
        let state = self.encode_state();
        self.persister.save_raft_state(state);
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
        match labcodec::decode(data) {
            Ok(o) => {
                let ps: PersistStorage = o;

                if ps.voted_for != -1 {
                    self.voted_for = Some(ps.voted_for as usize);
                }

                self.state.term = ps.term;
                self.commit_index = ps.commit_index as usize;
                self.last_applied = ps.last_applied as usize;
                self.log = ps.log.clone();
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn encode_state(&self) -> Vec<u8> {
        let mut voted_for = -1_i64;
        if self.voted_for.is_some() {
            voted_for = self.voted_for.clone().unwrap() as i64;
        }

        let mut result = Vec::new();
        let ps = PersistStorage {
            term: self.state.term,
            voted_for,
            commit_index: self.commit_index as u64,
            last_applied: self.last_applied as u64,
            log: self.log.clone(),
        };
        labcodec::encode(&ps, &mut result).unwrap();

        result
    }

    fn save_state_and_snapshot(&self, snapshot: Vec<u8>) {
        let state = self.encode_state();
        self.persister.save_state_and_snapshot(state, snapshot);
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

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.state.is_leader() {
            let index = self.log.len() + 1;
            let term = self.state.term;

            let mut buf = Vec::new();
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).

            self.log.push(LogEntry { term, command: buf });

            info!(
                "[LEADER] peer{} push index={}, term={}",
                self.me, index, term
            );

            Ok((index as u64, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn set_state(&mut self, inner: RaftState) {
        self.state.inner = inner;
    }

    fn get_last_log_index_and_term(&self) -> (u64, u64) {
        if self.log.len() > 0 {
            let index = self.log.len() - 1;
            (index as u64, self.log[index].term)
        } else {
            (0, 0)
        }
    }

    fn get_prev_log_index_and_term(&self, peer_id: usize) -> (u64, u64) {
        let index = (self.next_index[peer_id] - 1) as usize;

        if self.log.len() > index {
            (index as u64, self.log[index].term)
        } else {
            (index as u64, 0)
        }
    }

    fn apply_logs(&mut self) {
        while self.commit_index > self.last_applied {
            let command = self.log[self.last_applied].command.clone();
            self.last_applied += 1;

            info!(
                "[{:?}] peer{} apply entry({})",
                self.state.inner, self.me, self.last_applied
            );

            let _ = self.apply_ch.unbounded_send(ApplyMsg {
                command_valid: true,
                command,
                command_index: self.last_applied as u64,
            });

            self.persist();
        }
    }

    fn find_majority_match_index(&self, peer_id: usize) -> usize {
        let mut res = 0;
        let n = self.peers.len();
        let current_term = self.state.term;

        for index in ((self.commit_index + 1)..=self.match_index[peer_id] as usize).rev() {
            let mut count = 1;
            for m in self.match_index.iter() {
                if *m >= index as u64 {
                    count += 1;
                }
            }

            if count > n / 2 && self.log[index - 1].term == current_term {
                res = index;
                break;
            }
        }

        res
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
        //crate::your_code_here(command)
        let mut rf = self.raft.lock().unwrap();
        rf.start(command)
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
        let mut rf = self.raft.lock().unwrap();
        rf.set_state(RaftState::Shutdown);
        drop(rf);

        self.sc_tx.send(RaftState::Shutdown).unwrap();
        if let Some(handle) = self.join_handle.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }

    pub fn save_state_and_snapshot(&self, snapshot: Vec<u8>) {
        let rf = self.raft.lock().unwrap();
        rf.save_state_and_snapshot(snapshot);
    }

    fn convert_to_follower(&self, term: u64) {
        let mut rf = self.raft.lock().unwrap();
        rf.set_state(RaftState::Follower);
        rf.state.term = term;
        rf.voted_for = None;

        rf.persist();
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
                let state = self.get_state();

                if ns == RaftState::Leader && state.is_candidate() {
                    let mut rf = self.raft.lock().unwrap();
                    let n = rf.peers.len();
                    let (last_log_index, _) = rf.get_last_log_index_and_term();

                    rf.match_index = vec![last_log_index; n];
                    rf.next_index = vec![last_log_index + 1; n];

                    rf.set_state(RaftState::Leader);

                    info!("[LEADER] peer{} term={} become leader.", id, state.term);
                } else {
                    info!(
                        "[CANDIDATE] peer{} term={} change to {:?}.",
                        id, state.term, ns
                    );
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
            rf.persist();

            (rf.state.term, rf.peers.clone())
        };

        let id = self.id;
        let nums = peers.len();
        let passed = Arc::new(Mutex::new(1));
        let args = self.make_vote_args();

        info!("[CANDIDATE] peer{} request vote term {}.", id, term);

        for (i, peer) in peers.iter().enumerate() {
            if i == id {
                continue;
            }

            let passed_clone = passed.clone();
            let node_clone = self.clone();

            self.spawn_future(
                peer.request_vote(&args)
                    .map_err(move |e| debug!("[CANDIDATE] peer{} request_vote fail, {:?}", id, e))
                    .map(move |resp| {
                        // If RPC request or response contains term T > currentTerm:
                        // set currentTerm = T, convert to follower (§5.1)
                        if resp.term > node_clone.term() {
                            node_clone.convert_to_follower(resp.term);
                        }

                        if resp.vote_granted {
                            let mut passed = passed_clone.lock().unwrap();
                            *passed += 1;

                            info!("[CANDIDATE] peer{} => peer{} granted.", i, id);

                            // If votes received from majority of servers: become leader
                            if *passed > nums / 2 && node_clone.get_state().is_candidate() {
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
        let id = self.id;
        let (current_term, peers) = {
            let rf = self.raft.lock().unwrap();
            (rf.state.term, rf.peers.clone())
        };

        for (i, peer) in peers.iter().enumerate() {
            if i == id {
                continue;
            }

            let args = self.make_append_entries_args(i);
            let node_clone = self.clone();

            self.spawn_future(
                peer.append_entries(&args)
                    .map_err(move |e| {
                        debug!(
                            "[LEADER] peer{} term({}) heartbeat to peer{} fail, {:?}",
                            id, current_term, i, e
                        )
                    })
                    .map(move |resp| {
                        // If RPC request or response contains term T > currentTerm:
                        // set currentTerm = T, convert to follower (§5.1)
                        if resp.term > current_term {
                            node_clone.convert_to_follower(resp.term);
                            node_clone.sc_tx.send(RaftState::Follower).unwrap();
                        }

                        let mut rf = node_clone.raft.lock().unwrap();

                        if resp.success {
                            // update nextIndex and matchIndex for follower (§5.3)
                            rf.match_index[i] = args.prev_log_index + args.entries.len() as u64;
                            rf.next_index[i] = rf.match_index[i] + 1;

                            let n = rf.find_majority_match_index(i);

                            // If there exists an N such that N > commitIndex, a majority
                            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                            // set commitIndex = N (§5.3, §5.4).
                            if n > rf.commit_index && rf.log[n - 1].term == current_term {
                                rf.commit_index = n;
                                rf.apply_logs();
                            }
                        } else {
                            // If AppendEntries fails because of log inconsistency:
                            // decrement nextIndex and retry (§5.3)
                            //rf.next_index[i] = max(rf.next_index[i] - 1, 1);
                            let mut tmp = resp.conflict_index;
                            if resp.conflict_term != 0 {
                                for j in (0..rf.log.len()).rev() {
                                    if rf.log[j].term == resp.conflict_term {
                                        tmp = j as u64;
                                        break;
                                    }
                                }
                            }
                            rf.next_index[i] = max(tmp, 1);
                        }
                    }),
            );
        }

        if let Ok(ns) = rx.recv_timeout(Duration::from_millis(HEARTBEAT_INTERVAL)) {
            info!("[LEADER] peer{} discover state change {:?}.", id, ns);
        }
    }

    fn make_vote_args(&self) -> RequestVoteArgs {
        let rf = self.raft.lock().unwrap();
        let term = rf.state.term;
        let candidate_id = rf.me as u64;
        let (last_log_index, last_log_term) = rf.get_last_log_index_and_term();

        RequestVoteArgs {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }

    fn make_append_entries_args(&self, peer_id: usize) -> AppendEntriesArgs {
        let rf = self.raft.lock().unwrap();
        let term = rf.state.term;
        let (prev_log_index, prev_log_term) = rf.get_prev_log_index_and_term(peer_id);

        let mut entries = Vec::new();

        rf.log[prev_log_index as usize..].iter().for_each(|e| {
            entries.push(e.clone());
        });

        AppendEntriesArgs {
            term,
            leader_id: rf.me as u64,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: rf.commit_index as u64,
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

                rf.persist();
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

        let success;
        let current_term = self.term();
        let (voted_for, last_log_index) = {
            let rf = self.raft.lock().unwrap();
            let (last_log_index, _) = rf.get_last_log_index_and_term();
            (rf.voted_for, last_log_index)
        };
        let mut conflict_index = last_log_index;
        let mut conflict_term = 0;

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
            let prev_log_index = args.prev_log_index as usize;
            let mut rf = self.raft.lock().unwrap();

            // Reply false if log doesn’t contain an entry at prevLogIndex
            // whose term matches prevLogTerm (§5.3)
            if prev_log_index > rf.log.len() {
                info!(
                    "[FOLLOWER] peer{} prev_log_index({}) > log.len({})",
                    rf.me,
                    prev_log_index,
                    rf.log.len()
                );

                success = false;
            } else if rf.log.len() > prev_log_index
                && rf.log[prev_log_index].term != args.prev_log_term
            {
                info!(
                    "[FOLLOWER] peer{} log mismatch index={}, {} != {}",
                    rf.me, prev_log_index, rf.log[prev_log_index].term, args.prev_log_term
                );

                conflict_term = rf.log[prev_log_index].term;
                conflict_index = 0;

                // search log for the first index
                for i in (0..prev_log_index).rev() {
                    if rf.log[i].term != conflict_term {
                        conflict_index = (i + 1) as u64;
                        break;
                    }
                }

                success = false;
            } else {
                success = true;

                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (§5.3)
                for i in 0..args.entries.len() {
                    let index = prev_log_index + i;

                    if index >= rf.log.len() {
                        // Append any new entries not already in the log
                        rf.log.push(args.entries[i].clone());
                    } else if rf.log.len() > index && rf.log[index] != args.entries[i] {
                        rf.log.truncate(index);

                        let mut entries = args.entries[i..].to_vec();
                        rf.log.append(&mut entries);
                        break;
                    }
                }

                // If leaderCommit > commitIndex, set commitIndex =
                // min(leaderCommit, index of last new entry)
                rf.commit_index = min(args.leader_commit as usize, rf.log.len());
                rf.apply_logs();
            }
        }

        Box::new(future::ok(AppendEntriesReply {
            term: current_term,
            success,
            conflict_index,
            conflict_term,
        }))
    }
}
