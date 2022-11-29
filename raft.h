#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <numeric>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

// #define RAFT_LOG(fmt, args...) \
//     do {                       \
//     } while (0);

    #define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    //今年lab没有给
    /* some static value */
    const int timeout_heartbeat = 120;
    const int timeout_follower_election_lower = 300;
    const int timeout_follower_election_upper = 500;
    const int timeout_candidate_election_timeout = 1000;
    const int timeout_commit = 150;

     /* index correponse to term number, value corresponse to candidateId, -1 for null */
    int votedFor;
    std::vector< log_entry<command> > log; /* log entries; each entry contains command for state machine, and term when entry was received by leader */
    int commitIndex; /* index of highest log entry known to be committed, init to 0, start from 1 */
    int lastApplied; /* index of highest log entry applied to state machine, init to 0, start from 1 */


    long long election_timer; /* update it to current time whenever get a rpc request or response, init to current time */
    std::vector< bool > calculateVote; /* 0 for not response or not support, 1 for support, restore whenever server become candidate, not need to init */
    std::vector< bool > calculateAppend; /* 0 for not response or fail, 1 for success, restore whenver leader send new Append to all servers, not need to init */

    std::vector< int > nextIndex; /* for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) */
    std::vector< int> matchIndex; /* for each server, index of highest log entry known to be replicated on server */


    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    long long get_current_time ();
    int get_random (int lower, int upper);
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    votedFor = -1;

    log = std::vector<log_entry<command>>();

    // append a null log as the first log
    log_entry<command> first_null_log = log_entry<command>();
    first_null_log.term = 0;
    first_null_log.index = 0;
    log.push_back(first_null_log);

    // init status
    commitIndex = 0;
    lastApplied = 0;
    election_timer = get_current_time();

    // TODO:restore
    /* restore metadata and logdata */
    storage->restore_metadata(current_term, votedFor);
    storage->restore_logdata(log);
    
     
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    mtx.lock();

    if(is_leader(current_term))
    {
        log_entry<command> current_log = log_entry<command>(cmd,current_term,(int)log.size());
        /* persist log state */
        storage->persist_logdata(current_log);

        log.push_back(current_log);

        term = current_term;
        index = log.size() - 1;

        RAFT_LOG("new_command, term: %d, index: %d", term, index);
    }
    // term = current_term;
    mtx.unlock();
    // return true;
    return is_leader(current_term);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    /* Reply false if term < currentTerm */
    if (args.term < current_term) {
        reply.term = current_term;
        reply.voteGranted = false;
    }

    /* if votedFor for args.term is -1 or candidateId,
     * and candidate's log is at least as up-to-date as receiver's log,
     * grant vote
     */
    else if ( ( votedFor == -1 || votedFor == args.candidateId)
    // else if  ( votedFor == -1 || votedFor == args.candidateId)
        // {
        && (log[commitIndex].term < args.lastLogTerm || (log[commitIndex].term == args.lastLogTerm && commitIndex <= args.lastLogIndex))) {
            reply.term = current_term;
            reply.voteGranted = true;

            votedFor = args.candidateId;

            // /* persist metadata */
            storage->persist_metadata(current_term, votedFor);
        }
    
    else {
        reply.term = current_term;
        reply.voteGranted = false;
    }

    mtx.unlock();

    return raft_rpc_status::OK;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    /* update timestamp first */
    election_timer = get_current_time();

    if (reply.term > current_term) {
        /* update current_term and become follower */
        current_term = reply.term;
        
        role = follower;
        votedFor = -1;

        /* persist metadata state: term changes */
        storage->persist_metadata(current_term, votedFor);
    }

    else {
        assert(target < (int) calculateVote.size());

        calculateVote[target] = reply.voteGranted;
    }

    mtx.unlock();

    RAFT_LOG("RPC request_vote : get from %d, result: %d", arg.candidateId, reply.voteGranted);
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    /* update timestamp first */
    election_timer = get_current_time();

    /* for heartbeat only */
    if (arg.heartbeat) {
        RAFT_LOG("heartbeat");
        if (arg.term >= current_term) {

            role = follower;
            current_term = arg.term;
            reply.term = current_term;
            reply.success = true;

            /* persist metadata state: term changes */
            storage->persist_metadata(current_term, votedFor);
        } else {
            reply.term = current_term;
            reply.success = false;
        }
    }

    /* Reply false if term < currentTerm */
    else if (arg.term < current_term) {
        reply.term = current_term;
        reply.success = false;
    }

    /* Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm */
    else if ((int) log.size() < arg.prevLogIndex || log[arg.prevLogIndex].term != arg.prevLogTerm) {
        reply.term = current_term;
        reply.success = false;
    }

    else {
        /* If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it */
        int i = arg.prevLogIndex + 1;
        while (i < (int) arg.entries.size() && i < (int) log.size()) {
            if (arg.entries[i].term != log[i].term) break;
            else i++;
        };
        while ((int) log.size() > i) {
            log.pop_back();
        };
        /* Append any new entries not already in the log */
        while (i < (int) arg.entries.size()) {
            /* persist log data */
            storage->persist_logdata(arg.entries[i]);

            log.push_back(arg.entries[i]);
            i++;
        };
        /* If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
        if (arg.leaderCommit > commitIndex) {
            commitIndex = std::min(arg.leaderCommit, (int) log.size() - 1);
        };

        reply.term = current_term;
        reply.success = true;
    }

    mtx.unlock();

    RAFT_LOG("RPC append_entries, commitIndex: %d, success? : %d", commitIndex, reply.success);
    return raft_rpc_status::OK;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    /* update timestamp first */
    election_timer = get_current_time();

    if (reply.term > current_term) {
        /* update current_term and become follower */
        current_term = reply.term;
        
        role = follower;
        votedFor = -1;

        /* persist metadata state: term changes */
        storage->persist_metadata(current_term, votedFor);
    } else if (arg.heartbeat) {}
    else if (reply.success) {
        matchIndex[node] = std::max(matchIndex[node], (int) arg.entries.size() - 1);

        std::vector<int> v = matchIndex;
        std::sort(v.begin(), v.end(), std::less<int>());
        commitIndex = std::max(commitIndex, v[(v.size() + 1) / 2 - 1]);

        RAFT_LOG("~~ id: %d, commitIndex: %d", node, commitIndex);
    } else {
        RAFT_LOG("decrement: target: %d", node);
        nextIndex[node] = 1;
    }

    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    srand(time(NULL));

    
    while (true) {

        RAFT_LOG("id: %d, term: %d, role: %d", my_id, current_term, role);

        if (is_stopped()) {
            RAFT_LOG("return");
            return;}
        // Lab3: Your code here

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    long long current_time = get_current_time();
    // Work for followers and candidates.

    switch (role)
        {
        case follower:
        {
            long long timeout_follower_election = get_random(timeout_follower_election_lower, timeout_follower_election_upper);
            if (current_time - election_timer > timeout_follower_election) {
                role = candidate;

                current_term++;

                votedFor = my_id;

                /* persist metadata */
                storage->persist_metadata(current_term, votedFor);

                calculateVote.assign(rpc_clients.size(), false);
                calculateVote[my_id] = true;

                request_vote_args args(current_term, my_id, log.back().term, log.size() - 1);
                
                /* update timestamp first */
                election_timer = get_current_time();
                for (int i = 0; i < (int) rpc_clients.size(); i++)
                    if (i != my_id)
                        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                
            };
            break;
        }

        case candidate:
        {

            if (std::accumulate(calculateVote.begin(), calculateVote.end(), 0) > (int) rpc_clients.size() / 2) {
                /* the leader initializes all nextIndex values to the index just after the last one in its log */
                nextIndex.assign(rpc_clients.size(), log.size());
                matchIndex.assign(rpc_clients.size(), 0);

                role = leader;
            }
            else if (current_time - election_timer > timeout_candidate_election_timeout) {
                role = follower;

                /* update timestamp first */
                election_timer = get_current_time();

                votedFor = -1;
                current_term--;

                /* persist metadata */
                storage->persist_metadata(current_term, votedFor);
            }

            break;
        }

        case leader:
        {            
            break;
        }
        
        default:
            break;
        }
    }
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    
        while (true) {
            if (is_stopped()) return;
            // Lab3: Your code here

            //只有leader执行到这里
            if(is_leader(current_term)){
                matchIndex[my_id] = log.size() - 1;
                for(int i = 0; i < (int) rpc_clients.size(); i ++)
                {
                    if(i != my_id && (int) log.size() > nextIndex[i])
                    {
                        append_entries_args<command> arg(false,current_term,my_id,nextIndex[i] -1,log[nextIndex[i] - 1].term,log,commitIndex);
                        thread_pool->addObjJob(this,&raft::send_append_entries,i,arg);
                    }
                }
            }

            RAFT_LOG("role: %d, CommitIndex: %d", role, commitIndex);
        
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_commit));
        }

        

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:

        if (commitIndex > lastApplied)
        {
            RAFT_LOG("role: %d, commitIndex: %d, lastApplied: %d", my_id, commitIndex, lastApplied);
            for (int i = lastApplied + 1; i <= commitIndex; i++) 
            {
                state->apply_log(log[i].cmd);
            }
            lastApplied = commitIndex;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if (is_leader(current_term)) {
            for (int i = 0; i < (int) rpc_clients.size(); i++)
                if (i != my_id) {
                    append_entries_args<command> args(true, current_term, my_id, 0, 0, std::vector< log_entry<command> >(), matchIndex[i]); /* null entry for heartbeat */
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                } 
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_heartbeat)); // Change the timeout here!
   
    }    
    
    

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template<typename state_machine, typename command>
long long raft<state_machine, command>::get_current_time () {
    using namespace std;
    using namespace std::chrono;

    /* 获取当前时间 */
    system_clock::time_point now = system_clock::now();

    /* 距离 1970-01-01 00:00:00 的纳秒数 */
    chrono::nanoseconds d = now.time_since_epoch();

    /* 转换为毫秒数，会有精度损失 */
    chrono::milliseconds millsec = chrono::duration_cast<chrono::milliseconds>(d);

    return millsec.count();
}

template<typename state_machine, typename command>
int raft<state_machine, command>::get_random(int lower, int upper)
{
    assert(upper >= lower);
    // [a,b)
    int ans = rand() % (upper - lower) + lower;
    return ans;
}

#endif // raft_h