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
#include <random>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

// convert logical index to physical index, use when access log
#define LOGI_TO_PHYSI(logi) (logi - last_include_index)
// always convert physical to logical when compare.
#define PHYSI_TO_LOGI(physi) (physi + last_include_index)
#define LOGI_LOG_SIZE   (logs.size() + last_include_index)

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
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

    // it seems different from the paper, the node will redirect command to leader.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;



    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:

    // for all servers
    //  persistent:
    int vote_for;
    std::vector<log_entry<command>> logs;   // attention: base 1!, physical
    int last_include_index; // logical index
    int last_include_term;
    std::vector<char> snapshot;
    //  volatile:
    int commit_index;   // logical
    int last_applied;   // logical

    // for followers & candidates
    //  volatile:
    std::chrono::milliseconds timeout;
    std::chrono::system_clock::time_point last_recv_rpc;

    // for candidates
    // valatile:
    int recvd_vote;


    // for leaders: should be reinitialized after election
    //  volatile:
    std::vector<int> next_index;
    std::vector<int> match_index;

    static const int F_TIMEOUT_MIN_MS = 300;
    static const int F_TIMEOUT_MAX_MS = 500;
    static const int C_TIMEOUT_MIN_MS = 800;
    static const int C_TIMEOUT_MAX_MX = 1200;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    std::chrono::milliseconds gene_timeout(raft_role role);
    void start_election();
    void synchronize_term(int term);
    void reinit_leader_state();
};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    timeout = gene_timeout(role);
    vote_for = -1;
    recvd_vote = 0;
    commit_index = 0;
    last_applied = 0;
    next_index.resize(num_nodes(), 0);
    match_index.resize(num_nodes(), 0);

    log_entry<command> empty_entry;
    empty_entry.term = 0;
    logs.push_back(empty_entry);

    last_include_index = 0;
    last_include_term = 0;

}

template<typename state_machine, typename command>
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

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    // RAFT_LOG("Restore");
    storage->restore(current_term, vote_for, last_include_index, last_include_term, snapshot, logs);
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
        last_applied = last_include_index;
        logs[0].term = last_include_term;
    }
    
    // RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    mtx.lock();

    if (role != raft_role::leader) {
        mtx.unlock();
        return false;
    }

    term = current_term;
    index = LOGI_LOG_SIZE;

    // RAFT_LOG("new command %d", index);

    log_entry<command> new_entry(term, cmd);
    logs.push_back(new_entry);
    
    storage->persist_logs(logs);

    ++next_index[my_id];
    ++match_index[my_id];

    mtx.unlock();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    // if logs is too long, save snapshot
    // triggered by test program.
    mtx.lock();

    // apply the logs which are just commit
    if (commit_index > last_applied) {
        for (int i = last_applied + 1; i <= commit_index; ++i) {
            state->apply_log(logs[LOGI_TO_PHYSI(i)].cmd);
        }
        last_applied = commit_index;
    }

    // RAFT_LOG("Snapshot last index: %d", last_applied);

    snapshot = state->snapshot();
    storage->persist_snapshot(snapshot);

    last_include_term = logs[LOGI_TO_PHYSI(last_applied)].term;
    // save the last include index in logs[0]
    logs.erase(logs.begin(), logs.begin() + LOGI_TO_PHYSI(last_applied));
    storage->persist_logs(logs);
    last_include_index = last_applied;

    storage->persist_metadata(current_term, vote_for, last_include_index, last_include_term);

    // RAFT_LOG("new log size: %d", logs.size());

    mtx.unlock();
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    mtx.lock();

    // RAFT_LOG("vote for %d, current vote_for : %d", args.candidate_id, vote_for);

    // if old, begin a new term.
    if (current_term < args.term) {
        synchronize_term(args.term);
    }

    reply.term = this->current_term;

    if (args.term < this->current_term) {
        reply.vote_granted = false;
    }
    else if (logs[logs.size() - 1].term < args.last_log_term || 
            (logs[logs.size() - 1].term == args.last_log_term && LOGI_LOG_SIZE <= (args.last_log_index + 1))) {
        if (this->vote_for == -1 || this->vote_for == args.candidate_id) {
            this->vote_for = args.candidate_id;
            reply.vote_granted = true;

            storage->persist_metadata(current_term, vote_for, last_include_index, last_include_term);
        }else {
            reply.vote_granted =false;
        }
    }
    else {
        reply.vote_granted = false;
    }

    mtx.unlock();
    return raft_rpc_status::OK;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    mtx.lock();

    // RAFT_LOG("Recieve vote reply %d : %d", target, reply.vote_granted);

    if (role != raft_role::candidate) {
        mtx.unlock();
        return;
    }

    last_recv_rpc = std::chrono::system_clock::now();

    if (current_term < reply.term) {
        synchronize_term(reply.term);
    }

    if (reply.vote_granted) {
        ++recvd_vote;
        if (recvd_vote > num_nodes() / 2) {
            reinit_leader_state();
        }
    }

    mtx.unlock();
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here: 
    mtx.lock();

    if (arg.term < current_term) {
        reply.term = current_term;
        reply.success = false;

        mtx.unlock();
        return raft_rpc_status::OK;
    } else if (arg.term > current_term) {
        // do the same thing for > & ==
        synchronize_term(arg.term);
        reply.term = current_term;
    } else {
        // do not clear it vote_for!
        // synchronize does NOT mean refresh!
        timeout = gene_timeout(role);
        last_recv_rpc = std::chrono::system_clock::now();
        reply.term = current_term;
    }
    
    // reply for heartbeat.
    if (arg.entries.empty()) {
        reply.success = true;
        if (arg.leader_commit > commit_index) {
            commit_index = (arg.leader_commit < LOGI_LOG_SIZE - 1) ? arg.leader_commit : LOGI_LOG_SIZE - 1;
            // RAFT_LOG("commit index change to %d", commit_index);
        }
    }
    else {
        // RAFT_LOG("append entries from %d to %d", arg.prev_log_index + 1, arg.prev_log_index + arg.entries.size());

        // we can tolerance inconsistency. at least once is for fixing inconsistency rather than insuring it won't happen.
        // to do: I think it has no need to delete logs every time, we do this in "else"
        if (LOGI_LOG_SIZE <= arg.prev_log_index){
            reply.success = false;
        } else {
            if (arg.prev_log_index < last_include_index || logs[LOGI_TO_PHYSI(arg.prev_log_index)].term != arg.prev_log_term) {
                reply.success = false;
            } else {
                // to do: ignore if already exist.
                logs.resize(LOGI_TO_PHYSI(arg.prev_log_index) + 1);
                logs.insert(logs.end(), arg.entries.begin(), arg.entries.end());
                if (arg.leader_commit > commit_index) {
                    commit_index = (arg.leader_commit < LOGI_LOG_SIZE - 1) ? arg.leader_commit : LOGI_LOG_SIZE - 1;
                    // RAFT_LOG("commit index change to %d", commit_index);
                }

                storage->persist_logs(logs);

                reply.success = true;
            }
        }
    }

    mtx.unlock();
    return raft_rpc_status::OK;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    mtx.lock();

    // leader is old.
    if (reply.term > current_term) {
        synchronize_term(reply.term);
    }

    if (role != raft_role::leader) {
        mtx.unlock();
        return;
    }

    // not optimized
    if (!reply.success) {
        // only resend when lateset request failed.
        if (next_index[target] == arg.prev_log_index + 1) {
            next_index[target] = arg.prev_log_index;
        }
    } else {
        if (!arg.entries.empty()) {
            // RAFT_LOG("append success for %d from %d to %d", target, arg.prev_log_index + 1, arg.prev_log_index + arg.entries.size());
            next_index[target] = arg.prev_log_index + arg.entries.size() + 1;
            match_index[target] = next_index[target] - 1;
        }

    }

    mtx.unlock();
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    mtx.lock();

    // RAFT_LOG("install snapshot");

    if (current_term < args.term) {
        synchronize_term(args.term);
    }else if (current_term > args.term) {
        reply.term = current_term;
        mtx.unlock();
        return raft_rpc_status::OK;
    }

    // different from paper, since we don't have partition.
    reply.term = current_term;

    // recieve a repeat snapshot.
    if (args.last_include_index == last_include_index && args.last_include_term == last_include_term) {
        mtx.unlock();
        return raft_rpc_status::OK;
    }

    if (PHYSI_TO_LOGI(logs.size() - 1) > args.last_include_index
        && 
        (LOGI_TO_PHYSI(args.last_include_index) >= 0 && logs[LOGI_TO_PHYSI(args.last_include_index)].term == args.term)) {
        // [begin, last_include_index)
        logs.erase(logs.begin(), logs.begin() + LOGI_TO_PHYSI(args.last_include_index));
    } else {
        logs.clear();

        log_entry<command> last_include_entry;
        last_include_entry.term = args.last_include_term;
        logs.push_back(last_include_entry);
    }
    
    snapshot = args.data;
    last_include_index = args.last_include_index;
    last_include_term = args.last_include_term;
    last_applied = args.last_include_index;

    state->apply_snapshot(snapshot);
    storage->persist_metadata(current_term, vote_for, last_include_index, last_include_term);
    storage->persist_snapshot(snapshot);
    storage->persist_logs(logs);

    mtx.unlock();
    return raft_rpc_status::OK;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    mtx.lock();

    if (reply.term > current_term) {
        synchronize_term(reply.term);
    }else if (role == raft_role::leader) {
        if (next_index[target] < arg.last_include_index + 1) {
            next_index[target] = arg.last_include_index + 1;
            match_index[target] = next_index[target] - 1;
        }
    }

    mtx.unlock();
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
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

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    last_recv_rpc = std::chrono::system_clock::now();
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();

        if (role == raft_role::follower) {
            // if timeout, become a candidate
            if (std::chrono::system_clock::now() - last_recv_rpc > timeout) {
                start_election();
            }
        } else if (role == raft_role::candidate) {
            if (std::chrono::system_clock::now() - last_recv_rpc > timeout) {
                synchronize_term(current_term);
            }
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();

        if (role == raft_role::leader) {
            for (int i = 0; i < next_index.size(); ++i) {
                if (i == my_id) continue;

                // if a follower is leg, let send snapshot to handle it.
                if (next_index[i] <= LOGI_LOG_SIZE - 1 && next_index[i] > last_include_index) {
                    append_entries_args<command> arg;
                    arg.term = current_term;
                    arg.prev_log_index = next_index[i] - 1;
                    arg.prev_log_term = logs[LOGI_TO_PHYSI(next_index[i] - 1)].term;
                    arg.leader_commit = commit_index;

                    auto begin = logs.begin() + LOGI_TO_PHYSI(next_index[i]);
                    arg.entries.assign(begin, logs.end());

                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }else if (next_index[i] <= LOGI_LOG_SIZE - 1 && next_index[i] <= last_include_index && !snapshot.empty()) {
                    install_snapshot_args arg;
                    arg.term = current_term;
                    arg.last_include_index = last_include_index;
                    arg.last_include_term = last_include_term;
                    arg.data = snapshot;

                    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
                }
            }

            // optimization: if i > j && logs[i] is majority, then logs[j] is also majority
            for (int i = LOGI_LOG_SIZE - 1; i > commit_index; --i) {
                int counter = 0;

                // never commit a previous term log by counting replica
                if (logs[LOGI_TO_PHYSI(i)].term < current_term) break;

                for (int j = 0; j < next_index.size(); ++j) {
                    if (next_index[j] > i) ++counter;
                }

                if (counter > num_nodes() / 2) {
                    // RAFT_LOG("leader : commit index change to %d", i);
                    commit_index = i;
                    break;
                }
            }
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();

        // there must have some bug.
        if (commit_index > last_applied) {
            for (int i = last_applied + 1; i <= commit_index; ++i) {
                state->apply_log(logs[LOGI_TO_PHYSI(i)].cmd);
            }
            last_applied = commit_index;
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        mtx.lock();

        if (role == raft_role::leader) {
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id) continue;
                append_entries_args<command> arg;
                arg.term = current_term;
                arg.leader_commit = commit_index;

                thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
            }
        }

        mtx.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
std::chrono::milliseconds raft<state_machine, command>::gene_timeout(raft_role role) {

    static std::mt19937 generator;
    static std::uniform_int_distribution<int> f_timeout(F_TIMEOUT_MIN_MS, F_TIMEOUT_MAX_MS);
    static std::uniform_int_distribution<int> c_timeout(C_TIMEOUT_MIN_MS, C_TIMEOUT_MAX_MX);

    if (role == raft_role::follower) {
        return std::chrono::milliseconds(f_timeout(generator));
    } else if (role == raft_role::candidate)
    {
        return std::chrono::milliseconds(c_timeout(generator));
    } else {
        RAFT_LOG("[Error] a leader generate timeout\n");
        assert(0);
    }
    
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start_election() {
    // RAFT_LOG("start election");
    role = raft_role::candidate;

    timeout = gene_timeout(role);
    vote_for = my_id;
    recvd_vote = 1;
    current_term++;

    storage->persist_metadata(current_term, vote_for, last_include_index, last_include_term);

    request_vote_args args(current_term, my_id, LOGI_LOG_SIZE - 1, logs[logs.size() - 1].term); 

    for (int i = 0; i < num_nodes(); ++i) {
        if (i == my_id) continue;

        int ret = thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
    }

    last_recv_rpc = std::chrono::system_clock::now();
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::synchronize_term(int term) {
    // RAFT_LOG("synchronize term to %d", term);

    // reset base state
    role = raft_role::follower;
    vote_for = -1;
    recvd_vote = 0;
    current_term = term;

    // persist metadata
    storage->persist_metadata(current_term, vote_for, last_include_index, last_include_term);

    // refresh timeout
    timeout = gene_timeout(role);
    last_recv_rpc = std::chrono::system_clock::now();

    // reset leader state:
    // unnecessary to do

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::reinit_leader_state() {
    // RAFT_LOG("become a leader");
    role = raft_role::leader;
    
    for (int i = 0; i < next_index.size(); ++i) {
        next_index[i] = LOGI_LOG_SIZE;

        if (i == my_id) {
            match_index[i] = next_index[i] - 1;
        }else {
            match_index[i] = 0;
        }
    }

    return;
}


#endif // raft_h