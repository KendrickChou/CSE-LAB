#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;

    request_vote_args() {};
    request_vote_args(int term_, int candidate_id_, int last_log_index_, int last_log_term_)
        : term(term_), candidate_id(candidate_id_),
         last_log_index(last_log_index_), last_log_term(last_log_term_) {}
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int term;
    bool vote_granted;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int term;
    command cmd;

    log_entry() {};
    log_entry(int term_, command cmd_) : term(term_), cmd(cmd_) {}
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.term << entry.cmd;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.term;
    u >> entry.cmd;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int prev_log_index;
    int prev_log_term;
    std::vector<log_entry<command>> entries;
    int leader_commit;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term;
    m << args.prev_log_index;
    m << args.prev_log_term;
    m << args.entries;
    m << args.leader_commit;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.term;
    u >> args.prev_log_index;
    u >> args.prev_log_term;
    u >> args.entries;
    u >> args.leader_commit;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
    int term;
    int last_include_index;
    int last_include_term;
    std::vector<char> data;
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
    int term;
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h