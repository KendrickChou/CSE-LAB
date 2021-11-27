#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.term;
    m << args.candidate_id;
    m << args.last_log_index;
    m << args.last_log_term;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.term;
    u >> args.candidate_id;
    u >> args.last_log_index;
    u >> args.last_log_term;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.vote_granted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.vote_granted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term << reply.success;
    return m;
}
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply) {
    // Your code here
    m >> reply.term;
    m >> reply.success;
    return m;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here
    m << args.term << args.last_include_index
        << args.last_include_term << args.data;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here
    u >> args.term >> args.last_include_index
        >> args.last_include_term >> args.data;
    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m << reply.term;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u >> reply.term;
    return u;
}