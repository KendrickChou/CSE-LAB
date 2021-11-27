#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <iomanip>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here

    void persist_metadata(int cur_term, int vote_for, int last_include_index, int last_include_term);
    void persist_logs(const std::vector<log_entry<command>> &append_logs);
    void persist_snapshot(const std::vector<char> &snapshot);

    void restore(int &cur_term, int &vote_for, int &last_include_index, 
                    int &last_include_term, std::vector<char> &snapshot, std::vector<log_entry<command>> &logs);

private:
    std::mutex mtx;

    // meta file: current_term 
    std::string meta_name;
    std::string logs_name;
    std::string snapshot_name;

};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    meta_name = dir + "/meta";
    logs_name = dir + "/logs";
    snapshot_name = dir + "/snapshot";
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}

template<typename command>
void raft_storage<command>::persist_metadata(int cur_term, int vote_for, int last_include_index, int last_include_term) {
    mtx.lock();
    std::ofstream f(meta_name, std::ios::out | std::ios::trunc);

    f << cur_term << " " << vote_for << " " << last_include_index << " " << last_include_term;

    f.close();
    mtx.unlock();
    return;
}

template<typename command>
void raft_storage<command>::persist_logs(const std::vector<log_entry<command>> &append_logs) {
    mtx.lock();

    std::fstream f;
    f.open(logs_name, std::ios::out | std::ios::trunc);

    // do not persist 0
    for (int i = 1; i < append_logs.size(); ++i) {
        
        char serialized_cmd[append_logs[i].cmd.size() + 1] = {0};
        append_logs[i].cmd.serialize(serialized_cmd, append_logs[i].cmd.size());

        //  serialized may reduce " "
        std::string s(serialized_cmd, append_logs[i].cmd.size());
        f << append_logs[i].term << " " << append_logs[i].cmd.size() << " " << s << " ";
    }

    f.close();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persist_snapshot(const std::vector<char> &snapshot) {
    mtx.lock();

    std::fstream f;
    f.open(snapshot_name, std::ios::out | std::ios::trunc);

    std::string s;
    s.assign(snapshot.begin(), snapshot.end());

    f << s;

    f.close();

    mtx.unlock();
    return;
}

template<typename command>
void raft_storage<command>::restore(int &cur_term, int &vote_for, int &last_include_index, 
                    int &last_include_term, std::vector<char> &snapshot, std::vector<log_entry<command>> &logs) {
    mtx.lock();
    std::ifstream meta_file(meta_name, std::ios::in);
    std::ifstream logs_file(logs_name, std::ios::in);
    std::ifstream snapshot_file(snapshot_name, std::ios::in);

    if (meta_file.good()){
        meta_file >> cur_term >> vote_for >> last_include_index >> last_include_term;
        // printf("current_term: %d vote_for: %d\n", cur_term, vote_for);
    }

    if (logs_file.good()) {
        int term = 0;
        
        while (logs_file >> term) {
            log_entry<command> new_entry;
            new_entry.term = term;
            int size = 0;
            logs_file >> size;

            char cmd[size + 1] = {0};

            // skip a ' '
            logs_file.seekg(1, std::ios_base::cur);

            logs_file.read(cmd, size);
            new_entry.cmd.deserialize(cmd, size);

            logs.push_back(new_entry);
            // std::cout << "term: " << new_entry.term << " cmd:" << new_entry.cmd.value << "\n";
        }
    }

    if (snapshot_file.good()) {
        snapshot_file.seekg(0, std::ios::end);
        int len = snapshot_file.tellg();
        snapshot_file.seekg(0, std::ios::beg);

        std::string s;
        snapshot_file >> std::setw(len) >> s;
        snapshot.assign(s.begin(), s.end());
    }

    meta_file.close();
    logs_file.close();
    snapshot_file.close();

    mtx.unlock();
    return;
}


#endif // raft_storage_h