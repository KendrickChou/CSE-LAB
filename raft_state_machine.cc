#include "raft_state_machine.h"
#include <sstream>
#include <iomanip>
#include <cstring>

kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    size_t res = std::to_string(cmd_tp).size() + 1
                 + std::to_string(key.size()).size() + 1
                  + key.size() + 1
                   + std::to_string(value.size()).size() + 1
                    + value.size();
    return res;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    if (size != this->size()) {
        printf("serialize failed: size unmatch\n");
        return;
    }

    std::string s;
    s = std::to_string(cmd_tp) + " " 
        + std::to_string(key.size()) + " "
         + key + " "
          + std::to_string(value.size()) + " "
            + value;

    std::strncpy(buf, s.c_str(), s.size());
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    std::string s(buf, size);
    std::stringstream ss(s);

    int key_size = 0, value_size = 0;
    int type_ = 0;
    ss >> type_;
    ss >> key_size;
    ss.seekg(1, std::ios_base::cur);

    // prevent \0
    ss >> std::setw(key_size) >> key;

    ss >> value_size;
    ss.seekg(1, std::ios_base::cur);
    ss >> std::setw(value_size) >> value;

    cmd_tp = static_cast<kv_command::command_type>(type_);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    int type_ = cmd.cmd_tp;
    m << type_ << cmd.key << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int type_;
    u >> type_;
    cmd.cmd_tp = static_cast<kv_command::command_type>(type_);
    u >> cmd.key;
    u >> cmd.value;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->done = true;
    auto iter = kv.find(kv_cmd.key);

    switch (kv_cmd.cmd_tp)
    {
    case kv_command::CMD_GET:
        kv_cmd.res->value = (iter == kv.end()) ? "" : iter->second;
        kv_cmd.res->succ = (kv_cmd.res->value.size()) ? true : false;
        break;
    case kv_command::CMD_DEL:
        kv_cmd.res->value = (iter == kv.end()) ? "" : iter->second;
        kv.erase(iter);
        kv_cmd.res->succ = (kv_cmd.res->value.size()) ? true : false;
        break;
    case kv_command::CMD_PUT:
        kv_cmd.res->value = (iter == kv.end()) ? kv_cmd.value : iter->second;
        kv[kv_cmd.key] = kv_cmd.value;
        kv_cmd.res->succ = (kv_cmd.res->value == kv_cmd.value) ? true : false;
        break;
    default:
        break;
    }

    kv_cmd.res->cv.notify_all();

    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:      
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<char> data;
    std::stringstream ss;
    int kv_size = kv.size();
    ss << kv_size;
    for (auto iter : kv) {
        int key_size = iter.first.size();
        int value_size = iter.second.size();

        ss << " " << key_size << " " << iter.first 
            << " " << value_size << " " << iter.second;
    }

    std::string str = ss.str();
    data.assign(str.begin(), str.end());

    return data;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    std::string str;
    str.assign(snapshot.begin(), snapshot.end());
    std::stringstream ss(str);

    kv.clear();

    int kv_size = 0;
    ss >> kv_size;

    for (int i = 0; i < kv_size; ++i) {
        int key_size = 0, value_size = 0;
        std::string key,value;

        ss >> key_size;
        ss.seekg(1, std::ios_base::cur);
        ss >> std::setw(key_size) >> key;

        ss >> value_size;
        ss.seekg(1, std::ios_base::cur);
        ss >> std::setw(value_size) >> value;

        kv[key] = value;
    }

    return;    
}
