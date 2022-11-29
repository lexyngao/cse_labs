#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    int ans = sizeof(command_type) + buf.size() + sizeof(int);
    return ans;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    if(size != this->size())
    return;
    int buf_size = buf.size();
    int pos = 0;

    memcpy(buf_out + pos, (char*) &cmd_tp,sizeof(command_type));
    pos += sizeof(command_type);
    memcpy(buf_out + pos,(char*) &buf_size,sizeof(int));
    pos += sizeof(int);
    memcpy(buf_out + pos,buf.c_str(),buf_size);

    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int buf_size;
    int pos = 0;

    memcpy((char *)&cmd_tp,buf_in + pos,sizeof(command_type));
    pos += sizeof(command_type);
    memcpy((char *)&buf_size,buf_in + pos,sizeof(int));
    pos += sizeof(int);
    buf = std::string(buf_in + pos,buf_size);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << (int) cmd.cmd_tp << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int type;
    u >> type >> cmd.buf;
    cmd.cmd_tp = chfs_command_raft::command_type(type);
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here

    chfs_cmd.res->cv.notify_all();
    return;
}


