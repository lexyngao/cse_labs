#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        // other operations
        CMD_CREATE,
        CMD_PUT,
        CMD_GET,
        CMD_GETATTR,
        CMD_REMOVE,
        // for debug
        CMD_ERROR


    };

    struct numberSet
    {
        txid_t id = 0;
        cmd_type type = CMD_BEGIN;
        unsigned long long optional;
        unsigned long long contentsize = 0;
        void assign(txid_t id1, cmd_type type1, unsigned long long optional1, unsigned long long contentsize1){
            id = id1;
            type = type1;
            optional = optional1;
            contentsize = contentsize1;
        }
    };
    numberSet numSet;


    cmd_type type = CMD_BEGIN;
    txid_t id = 0;

    //
    // bool need_ino = 0;
    unsigned long long ino_num = 0;
    unsigned long long length_of_buf = 0;
    std::string buf = "";


std::string content = "";
    unsigned long long optional = 0;
    unsigned long long contentsize = 0;

    // constructor
    chfs_command(txid_t txid, cmd_type cmd_type, std::string str="", unsigned long long op=0) {
        id = txid;
        type = cmd_type;
        content = str;
        optional = op;
        contentsize = str.size();
        numSet.assign(txid, cmd_type, op, contentsize);
    }


    // constructor
    chfs_command(txid_t txid, cmd_type cmd_type,
        unsigned long long ino_num,std::string buf) {
            this->id = txid;
            this->type = cmd_type;
            this->ino_num = ino_num;
            this->buf = buf;
        }

    uint64_t size() const {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
        return s;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const chfs_command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    std::vector<chfs_command> restore_logdata();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::append_log(const chfs_command& log) {
    // Your code here for lab2A
    // ???
    log_entries.push_back(log);

    FILE *logf = fopen(file_path_logfile.c_str(), "a+b");
    if(logf == NULL){
        printf("append: log文件打开失败");
    }
    fwrite(&log.numSet, sizeof(log.numSet), 1, logf);
    fwrite(log.content.c_str(), log.content.size(), 1, logf);
    fclose(logf);

}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

}

template<typename command>
std::vector<chfs_command> persister<command>::restore_logdata() {

    // Your code here for lab2A
    FILE* logf = fopen(file_path_logfile.c_str(), "rb");
    if(logf == NULL){
        printf("restoreLogData: 文件不存在,新建！ \n");
        logf = fopen(file_path_logfile.c_str(), "wb");
        fclose(logf);
        return log_entries;
    }
    while(!feof(logf)){
        std::string content = "xxx";
        chfs_command::numberSet set;
        fread(&set, sizeof(set), 1, logf);
        content.resize(set.contentsize);
        fread((char*)content.c_str(), set.contentsize, 1, logf);
        chfs_command cmd(set.id, set.type, content, set.optional);
        log_entries.push_back(cmd);
    }
    fclose(logf);
    return log_entries;

};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

};

using chfs_persister = persister<chfs_command>;

#endif // persister_h