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

    struct fixed_struct
    {
        txid_t id = 0;
        cmd_type type = CMD_BEGIN;
        unsigned long long inode;
        unsigned long long contentsize = 0;
        unsigned long long origin_size = 0;

         void assign(txid_t id1, cmd_type type1, unsigned long long optional1,
         unsigned long long contentsize1){
            id = id1;
            type = type1;
            inode = optional1;
            contentsize = contentsize1;
        
        }

        void assign_origin(txid_t id1, cmd_type type1, unsigned long long optional1,
         unsigned long long contentsize1, unsigned long long origin_size1){
            id = id1;
            type = type1;
            inode = optional1;
            contentsize = contentsize1;
            origin_size = origin_size1;
        }
    };
    fixed_struct Set;


    cmd_type type = CMD_BEGIN;
    txid_t id = 0;

    //
    // bool need_ino = 0;
    unsigned long long ino_num = 0;
    unsigned long long length_of_buf = 0;
    std::string buf = "";


    std::string content = "";
    std::string origin_content = "";
    unsigned long long optional = 0;
    unsigned long long contentsize = 0;
    unsigned long long origin_size = 0;

    // constructor
    // chfs_command(txid_t txid, cmd_type cmd_type, std::string str="", unsigned long long op=0) {
    //     id = txid;
    //     type = cmd_type;
    //     content = str;
    //     optional = op;
    //     contentsize = str.size();
    //     Set.assign(txid, cmd_type, op, contentsize);
    // }

    // TODO: 这个undo log暂时也没有实现

    // constructor_undo
    chfs_command(txid_t txid, cmd_type cmd_type, std::string str="", unsigned long long op=0, std::string str_before = "") {
        id = txid;
        type = cmd_type;
        content = str;
        optional = op;
        contentsize = str.size();
        origin_size = str_before.size();
        
        Set.assign_origin(txid, cmd_type, op, contentsize, str_before.size());
    }

    // TODO：为啥不能拆开传我也不知道

    // constructor
    // chfs_command(txid_t txid, cmd_type cmd_type,
    //     std::string buf = "",unsigned long long ino_num = 0) {
    //         this->id = txid;
    //         this->type = cmd_type;
    //         this->ino_num = ino_num;
    //         this->buf = buf;
    //         this->length_of_buf = buf.size();
    //     }

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
    void clear_log();
    bool save_blocks(const char *buf);

    // restore data from solid binary file
    // You may modify parameters in these functions
    std::vector<chfs_command> restore_logdata();
    bool have_checkpoint();

    bool need_checkpoint();

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
    // fwrite(&log.id,sizeof(log.id),1,logf);
    // fwrite(&log.type,sizeof(log.type),1,logf);
    // fwrite(&log.ino_num,sizeof(log.ino_num),1,logf);
    // fwrite(&log.length_of_buf,sizeof(log.length_of_buf),1,logf);
    // fwrite(&log.buf.c_str(),log.buf.size,1,logf);

    fwrite(&log.Set, sizeof(log.Set), 1, logf);
    fwrite(log.content.c_str(), log.content.size(), 1, logf);
    fclose(logf);

    

}

template<typename command>
bool persister<command>::need_checkpoint()
{
    bool need = 0;
    //以只读的方式打开log文件
    FILE *logfile = fopen(file_path_logfile.c_str(), "r");
    fseek(logfile, 0, SEEK_END);
    long log_size = ftell(logfile);
    
    printf("log文件的size: %ld\n", log_size);

    if(log_size >= MAX_LOG_SZ)
    need = true;

    fclose(logfile);
    return need;
}


template<typename command>
void persister<command>::clear_log() {
    
    //清除log文件：再用w方式打开
    FILE *pfd = NULL;
    pfd = fopen(file_path_logfile.c_str(), "w+");
    if(pfd)
    {
        printf("已清除!clear log file context!\n");
        fclose(pfd);
    }

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
        chfs_command::fixed_struct set;
        fread(&set, sizeof(set), 1, logf);
        content.resize(set.contentsize);
        fread((char*)content.c_str(), set.contentsize, 1, logf);
        chfs_command cmd(set.id, set.type, content, set.inode);
        log_entries.push_back(cmd);

        // txid_t id = 0;
        // fread(&id, sizeof(id), 1, logf);
        // chfs_command::cmd_type type = chfs_command::CMD_ERROR;
        // fread(&type, sizeof(type), 1, logf);
        // unsigned long long ino_num = 0;
        // fread(&ino_num, sizeof(ino_num), 1, logf);
        // unsigned long long length = 0;
        // fread(&length,sizeof(length),1,logf);
        // content.resize(length);
        // fread((char*)content.c_str(), length, 1, logf);
        // chfs_command cmd(id,type,ino_num,content);
        // log_entries.push_back(cmd);
    }
    fclose(logf);
    return log_entries;

};

template<typename command>
bool persister<command>::have_checkpoint() {
   
    FILE * checkfile = fopen(file_path_checkpoint.c_str(), "r");

    if(!checkfile)
    {
        printf("之前没有触发checkpoint\n");
        return false;
    }
    
    return true;
    
};


template<typename command>
bool persister<command>::save_blocks(const char *buf){
    // 打开checkpoint文件
    FILE * checkfile = fopen(file_path_checkpoint.c_str(), "wb");
    // debug checkpoint
    printf("block经过层层传递的内容是%s\n",buf);
    fwrite(buf,sizeof(char),strlen(buf),checkfile);
    fclose(checkfile);
    
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h