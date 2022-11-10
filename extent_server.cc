// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  // restoredata();

}

/**
 * 将checkpoint文件中的快照恢复到disk，
 * 将log文件中的记录恢复到log_entries，并且执行它们
*/
void extent_server::restoredata(){
  printf("开始restore数据\n");
  //检查之前有没有checkpoint过
  // if(!_persister->have_checkpoint())
  // {
  //    printf("没有触发inode的保存呀\n");
  // }
  // else{
  //     // 恢复inode数据 
  //     // im->restore_blocks();
  //     if(im->restore_inodes())
  //     printf("嗯嗯inodes应该恢复成功了捏\n");
  //     else
  //     printf("inodes恢复碰到一点小问题\n");
  // }
   
  
  
  // 恢复log which contains 没有commit的执行的部分
  std::vector<chfs_command>logs = _persister->restore_logdata();
  printf("log里还有数据%d条\n",logs.size());
  // for(chfs_command log :logs)
  // {
  //   printf("没有清除的数据id:%d,type:%d\n",log.id,log.type);
  // }

  std::vector<chfs_command>tx;
  tx.clear();
  bool flag = false;
  while(!logs.empty()){
    chfs_command cmd = logs.front();
    // 如果是begin，开启事务flag=true
    if(cmd.type == chfs_command::CMD_BEGIN){
      tx.clear();
      flag = true;
      logs.erase(logs.begin());
      continue;
    }
    // 如果是commit，执行这个事务
    if(cmd.type == chfs_command::CMD_COMMIT){
      while(!tx.empty()){
        chfs_command cmd = tx.front();
        switch(cmd.type){
          case chfs_command::CMD_CREATE:{
            extent_protocol::extentid_t inum = 0;
            create(cmd.optional, inum);
            break;
          }
          case chfs_command::CMD_PUT:{
            int tmp = 0;
            put(cmd.optional, cmd.content, tmp);
            break;
          }
          case chfs_command::CMD_REMOVE:{
            int tmp = 0;
            remove(cmd.optional, tmp);
          }
          default:
            break;
        }
        tx.erase(tx.begin());
      }
      flag = false;
      logs.erase(logs.begin());
      continue;
    }
    // 剩下指令，如果flag为true，加入事务
    if(flag){
      tx.push_back(cmd);
      logs.erase(logs.begin());
      continue;
    }
    else{
      logs.erase(logs.begin());
      continue;
    }
  }
}

int extent_server::log_begin(unsigned long long txid)
{
  // printf("enter log_create \n");
  chfs_command::cmd_type cmd_type = chfs_command::CMD_BEGIN;
  chfs_command command(txid, cmd_type, "");
  _persister->append_log(command);
  return 0;
}

void extent_server::log_commit(unsigned long long tid){
    chfs_command::cmd_type cmd_type = chfs_command::CMD_COMMIT;
    chfs_command command(tid, cmd_type, "", 0);
    _persister->append_log(command);
    
    }

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  // printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_server::log_create(unsigned long long txid, uint32_t type)
{
  // printf("enter log_create \n");
  chfs_command::cmd_type cmd_type = chfs_command::CMD_CREATE;
  chfs_command command(txid, cmd_type, "", type);
  _persister->append_log(command);
  return 1;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  
  return extent_protocol::OK;
}


int extent_server::log_put(unsigned long long txid, unsigned long long inum, 
                          std::string buf,std::string origin_buf)
{
  // printf("enter log_put \n");
  chfs_command::cmd_type cmd_type = chfs_command::CMD_PUT;
  chfs_command command(txid, cmd_type, buf, inum,origin_buf);
  _persister->append_log(command);
  return 1;
}



int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);
 
  return extent_protocol::OK;
}

int extent_server::log_remove(unsigned long long txid, unsigned long long inum,std::string origin_buf){
  chfs_command::cmd_type cmd_type = chfs_command::CMD_REMOVE;
  chfs_command command(txid, cmd_type, "", inum,origin_buf);
  _persister->append_log(command);
  return 1;
}

void extent_server::set_checkpoint()
{
  //检查log文件大小
  if(!_persister->need_checkpoint())
  return;

  //TODO:这里是因为每次都在commit之后check有没有超过份额 所以没有考虑uncommit的事务

  // 保存现在的快照->blocks有点太底层了 所以还是保存inodes
  // char *blocks;
  // im->snapshot_blocks(blocks);
  // printf("保存好所有之前的status\n");
  if(im->save_inodes())
  printf("inodes应该已经保存好了\n");
  

  //清除log
  _persister->clear_log();
  //TODO:那些没有commit的命令
}
