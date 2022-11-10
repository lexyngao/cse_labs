// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;

 public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

  // Your code here for lab2A: add logging APIs
  void restoredata();
  void set_checkpoint();

  int log_begin(unsigned long long);
  void log_commit(unsigned long long tid);
  int log_create(unsigned long long txid, uint32_t type);
  int log_put(unsigned long long txid, unsigned long long inum, std::string,std::string);
  int log_remove(unsigned long long txid, unsigned long long inum,std::string);

  // void restoredisk(std::string checkfile);
  
};

#endif 