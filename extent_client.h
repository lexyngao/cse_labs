// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;
  extent_server *es;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  // int begin_cmd(){return es->log_begin();}
  // void commit_cmd(unsigned long long txid){ es->log_commit(txid);}
  // void create_cmd(unsigned long long txid, uint32_t type){es->log_create(txid, type);}
  // void put_cmd(unsigned long long txid, unsigned long long inum, std::string str){
  //   es->log_put(txid, inum, str);
  // }
  // void remove_cmd(unsigned long long txid, unsigned long long inum){
  //   es->log_remove(txid, inum);
  // }

};

#endif 

