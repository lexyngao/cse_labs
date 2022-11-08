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
  unsigned long long tx_counter = 1;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  unsigned long long begin_tx()
  {
    es->log_begin(tx_counter);
    return tx_counter;
  }

  void commit_tx(unsigned long long txid)
  {
   
    //other things to do
    es->log_commit(txid);
    tx_counter++;
    //to check if checkpoint is needed
    es->set_checkpoint();
  }

  extent_server* get_es()
  {
    return es;
  }
};

#endif 

