// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  mutex = PTHREAD_MUTEX_INITIALIZER;
  cond = PTHREAD_COND_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex);

  if (locks.find(lid) == locks.end()) 
  {
    // pthread_cond_t cond;
    // pthread_cond_init(&cond, NULL);
    locks[lid] =  true;
  }
    
  else {
    //waiting..
    while(locks[lid])
      {
        pthread_cond_wait(&cond, &mutex);
      }
    locks[lid] = true;
  }
  pthread_mutex_unlock(&mutex);
  r = ret;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex);
  locks[lid] = false;
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&mutex);
  r = ret;
  return ret;
}