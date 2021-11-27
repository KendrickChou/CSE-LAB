// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = this->cl->call(extent_protocol::create, type, id);
  if (ret != extent_protocol::OK) {
    printf("create RPC error: %d\n", ret);
  }
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = this->cl->call(extent_protocol::get, eid, buf);
  if (ret != extent_protocol::OK){
    printf("get RPC error: %d\n", ret);
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = this->cl->call(extent_protocol::getattr, eid, attr);
  if (ret != extent_protocol::OK){
    printf("getattr RPC error: %d\n", ret);
  }
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int flag = 0;
  ret = this->cl->call(extent_protocol::put, eid, buf, flag);
  if (ret != extent_protocol::OK){
    printf("put RPC error: %d\n", ret);
  }
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int flag = 0;
  ret = this->cl->call(extent_protocol::remove, eid, flag);
  if (ret != extent_protocol::OK){
    printf("remove RPC error: %d\n", ret);
  }
  return ret;
}
