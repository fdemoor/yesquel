//
// inbac.h
//
// Data structures and event handlers for INBAC protocol
//

/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  Modified code: Copyright (c) 2017 LPD, EPFL
  All rights reserved.

  Written by Florestan De Moor

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the "Software"), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

#ifndef _INBAC_H
#define _INBAC_H

#include "ipmisc.h"
#include "task.h"
#include "datastruct.h"
#include "options.h"
#include "gaiarpcaux.h"
#include "grpctcp.h"

#include <chrono>
#include <thread>

#include <boost/dynamic_bitset.hpp>

class InbacData;

int inbacTimeoutHandler(void* arg);
int inbacTimeoutHandler(InbacData* data, bool type);

struct InbacMessageCallbackData {
  InbacMessageRPCResp data;
  InbacMessageCallbackData *prev, *next; // linklist stuff
};
void inbacmessagecallback(char *data, int len, void *callbackdata);

struct InbacDataParm {
  InbacRPCParm *parm;
  IPPort ipport;
  Ptr<RPCTcp> rpc;
  u64 k;
  int vote;
  CommitRPCData *commitData;
  RPCTaskInfo *rti;
};

class InbacData {

private:

  Ptr<RPCTcp> Rpcc;
  Set<IPPortServerno> *serverset;
  int id;
  IPPortServerno server;
  CommitRPCData *crpcdata;
  RPCTaskInfo *rti;

  int maxNbCrashed;
  int NNodes;

  bool t0;
  bool t1;
  bool d0;
  bool d1;

  int phase;
  bool proposed;
  bool decided;

  boost::dynamic_bitset<> collection0;
  int size0;
  bool and0;
  int *votes0;

  bool and1;
  bool all1;

  boost::dynamic_bitset<> collectionHelp;
  int sizeHelp;
  bool andHelp;

  bool wait;
  bool val;
  bool decision;
  bool proposal;
  int cnt;
  int cntHelp;

  static HashTable<u64,InbacData> *inbacDataObjects;

  static LinkList<InbacMessageRPCParm> *msgQueue;

  void timeoutEvent0();
  void timeoutEvent1();

  int addVote0(int owner, bool vote);
  void addVote1(int *owners, int size, bool vote);

  bool checkAllExistVotes1();

  void consensusRescue1();
  void consensusRescue2();

  bool checkHelpVotes();

  void tryDelete();

public:

  #ifdef TX_DEBUG_2
  static int nbTotalTx;
  static int nbTotalCons;
  static int nbTotalAbort;
  static int nbSpeedUp0;
  static int nbSpeedUp1;
  #endif

  InbacData *prev, *next, *sprev, *snext;

  u64 inbacId;
  u64 GetKey() { return inbacId; }

  InbacData() {}
  InbacData(InbacDataParm *parm);

  void propose(int vote);
  void decide(bool d);
  void timeoutEvent(bool type);
  void timeoutEventHelp();
  void deliver0(int owner, bool vote);
  void deliver1(int *owners, int size, bool vote, bool all);
  void deliverHelp(int *owners, int size, bool vote);

  int getId() { return id; }
  int getF() { return maxNbCrashed; }

  int* getVote0() { return votes0; }
  int getSize0() { return size0; }
  bool getAnd0() { return and0; }

  static InbacData* getInbacData(u64 key);
  static void insertInbacData(InbacData *data);
  static void removeInbacData(InbacData *data);
  static int HashKey(u64 n) { return (int) n; }
  static int CompareKey(u64 a, u64 b) {
      if (a < b) { return -1; }
      else if (a == b) { return 0; }
      else { return +1; }
  }

  static const char* toString(int p, bool b) {
    std::stringstream ss;
    ss << "<" << p;
    ss << "," << (b ? "true" : "false") << ">";
    return ss.str().c_str();
  }

  static const char* toString(int *set, int size, bool b) {
    std::stringstream ss;
    ss << "<[";
    for (int i = 0; i < size; i++) {
      ss << set[i] << ", ";
    }
    ss << "]," << (b ? "true" : "false") << ">";
    return ss.str().c_str();
  }

  static void addMsgQueue(InbacMessageRPCParm* msg) { msgQueue->pushHead(msg); }

};

struct InbacTimeoutData {
  InbacData *data;
  bool type; // False: type 0, true: type 1
};

void startInbac(void *arg);

#endif
