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

int inbacTimeoutHandler(void* arg);

struct InbacMessageCallbackData {
  Semaphore sem; // to wait for response
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
  int maxNbCrashed;
  RPCTaskInfo *rti;

  bool r1;
  bool r2;

  int phase;
  bool proposed;
  bool decided;
  Set<IPPortServerno> *collection0;
  bool and0;
  Set<SetPair> *collection1;
  bool and1;
  Set<IPPortServerno> *collectionHelp;
  bool andHelp;
  bool wait;
  bool val;
  bool decision;
  bool proposal;
  int cnt;
  int cntHelp;

  static HashTable<u64,InbacData> *inbacDataObjects;

  void timeoutEvent0();
  void timeoutEvent1();

  bool checkAllVotes1();
  bool checkBackupVotes1();
  bool checkAllExistVotes1();
  void addAllVotes1ToVotes0();

  void consensusRescue1();
  void consensusRescue2();

  bool checkHelpVotes();

  void tryDelete();

public:

  static int nbTotalTx;
  static int nbTotalCons;
  static int nbTotalAbort;

  InbacData *prev, *next, *sprev, *snext;

  u64 inbacId;
  u64 GetKey() { return inbacId; }

  InbacData() {}
  InbacData(InbacDataParm *parm);

  void propose(int vote);
  void decide(bool d);
  void timeoutEvent();
  void timeoutEventHelp();

  int getPhase() { return phase; }
  int getId() { return id; }
  bool getDecision() { return decision; }
  int getNNodes() { return serverset->getNitems(); }
  void incrCnt() { cnt++;  }
  void incrCntHelp() { cntHelp++;  }
  int getCnt() { return cnt; }
  int getCntHelp() { return cntHelp; }
  int getF() { return maxNbCrashed; }
  bool waiting() { return wait; }

  int addVote0(IPPortServerno owner, bool vote);
  int addVote0(Set<IPPortServerno> *owners, bool vote);
  Set<IPPortServerno>* getVote0() { return collection0; }
  bool getAnd0() { return and0; }

  void addVote1(Set<IPPortServerno> *owners, bool vote);

  int addVoteHelp(Set<IPPortServerno> *owners, bool vote);

  static InbacData* getInbacData(u64 key);
  static void insertInbacData(InbacData *data);
  static void removeInbacData(InbacData *data);
  static int HashKey(u64 n) { return (int) n; }
  static int CompareKey(u64 a, u64 b) {
      if (a < b) { return -1; }
      else if (a == b) { return 0; }
      else { return +1; }
  }

  void setR1(bool b) { r1 = b; }
  void setR2(bool b) { r2 = b; }

  static const char* toString(IPPortServerno &p, bool b) {
    std::stringstream ss;
    ss << "<" << p.ipport.ip << ":" << p.ipport.port;
    ss << "," << (b ? "true" : "false") << ">";
    return ss.str().c_str();
  }
  static const char* toString(Set<IPPortServerno> *set, bool b) {
    std::stringstream ss;
    ss << "<[";
    SetNode<IPPortServerno> *it;
    for (it = set->getFirst(); it != set->getLast(); it = set->getNext(it)) {
      ss << it->key.ipport.ip << ":" << it->key.ipport.port << ", ";
    }
    ss << "]," << (b ? "true" : "false") << ">";
    return ss.str().c_str();
  }

};

void* startInbac(void *arg_);

#endif
