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

int inbacTimeoutHandler(void* arg);

struct InbacMessageCallbackData {
  Semaphore sem; // to wait for response
  InbacMessageRPCResp data;
  InbacMessageCallbackData *prev, *next; // linklist stuff
};
void inbacmessagecallback(char *data, int len, void *callbackdata);


class InbacData {

private:

  Ptr<RPCTcp> Rpcc;
  InbacRPCParm *parm;
  int id;
  IPPortServerno server;

  bool r1;
  bool r2;

  int phase;
  bool proposed;
  bool decided;
  Set<VotePair> *collections0;
  Set<SetPair> *collections1;
  Set<VotePair> *collectionHelp;
  bool wait;
  bool val;
  bool decision;
  bool proposal;
  int cnt;
  int cntHelp;

  static HashTable<int,InbacData> *inbacDataObjects;

  void timeoutEvent0();
  void timeoutEvent1();

public:

  int inbacId;
  InbacData *prev, *next, *sprev, *snext;
  InbacData() {}
  InbacData(InbacRPCParm *param, IPPort ipport, Ptr<RPCTcp> rpc, int k);
  void propose(int vote);
  void timeoutEvent();
  int getPhase() { return phase; }
  int getId() { return id; }
  int getNNodes() { return parm->serverset->getNitems(); }
  void incrCnt() { cnt++;  }
  void incrCntHelp() { cntHelp++;  }
  int getCnt() { return cnt; }
  int getCntHelp() { return cntHelp; }
  int addVote0(VotePair vote);
  int addVote0(Set<VotePair> *votes);
  Set<VotePair>* getVote0() { return collections0; }
  void addVote1(Set<VotePair> *set, IPPortServerno owner);
  int GetKey() { return inbacId; }
  static InbacData* getInbacData(int key);
  static void insertInbacData(InbacData *data);
  static int HashKey(int n) { return n; }
  static int CompareKey(int a, int b) {
      if (a < b) { return -1; }
      else if (a == b) { return 0; }
      else { return +1; }
  }
  bool getR1() { return r1; }
  bool getR2() { return r2; }
  void setR1(bool b) { r1 = b; }
  void setR2(bool b) { r2 = b; }

};

#endif
