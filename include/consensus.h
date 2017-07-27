//
// consensus.h
//
// Data structures and event handlers for raft leader election
// Used as consensus module for INBAC protocol
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

#ifndef _CONSENSUS_H
#define _CONSENSUS_H

#include "ipmisc.h"
#include "task.h"
#include "datastruct.h"
#include "options.h"
#include "gaiarpcaux.h"
#include "grpctcp.h"
#include "inbac.h"

// Timeout function
int consensusTimeoutHandler(void* arg);

// Callback
struct ConsensusMessageCallbackData {
  ConsensusMessageRPCResp data;
  ConsensusMessageCallbackData *prev, *next; // linklist stuff
};
void consmessagecallback(char *data, int len, void *callbackdata);


class ConsensusData {

private:

  Ptr<RPCTcp> Rpcc;
  Set<IPPortServerno> *serverset;
  int id;
  IPPortServerno server;
  CommitRPCData *crpcdata;
  bool canDelete;

  bool started;
  bool vote;
  bool elected;
  bool done;
  bool tryingLead;
  int phase;
  int nbAcks;
  int decisionAcks;
  bool voted;

  bool r; // Boolean to discard some timeouts

  static HashTable<u64,ConsensusData> *consDataObjects;

public:

  void setTimeout();
  void setR() { r = false; }
  u64 consId;
  ConsensusData *prev, *next, *sprev, *snext;
  ConsensusData() {}
  ConsensusData(Set<IPPortServerno> *set, IPPortServerno no, Ptr<RPCTcp> rpc, u64 k);
  void propose(bool v);
  void timeoutEvent();
  void lead();
  void addAck() { nbAcks++; }
  void addDecisionAck() { decisionAcks++; }
  bool isElected() { return elected; }
  bool hasVoted() { return voted; }
  void doVote() { voted = true; }
  bool allDecisionAcks() { return decisionAcks == getNNodes(); }
  bool enoughAcks() { return (nbAcks + 1  > (getNNodes() / 2)); }
  bool isTryingLead() { return tryingLead; }
  void resetTryingLead() { tryingLead = false; }
  void setCanDelete() { canDelete = true; }
  void setDone() { done = true; }
  void tryDelete();
  bool isStarted() { return started; }
  int getPhase() { return phase; }
  int getNNodes() { return serverset->getNitems(); }
  u64 GetKey() { return consId; }
  void catchup(int p) { if (p > phase) { phase = p; voted = false; } }
  static ConsensusData* getConsensusData(u64 key);
  static void insertConsensusData(ConsensusData *data);
  static void removeConsensusData(ConsensusData *data);
  static int HashKey(u64 n) { return (int) n; }
  static int CompareKey(u64 a, u64 b) {
      if (a < b) { return -1; }
      else if (a == b) { return 0; }
      else { return +1; }
  }

};

#endif
