//
// consensus.cpp
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

#include "consensus.h"

void consmessagecallback(char *data, int len, void *callbackdata) {
  ConsensusMessageCallbackData *pcd = (ConsensusMessageCallbackData*) callbackdata;
  ConsensusMessageRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    pcd->data = *rpcresp.data;
    int type = pcd->data.type;
    if (type == 0 ||type == 1) {
      printf("*** Deliver Event - Consensus Id = %d - %s\n", pcd->data.consId, type == 0 ? "No" : "Yes");
      if (type == 1) {
        ConsensusData *consData = ConsensusData::getConsensusData(pcd->data.consId);
        consData->addAck();
        if (consData->enoughAcks()) { consData->lead(); }
      }
    }
  } else {
    pcd->data.type = -1;   // indicates an error
  }
  return; // free buffer
}


int consTimeoutHandler(void* arg) {
  ConsensusData *data = (ConsensusData*) arg;
  if (data) { data->timeoutEvent(); }
  return 0;
}

HashTable<int,ConsensusData>* ConsensusData::consDataObjects = new HashTable<int,ConsensusData>(100);

ConsensusData* ConsensusData::getConsensusData(int key) {
  ConsensusData* data = consDataObjects->lookup(key);
  return data;
}

void ConsensusData::insertConsensusData(ConsensusData *data) {
  consDataObjects->insert(data);
}

void ConsensusData::removeConsensusData(ConsensusData *data) {
  consDataObjects->remove(data);
}


ConsensusData::ConsensusData(Set<IPPortServerno> *set, IPPortServerno no, Ptr<RPCTcp> rpc, int k) {

  consId = k;
  Rpcc = rpc;
  serverset = set;
  server = no;

  elected = false;
  done = false;
  tryingLead = false;
  phase = 0;
  nbAcks = 0;

  insertConsensusData(this);

}

void ConsensusData::setTimeout() {
  int delay = rand() % MSG_DELAY;
  TaskEventScheduler::AddEvent(tgetThreadNo(), consTimeoutHandler, (ConsensusData*) this, 0, delay);
}

void ConsensusData::propose(bool v) {
  vote = v;
  setTimeout();
}

void ConsensusData::timeoutEvent() {
  if (!done) {
    phase++;
    nbAcks = 0;
    tryingLead = true;

    printf("*** Timeout Event - Inbac ID = %d - Consensus Round = %d\n", consId, phase);

    SetNode<IPPortServerno> *it;

    ConsensusMessageRPCData *rpcdata = new ConsensusMessageRPCData;
    rpcdata->data = new ConsensusMessageRPCParm;
    rpcdata->data->type = 0;
    rpcdata->data->consId = consId;
    rpcdata->data->phase = phase;
    ConsensusMessageCallbackData *imcd = new ConsensusMessageCallbackData;

    for (it = serverset->getFirst(); it != serverset->getLast();
         it = serverset->getNext(it)) {
      if (IPPortServerno::cmp(it->key, server) != 0) {

        printf("Asking election vote to %u:%u in consensus\n",
            it->key.ipport.ip, it->key.ipport.port);
        Rpcc->asyncRPC(it->key.ipport, CONSMESSAGE_RPCNO, 0, rpcdata,
                        consmessagecallback, imcd);

      }
    }
    setTimeout();
  }
}

void ConsensusData::lead() {
  if (!done) {
    done = true;
    elected = true;

    SetNode<IPPortServerno> *it;

    ConsensusMessageRPCData *rpcdata = new ConsensusMessageRPCData;
    rpcdata->data = new ConsensusMessageRPCParm;
    rpcdata->data->type = vote ? 1 : 2;
    rpcdata->data->consId = consId;
    rpcdata->data->phase = phase;
    ConsensusMessageCallbackData *imcd = new ConsensusMessageCallbackData;

    for (it = serverset->getFirst(); it != serverset->getLast();
         it = serverset->getNext(it)) {
      if (IPPortServerno::cmp(it->key, server) != 0) {

        printf("Sending election decision %s to %u:%u in consensus\n",
            vote ? "Commit" : "Abort", it->key.ipport.ip, it->key.ipport.port);
        Rpcc->asyncRPC(it->key.ipport, CONSMESSAGE_RPCNO, 0, rpcdata,
                        consmessagecallback, imcd);

      }
    }
    InbacData *inbacData = InbacData::getInbacData(consId);
    inbacData->decide(vote);
    removeConsensusData(this);
    delete this;
  }
}
