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
    if (type == 0 ||type == 1) { // Reply to vote request
      #ifdef TX_DEBUG
      printf("*** Deliver Event - Consensus Id = %lu - %s\n", pcd->data.consId, type == 0 ? "No" : "Yes");
      #endif
      if (type == 1) { // Positive reply
        ConsensusData *consData = ConsensusData::getConsensusData(pcd->data.consId);
        if (consData) {
          consData->addAck();
          if (consData->enoughAcks()) { consData->lead(); }
        }
      }
    } else if (type == 2) { // Ack
      ConsensusData *consData = ConsensusData::getConsensusData(pcd->data.consId);
      if (consData) {
        consData->addDecisionAck();
        if (consData->allDecisionAcks()) {
          if (!consData->isStarted()) { consData->setCanDelete(); consData->tryDelete(); }
        }
      }
    }
  } else {
    pcd->data.type = -1;   // indicates an error
  }
  return; // free buffer
}


int consTimeoutHandler(void* arg) {
  // Random timeout
  int delay = rand() % CONS_DELAY;
  struct timespec tim;
  tim.tv_sec  = 0;
  tim.tv_nsec = delay * 1000;
  nanosleep(&tim, NULL);
  ConsensusData *data = (ConsensusData*) arg;
  if (data) { data->timeoutEvent(); }
  return 0;
}

int consDeleteHandler(void* arg) {
  ConsensusData *data = (ConsensusData*) arg;
  if (data) { data->tryDelete(); }
  return 0;
}

// Hash table to store consensus date with id
HashTable<u64,ConsensusData>* ConsensusData::consDataObjects = new HashTable<u64,ConsensusData>(100);

ConsensusData* ConsensusData::getConsensusData(u64 key) {
  ConsensusData* data = consDataObjects->lookup(key);
  return data;
}

void ConsensusData::insertConsensusData(ConsensusData *data) {
  consDataObjects->insert(data);
}

void ConsensusData::removeConsensusData(ConsensusData *data) {
  consDataObjects->remove(data);
}


ConsensusData::ConsensusData(Set<IPPortServerno> *set, IPPortServerno no, Ptr<RPCTcp> rpc, u64 k) {

  canDelete = false;
  consId = k;
  Rpcc = rpc;
  serverset = set;
  server = no;
  r = true;

  started = false;
  elected = false;
  done = false;
  tryingLead = false;
  phase = 0;
  nbAcks = 0;
  decisionAcks = 0;

  insertConsensusData(this);

}

void ConsensusData::setTimeout() {
  TaskEventScheduler::AddEvent(tgetThreadNo(), consTimeoutHandler, (ConsensusData*) this, 0, 0);
}

void ConsensusData::propose(bool v) {

  #ifdef TX_DEBUG_2
  InbacData::nbTotalCons++;
  #endif

  started = true;
  vote = v;
  setTimeout();
}

void ConsensusData::timeoutEvent() {
  if (r) {
    if (!done) {
      
      if (phase < 1000) {
        phase++;
        voted = false;
        nbAcks = 0;
        tryingLead = true;

        #ifdef TX_DEBUG
        printf("*** Timeout Event - Inbac ID = %lu - Consensus Round = %d\n", consId, phase);
        #endif

        SetNode<IPPortServerno> *it;

        // Ask for vote
        for (it = serverset->getFirst(); it != serverset->getLast();
             it = serverset->getNext(it)) {
          if (IPPortServerno::cmp(it->key, server) != 0) {

            ConsensusMessageRPCData *rpcdata = new ConsensusMessageRPCData;
            rpcdata->data = new ConsensusMessageRPCParm;
            rpcdata->data->type = 0;
            rpcdata->data->consId = consId;
            rpcdata->data->phase = phase;
            ConsensusMessageCallbackData *imcd = new ConsensusMessageCallbackData;

            #ifdef TX_DEBUG
            printf("Asking election vote to %u:%u in consensus\n",
                it->key.ipport.ip, it->key.ipport.port);
            #endif
            Rpcc->asyncRPC(it->key.ipport, CONSMESSAGE_RPCNO, 0, rpcdata,
                            consmessagecallback, imcd);

          }
        }
        setTimeout();
        
      } else {
        vote = false;
        lead();
      }
    }
  } else {
    r = true;
  }
}

void ConsensusData::lead() {
  if (!done) {
    done = true;
    elected = true;

    SetNode<IPPortServerno> *it;

    // Send decision
    for (it = serverset->getFirst(); it != serverset->getLast();
         it = serverset->getNext(it)) {
      if (IPPortServerno::cmp(it->key, server) != 0) {

        ConsensusMessageRPCData *rpcdata = new ConsensusMessageRPCData;
        rpcdata->data = new ConsensusMessageRPCParm;
        rpcdata->data->type = vote ? 1 : 2;
        rpcdata->data->consId = consId;
        rpcdata->data->phase = phase;
        ConsensusMessageCallbackData *imcd = new ConsensusMessageCallbackData;

        #ifdef TX_DEBUG
        printf("Sending election decision %s to %u:%u in consensus\n",
            vote ? "Commit" : "Abort", it->key.ipport.ip, it->key.ipport.port);
        #endif
        Rpcc->asyncRPC(it->key.ipport, CONSMESSAGE_RPCNO, 0, rpcdata,
                        consmessagecallback, imcd);

      }
    }
    InbacData *inbacData = InbacData::getInbacData(consId);
    if (inbacData) { inbacData->decide(vote); }
  }
}

void ConsensusData::tryDelete() {
  if (canDelete) {
    removeConsensusData(this);
    delete this;
  } else {
    TaskEventScheduler::AddEvent(tgetThreadNo(), consDeleteHandler, (ConsensusData*) this, 0, MSG_DELAY);
  }
}
