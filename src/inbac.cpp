//
// inbac.cpp
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

#include "inbac.h"
#include "storageserver.h"

void inbacmessagecallback(char *data, int len, void *callbackdata) {
  InbacMessageCallbackData *pcd = (InbacMessageCallbackData*) callbackdata;
  InbacMessageRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    pcd->data = *rpcresp.data;
    #ifdef TX_DEBUG
    printf("*** Callback Event - Inbac Id = %lu - %d\n", pcd->data.inbacId, pcd->data.type);
    #endif
    if (pcd->data.type == 0) {
      #ifdef TX_DEBUG
      printf("*** Deliver Event - Inbac Id = %lu - %s\n", pcd->data.inbacId, "Helped");
      #endif
      InbacData *inbacData = InbacData::getInbacData(pcd->data.inbacId);
      if (inbacData->getId() >= inbacData->getF()) {
        inbacData->addVoteHelp(pcd->data.owners, pcd->data.vote);
        inbacData->incrCntHelp();
        inbacData->timeoutEventHelp();
      }
    }
  } else {
    pcd->data.type = -1;   // indicates an error
  }
  return; // free buffer
}


int inbacTimeoutHandler(void* arg) {
  InbacTimeoutData *timeoutData = (InbacTimeoutData*) arg;
  if (timeoutData->data) {
    #ifdef TX_DEBUG
    printf("Timeout Event - Inbac Id = %lu\n", data->GetKey());
    #endif
    timeoutData->data->timeoutEvent(timeoutData->type);
  }
  // delete timeoutData;
  return 0;
}

HashTable<u64,InbacData>* InbacData::inbacDataObjects = new HashTable<u64,InbacData>(100);
LinkList<InbacMessageRPCParm>* InbacData::msgQueue = new LinkList<InbacMessageRPCParm>(true);
int InbacData::nbTotalTx = 0;
int InbacData::nbTotalCons = 0;
int InbacData::nbTotalAbort = 0;
int InbacData::nbSpeedUp0 = 0;
int InbacData::nbSpeedUp1 = 0;

InbacData* InbacData::getInbacData(u64 key) {
  InbacData* data = inbacDataObjects->lookup(key);
  return data;
}

void InbacData::insertInbacData(InbacData *data) {
  inbacDataObjects->insert(data);
}

void InbacData::removeInbacData(InbacData *data) {
  inbacDataObjects->remove(data);
}

void InbacData::deliver0(IPPortServerno owner, bool vote) {
  if (phase == 0) {
    int k = addVote0(owner, vote);
    int n = getNNodes();
    if ( (id < maxNbCrashed && k == n) ||
          (id == maxNbCrashed && k == maxNbCrashed) ) {
      InbacData::nbSpeedUp0++;
      InbacTimeoutData *timeoutData = new InbacTimeoutData;
      timeoutData->data = this;
      timeoutData->type = 0;
      TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (void*) timeoutData, 0, 0);
    }
  }
}

void InbacData::deliver1(Set<IPPortServerno> *owners, bool vote) {
  addVote1(owners, vote);
  cnt++;
  if (cnt == maxNbCrashed) {
    InbacData::nbSpeedUp1++;
    InbacTimeoutData *timeoutData = new InbacTimeoutData;
    timeoutData->data = this;
    timeoutData->type = 1;
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (void*) timeoutData, 0, 0);
  }
}

int InbacData::addVote0(IPPortServerno owner, bool vote) {
  if (!collection0->belongs(owner)) { collection0->insert(owner); }
  and0 = and0 && vote;
  return collection0->getNitems();
}

int InbacData::addVote0(Set<IPPortServerno> *owners, bool vote) {
  SetNode<IPPortServerno> *it;
  for (it = owners->getFirst(); it != owners->getLast(); it = owners->getNext(it)) {
    if (!collection0->belongs(it->key)) { collection0->insert(it->key); }
  }
  and0 = and0 && vote;
  return collection0->getNitems();
}

void InbacData::addVote1(Set<IPPortServerno> *owners, bool vote) {
  SetPair *pair = new SetPair;
  pair->set = *owners;
  collection1->insert(*pair);
  and1 = and1 && vote;
}

int InbacData::addVoteHelp(Set<IPPortServerno> *owners, bool vote) {
  SetNode<IPPortServerno> *it;
  for (it = owners->getFirst(); it != owners->getLast(); it = owners->getNext(it)) {
    if (!collectionHelp->belongs(it->key)) { collectionHelp->insert(it->key); }
  }
  andHelp = andHelp && vote;
  return collectionHelp->getNitems();
}


InbacData::InbacData(InbacDataParm *parm) {

  rti = parm->rti;
  crpcdata = parm->commitData;
  inbacId = parm->k;
  Rpcc = parm->rpc;
  serverset = new Set<IPPortServerno>;
  *serverset = *(parm->parm->serverset);
  SetNode<IPPortServerno> *it;
  int i = 0;
  for (it = serverset->getFirst(); it != serverset->getLast();
       it = serverset->getNext(it), i++) {
    if (IPPort::cmp(it->key.ipport, parm->ipport) == 0) {
      id = i;
      server = it->key;
      break;
    }
  }

  int n = getNNodes();
  maxNbCrashed = (MAX_NB_CRASHED < n) ? MAX_NB_CRASHED : n - 1;

  t0 = true;
  t1 = true;

  #ifdef TX_DEBUG
  printf("My INBAC id is %d (%u:%u)\n", id, server.ipport.ip, server.ipport.port);
  #endif

  phase = 0;
  proposed = false;
  decided = false;
  collection0 = new Set<IPPortServerno>;
  and0 = true;
  collection1 = new Set<SetPair>;
  and1 = true;
  collectionHelp = new Set<IPPortServerno>;
  andHelp = true;
  wait = false;
  cnt = 0;
  cntHelp = 0;

  insertInbacData(this);

}

void InbacData::propose(int vote) {

  InbacData::nbTotalTx++;

  #ifdef TX_DEBUG
  printf("*** Propose %s Event - Inbac ID = %lu\n",
    (vote == 0) ? "true" : "false", inbacId);
  printf("Inbac is %p\n", this);
  #endif

  // Set value
  val = (vote == 0) ? true : false;

  // Send value to backups
  SetNode<IPPortServerno> *it;
  int i = 0;
  for (it = serverset->getFirst(); i < maxNbCrashed && it != serverset->getLast();
       it = serverset->getNext(it)) {

    if (IPPortServerno::cmp(it->key, server) != 0) {

      InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
      rpcdata->data = new InbacMessageRPCParm;
      rpcdata->data->vote = val;
      rpcdata->data->owner = server;
      rpcdata->data->type = 0;
      rpcdata->data->inbacId = inbacId;

      InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

      #ifdef TX_DEBUG
      printf("Sending vote to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
      #endif
      Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                      inbacmessagecallback, imcd);

      i++;
    } else {
      deliver0(server, val);
    }
  }

  // Set timer
  InbacTimeoutData *timeoutData = new InbacTimeoutData;
  timeoutData->data = this;
  timeoutData->type = 0;
  if (id <= maxNbCrashed) {
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (void*) timeoutData, 0, MSG_DELAY);
  } else {
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (void*) timeoutData, 0, 2 * MSG_DELAY);
    phase = 1;
  }

  // Look for messages already received
  InbacMessageRPCParm *msgIt;
  InbacMessageRPCParm *msg;
  bool found = false;
  msgIt = msgQueue->getFirst();
  while (msgIt != msgQueue->getLast()) {
    if (msgIt->inbacId == inbacId) {
      found = true;
      msg = msgIt;
      switch (msg->type) {
        case 0: {
          #ifdef TX_DEBUG_2
          printf("*** Found msg in queue - Inbac Id = %lu - %s\n",
              msg->inbacId, InbacData::toString(msg->owner, msg->vote));
          #endif
          deliver0(msg->owner, msg->vote);
          break;
        } case 1: {
          #ifdef TX_DEBUG_2
          printf("*** Found msg in queue - Inbac Id = %lu - %s\n",
              msg->inbacId, InbacData::toString(msg->owners, msg->vote));
          #endif
          deliver1(msg->owners, msg->vote);
          break;
        } default:
          break; // Should not happen
      }
    }
    msgIt = msgQueue->getNext(msgIt);
    if (found) {
      msgQueue->remove(msg);
      delete msg;
      found = false;
    }
  }

}

void InbacData::tryDelete() {
  // removeInbacData(this);
  // delete this;
}

void InbacData::timeoutEvent(int type) {
  if (type == 0 && t0) {
    timeoutEvent0();
    t0 = false;
  } else if (type == 1 && t1) {
    timeoutEvent1();
  } else {
    if (decided) { tryDelete(); }
  }
}

void InbacData::timeoutEvent0() {

  if (phase == 0) {

    #ifdef TX_DEBUG
    printf("*** Timeout 0 Event - Inbac ID = %lu\n", inbacId);
    printf("Inbac is %p\n", this);
    #endif

    SetNode<IPPortServerno> *it;
    int i = 0;

    if (id < maxNbCrashed) {

      for (it = serverset->getFirst(); it != serverset->getLast();
           it = serverset->getNext(it)) {
        if (IPPortServerno::cmp(it->key, server) != 0) {

          InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
          rpcdata->data = new InbacMessageRPCParm;
          rpcdata->data->type = 1;
          rpcdata->data->owners = collection0;
          rpcdata->data->vote = and0;
          rpcdata->data->inbacId = inbacId;
          InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

          #ifdef TX_DEBUG
          printf("Sending backup votes to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
          #endif
          Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                          inbacmessagecallback, imcd);

        } else {
          deliver1(collection0, and1);
        }
      }

    } else if (id == maxNbCrashed) {

      for (it = serverset->getFirst(); i < maxNbCrashed && it != serverset->getLast();
           it = serverset->getNext(it)) {
        if (IPPortServerno::cmp(it->key, server) != 0) {

          InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
          rpcdata->data = new InbacMessageRPCParm;
          rpcdata->data->type = 1;
          rpcdata->data->owners = collection0;
          rpcdata->data->vote = and0;
          rpcdata->data->owner = server;
          rpcdata->data->inbacId = inbacId;
          InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

          #ifdef TX_DEBUG
          printf("Sending backup votes to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
          #endif
          Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                          inbacmessagecallback, imcd);
          i++;
        }
      }
    }

    phase = 1;
    InbacTimeoutData *timeoutData = new InbacTimeoutData;
    timeoutData->data = this;
    timeoutData->type = 1;
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (void*) timeoutData, 0, MSG_DELAY);
  }
}

void InbacData::timeoutEvent1() {

  if (phase == 1 && !decided && !proposed) {

    #ifdef TX_DEBUG
    printf("*** Timeout 1 Event - Inbac ID = %lu\n", inbacId);
    #endif

    phase = 2;
    if (id < maxNbCrashed) {

      if (checkBackupVotes1()) {
        decision = and1;
        decide(decision);
      } else {
        consensusRescue1();
      }

    } else {

      addAllVotes1ToVotes0();
      if (checkAllVotes1()) {
        decision = and1;
        decide(decision);
      } else if (cnt >= 1) {
        consensusRescue1();
      } else {
        wait = true;
        SetNode<IPPortServerno> *it;
        int i = 0;

        for (it = serverset->getFirst(); it != serverset->getLast();
             it = serverset->getNext(it)) {

          if (i >= maxNbCrashed && IPPortServerno::cmp(it->key, server) != 0) {

            #ifdef TX_DEBUG
            printf("Sending Help request to %u:%u\n", it->key.ipport.ip, it->key.ipport.port); fflush(stdout);
            #endif

            InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
            rpcdata->data = new InbacMessageRPCParm;
            rpcdata->data->type = 2;
            rpcdata->data->inbacId = inbacId;
            InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

            Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                            inbacmessagecallback, imcd);
          }
          i++;
        }

        if (id == maxNbCrashed) { cntHelp++; }

        #ifdef TX_DEBUG
        printf("Waiting for help\n"); fflush(stdout);
        #endif

        timeoutEventHelp();

      }
    }

  }
}

void InbacData::timeoutEventHelp() {
  if ( (cnt + cntHelp >= getNNodes() - maxNbCrashed) && wait ) {
    wait = false;
    if (checkAllVotes1()) {
      decision = and1;
      decide(decision);
    } else if (cnt >= 1) {
      consensusRescue1();
    } else {
      consensusRescue2();
    }
  }
}

void InbacData::consensusRescue1() {
  ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
  if (checkAllExistVotes1()) {
    proposal = and1;
    proposed = true;
  } else {
    proposal = false;
    proposed = true;
  }
  #ifdef TX_DEBUG
  printf("***Consensus1 propose %s\n", proposal ? "true" : "false");
  #endif
  consData->propose(proposal);
}

void InbacData::consensusRescue2() {
  ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
  if (checkHelpVotes()) {
    proposal = andHelp;
    proposed = true;
  } else {
    proposal = false;
    proposed = true;
  }
  #ifdef TX_DEBUG
  printf("***Consensus2 propose %s\n", proposal ? "true" : "false");
  #endif
  consData->propose(proposal);
}

bool InbacData::checkAllVotes1() {
  if (collection1->getNitems() != maxNbCrashed) { return false; }
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    int size = it->key.set.getNitems();
    if (size != getNNodes()) { return false; }
  }
  return true;
}

bool InbacData::checkBackupVotes1() {
  if (collection1->getNitems() != maxNbCrashed + 1) { return false; }
  SetNode<SetPair> *it;
  bool foundF = false;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    int size = it->key.set.getNitems();
    if (size == maxNbCrashed) {
      if (foundF) { return false; } else { foundF = true; }
    } else if (size != getNNodes()) { return false; }
  }
  return foundF;
}

bool InbacData::checkAllExistVotes1() {
  Set<IPPortServerno> *owners = new Set<IPPortServerno>;
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    SetNode<IPPortServerno> *it2;
    for (it2 = it->key.set.getFirst(); it2 != it->key.set.getLast();
          it2 = it->key.set.getNext(it2)) {
      if (!owners->belongs(it2->key)) { owners->insert(it2->key); }
      if (owners->getNitems() == getNNodes()) { return true; }
    }
  }

  #ifdef TX_DEBUG
  printf("There are %d different values\n", owners->getNitems());
  #endif

  return  false;
}

void InbacData::addAllVotes1ToVotes0() {
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    addVote0(&(it->key.set), and1);
  }
  addVote0(server, val);
}

bool InbacData::checkHelpVotes() {
  return (collectionHelp->getNitems() == getNNodes());
}

void InbacData::decide(bool d) {

  if (!decided) {
    if (!d) { InbacData::nbTotalAbort++; }
    if (InbacData::nbTotalTx % 300 == 0) {
      printf("%d Consensus out of %d transactions, %d aborts, %d speed-up0, %d speed-up1\n",
        InbacData::nbTotalCons, InbacData::nbTotalTx, InbacData::nbTotalAbort, InbacData::nbSpeedUp0, InbacData::nbSpeedUp1);
    }
    decided = true;
    #ifdef TX_DEBUG
    printf("*** Decide %s Event - Inbac ID = %lu\n", d ? "true" : "false", inbacId);
    #endif
    crpcdata->data->commit = d ? 0 : 1;
    CommitRPCRespData *respCom = (CommitRPCRespData*) commitRpc(crpcdata);

    InbacRPCRespData *resp = new InbacRPCRespData;
    resp->data = new InbacRPCResp;
    resp->freedata = true;
    resp->data->status = respCom->data->status;
    resp->data->decision = crpcdata->data->commit;
    resp->data->committs = respCom->data->waitingts;
    rti->setResp(resp);
    TaskScheduler *ts = tgetTaskScheduler();
    ts->endTask(rti);

    // if (r2) { tryDelete(); }

  }

}

void* startInbac(void *arg_) {

  InbacDataParm *parm = (InbacDataParm*) arg_;
  InbacData *inbacData = new InbacData(parm);

  inbacData->propose(parm->vote);

  return NULL;
}
