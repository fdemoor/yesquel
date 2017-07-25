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
  if (data) {
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
    printf("Timeout Event - Inbac Id = %lu\n", timeoutData->data->GetKey());
    #endif

    timeoutData->data->timeoutEvent(timeoutData->type);
  }
  return 0;
}

HashTable<u64,InbacData>* InbacData::inbacDataObjects = new HashTable<u64,InbacData>(10000);
LinkList<InbacMessageRPCParm>* InbacData::msgQueue = new LinkList<InbacMessageRPCParm>(true);

#ifdef TX_DEBUG_2
int InbacData::nbTotalTx = 0;
int InbacData::nbTotalCons = 0;
int InbacData::nbTotalAbort = 0;
int InbacData::nbSpeedUp0 = 0;
int InbacData::nbSpeedUp1 = 0;
#endif

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

void InbacData::deliver0(int owner, bool vote) {
  if (phase == 0) {
    int k = addVote0(owner, vote);
    if ( (id < maxNbCrashed && k == NNodes) ||
          (id == maxNbCrashed && k == maxNbCrashed) ) {

      #ifdef TX_DEBUG_2
      InbacData::nbSpeedUp0++;
      #endif

      d0 = false;
      InbacTimeoutData *timeoutData = new InbacTimeoutData;
      timeoutData->data = this;
      timeoutData->type = 0;
      inbacTimeoutHandler((void*) timeoutData);
    }
  }
}

void InbacData::deliver1(bool *owners, bool vote, int owner, bool all) {
  addVote1(owners, vote, owner);
  cnt++;
  all1 = all1 && all;
  if (cnt == maxNbCrashed) {

    #ifdef TX_DEBUG_2
    InbacData::nbSpeedUp1++;
    #endif

    d1 = false;
    InbacTimeoutData *timeoutData = new InbacTimeoutData;
    timeoutData->data = this;
    timeoutData->type = 1;
    inbacTimeoutHandler((void*) timeoutData);
  }
}

int InbacData::addVote0(int owner, bool vote) {
  if (!collection0[owner]) {
    collection0[owner] = true;
    size0++;
  }
  and0 = and0 && vote;
  return size0;
}

int InbacData::addVote0(bool *owners, bool vote) {
  for (int i = 0; i < NNodes; i++) {
    if (owners[i]) {
      if (!collection0[i]) {
        collection0[i] = true;
        size0++;
      }
    }
  }
  and0 = and0 && vote;
  return size0;
}

void InbacData::addVote1(bool *owners, bool vote, int owner) {
  for (int i = 0; i < NNodes; i++) {
    if (owners[i]) {
      collection1[i + NNodes * owner] = true;
    }
  }
  size1++;
  and1 = and1 && vote;
}

int InbacData::addVoteHelp(bool *owners, bool vote) {
  for (int i = 0; i < NNodes; i++) {
    if (owners[i]) {
      if (!collectionHelp[i]) {
        collectionHelp[i] = true;
        sizeHelp++;
      }
    }
  }
  andHelp = andHelp && vote;
  return sizeHelp;
}


InbacData::InbacData(InbacDataParm *parm) {

  rti = parm->rti;
  crpcdata = parm->commitData;
  inbacId = parm->k;
  Rpcc = parm->rpc;
  serverset = new Set<IPPortServerno>;
  *serverset = *(parm->parm->serverset);
  NNodes = serverset->getNitems();
  maxNbCrashed = (MAX_NB_CRASHED < NNodes) ? MAX_NB_CRASHED : NNodes - 1;
  server = parm->parm->owner;
  id = parm->parm->rank;

  t0 = true;
  t1 = true;
  d0 = true;
  d1 = true;

  #ifdef TX_DEBUG
  printf("My INBAC id is %d (%u:%u)\n", id, server.ipport.ip, server.ipport.port);
  #endif

  phase = 0;
  proposed = false;
  decided = false;
  collection0 = new bool[NNodes];
  for (int i = 0; i < NNodes; i++) { collection0[i] = false; }
  size0 = 0;
  and0 = true;
  collection1 = new bool[NNodes * NNodes];
  for (int i = 0; i < NNodes * NNodes; i++) { collection1[i] = false; }
  size1 = 0;
  and1 = true;
  all1 = true;
  collectionHelp = new bool[NNodes];
  for (int i = 0; i < NNodes; i++) { collectionHelp[i] = false; }
  sizeHelp = 0;
  andHelp = true;
  wait = false;
  cnt = 0;
  cntHelp = 0;

  insertInbacData(this);

}

InbacData::~InbacData() {
  delete serverset;
  delete[] collection0;
  delete[] collection1;
  delete[] collectionHelp;
}

void InbacData::propose(int vote) {

  #ifdef TX_DEBUG_2
  InbacData::nbTotalTx++;
  #endif

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
      rpcdata->data->owner = id;
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
      deliver0(id, val);
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
          #ifdef TX_DEBUG
          printf("*** Found msg in queue - Inbac Id = %lu - %s\n",
              msg->inbacId, InbacData::toString(msg->owner, msg->vote));
          #endif
          deliver0(msg->owner, msg->vote);
          break;
        } case 1: {
          #ifdef TX_DEBUG
          printf("*** Found msg in queue - Inbac Id = %lu - %s\n",
              msg->inbacId, InbacData::toString(msg->owners, msg->size, msg->vote));
          #endif
          deliver1(msg->owners, msg->vote, msg->owner, msg->all);
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
  if (d0 && d1 && decided) {
    removeInbacData(this);
    delete this;
  }
}

void InbacData::timeoutEvent(int type) {
  if (type == 0) {
    if (t0) { timeoutEvent0();}
    else { d0 = true; }

  } else if (type == 1) {
    if (t1) { timeoutEvent1();}
    else { d1 = true; tryDelete(); }
  }
}

void InbacData::timeoutEvent0() {

  if (phase == 0) {

    t0 = false;

    #ifdef TX_DEBUG
    printf("*** Timeout 0 Event - Inbac ID = %lu\n", inbacId);
    printf("Inbac is %p\n", this);
    #endif

    SetNode<IPPortServerno> *it;
    int i = 0;

    if (id < maxNbCrashed) {

      bool all = (size0 == NNodes);

      for (it = serverset->getFirst(); it != serverset->getLast();
           it = serverset->getNext(it)) {
        if (IPPortServerno::cmp(it->key, server) != 0) {

          InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
          rpcdata->data = new InbacMessageRPCParm;
          rpcdata->data->type = 1;
          rpcdata->data->owners = collection0;
          rpcdata->data->size = NNodes;
          rpcdata->data->owner = id;
          rpcdata->data->all = all;
          rpcdata->data->vote = and0;
          rpcdata->data->inbacId = inbacId;
          InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

          #ifdef TX_DEBUG
          printf("Sending backup votes to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
          #endif

          Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                          inbacmessagecallback, imcd);

        } else {
          deliver1(collection0, and1, id, all);
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
          rpcdata->data->size = NNodes;
          rpcdata->data->all = (size0 == maxNbCrashed) ? true : false;
          rpcdata->data->vote = and0;
          rpcdata->data->owner = id;
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

    t1 = false;
    phase = 2;
    if (id < maxNbCrashed) {

      if (cnt == (maxNbCrashed + 1) && all1) {
        decision = and1;
        decide(decision);
      } else {
        consensusRescue1();
      }

    } else {

      addAllVotes1ToVotes0();
      if (cnt == maxNbCrashed && all1) {
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
  if ( (cnt + cntHelp >= NNodes - maxNbCrashed) && wait ) {
    wait = false;
    if (cnt == maxNbCrashed && all1) {
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

bool InbacData::checkAllExistVotes1() {
  addAllVotes1ToVotes0();
  return (size0 == NNodes);
}

void InbacData::addAllVotes1ToVotes0() {
  for (int i = 0; i < NNodes; i++) {
    for (int j = 0; j < NNodes; j++) {
      if (collection1[j + i * NNodes]) {
        addVote0(j, and1);
      }
    }
  }
  addVote0(id, val);
}

bool InbacData::checkHelpVotes() {
  return (sizeHelp == NNodes);
}

void InbacData::decide(bool d) {

  if (!decided) {

    #ifdef TX_DEBUG_2
    if (!d) { InbacData::nbTotalAbort++; }
    if (InbacData::nbTotalTx % 1000 == 0) {
      printf("%d Consensus out of %d transactions, %d aborts, %d speed-up0, %d speed-up1\n",
        InbacData::nbTotalCons, InbacData::nbTotalTx, InbacData::nbTotalAbort, InbacData::nbSpeedUp0, InbacData::nbSpeedUp1);
    }
    #endif

    #ifdef TX_DEBUG
    printf("*** Decide %s Event - Inbac ID = %lu\n", d ? "true" : "false", inbacId);
    #endif

    decided = true;
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

    tryDelete();

  }

}

void startInbac(void *arg) {

  InbacDataParm *parm = (InbacDataParm*) arg;
  InbacData *inbacData = new InbacData(parm);

  inbacData->propose(parm->vote);

}
