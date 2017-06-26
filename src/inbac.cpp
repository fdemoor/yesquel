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
    if (pcd->data.type == 0) {
      printf("*** Deliver Event - Inbac Id = %d - %s\n", pcd->data.inbacId, "Helped");
      InbacData *inbacData = InbacData::getInbacData(pcd->data.inbacId);
      if (inbacData->getId() >= MAX_NB_CRASHED) {
        inbacData->addVote0(pcd->data.votes);
        inbacData->incrCntHelp();
        if ( (inbacData->getCnt() + inbacData->getCntHelp())
          >= (inbacData->getNNodes() - MAX_NB_CRASHED)
          && inbacData->waiting() ) { pcd->sem.signal(); }
      }
    }
  } else {
    pcd->data.type = -1;   // indicates an error
  }
  return; // free buffer
}


int inbacTimeoutHandler(void* arg) {
  InbacData *data = (InbacData*) arg;
  if (data) { data->timeoutEvent(); }
  return 0;
}

HashTable<int,InbacData>* InbacData::inbacDataObjects = new HashTable<int,InbacData>(100);

InbacData* InbacData::getInbacData(int key) {
  InbacData* data = inbacDataObjects->lookup(key);
  return data;
}

void InbacData::insertInbacData(InbacData *data) {
  inbacDataObjects->insert(data);
}

void InbacData::removeInbacData(InbacData *data) {
  inbacDataObjects->remove(data);
}

int InbacData::addVote0(VotePair vote) {
  VotePair *v = new VotePair;
  v->vote = vote.vote;
  v->owner = vote.owner;
  collection0->insert(*v);
  return collection0->getNitems();
}

int InbacData::addVote0(Set<VotePair> *votes) {
  SetNode<VotePair> *it;
  for (it = votes->getFirst(); it != votes->getLast(); it = votes->getNext(it)) {
    VotePair *v = new VotePair;
    v->vote = it->key.vote;
    v->owner = it->key.owner;
    collection0->insert(*v);
  }
  return collection0->getNitems();
}

void InbacData::addVote1(Set<VotePair> *set, IPPortServerno owner) {
  SetPair *pair = new SetPair;
  pair->set = *set;
  pair->owner = owner;
  collection1->insert(*pair);
}


InbacData::InbacData(InbacRPCParm *param, IPPort p, Ptr<RPCTcp> rpc, int k, CommitRPCData *commitData) {

  crpcdata = commitData;
  inbacId = k;
  Rpcc = rpc;
  serverset = new Set<IPPortServerno>;
  *serverset = *(param->serverset);
  SetNode<IPPortServerno> *it;
  int i = 0;
  for (it = serverset->getFirst(); it != serverset->getLast();
       it = serverset->getNext(it), i++) {
    if (IPPort::cmp(it->key.ipport, p) == 0) {
      id = i;
      server = it->key;
      break;
    }
  }

  r1 = true;

  printf("My INBAC id is %d (%u:%u)\n", id, server.ipport.ip, server.ipport.port);

  phase = 0;
  proposed = false;
  decided = false;
  collection0 = new Set<VotePair>;
  collection1 = new Set<SetPair>;
  collectionHelp = new Set<VotePair>;
  wait = false;
  cnt = 0;
  cntHelp = 0;

  insertInbacData(this);

}

void InbacData::propose(int vote) {

  printf("*** Propose %s Event - Inbac ID = %d\n",
    (vote == 0) ? "true" : "false", inbacId);

  // Set value
  val = (vote == 0) ? true : false;

  // Send value to backups
  SetNode<IPPortServerno> *it;
  int i = 0;
  for (it = serverset->getFirst(); i < MAX_NB_CRASHED && it != serverset->getLast();
       it = serverset->getNext(it)) {

    VotePair *vote = new VotePair;
    vote->owner = server;
    vote->vote = val;

    if (IPPortServerno::cmp(it->key, server) != 0) {

      InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
      rpcdata->data = new InbacMessageRPCParm;
      rpcdata->data->vote = *vote;
      rpcdata->data->type = 0;
      rpcdata->data->inbacId = inbacId;

      InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

      printf("Sending vote to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
      Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                      inbacmessagecallback, imcd);

      i++;
    } else {
      collection0->insert(*vote);
    }
  }

  // Set timer
  if (id <= MAX_NB_CRASHED) {
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (InbacData*) this, 0, MSG_DELAY);
  } else {
    TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (InbacData*) this, 0, 2 * MSG_DELAY);
    phase = 1;
  }
}

void InbacData::timeoutEvent() {
  if (phase == 0) {
    timeoutEvent0();
  } else if (phase == 1 && !decided && !proposed) {
    timeoutEvent1();
  }
}

void InbacData::timeoutEvent0() {

  printf("*** Timeout 0 Event - Inbac ID = %d\n", inbacId);

  SetNode<IPPortServerno> *it;
  int i = 0;

  InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
  rpcdata->data = new InbacMessageRPCParm;
  rpcdata->data->type = 1;
  rpcdata->data->votes = collection0;
  rpcdata->data->owner = server;
  rpcdata->data->inbacId = inbacId;
  InbacMessageCallbackData *imcd = new InbacMessageCallbackData;

  if (id < MAX_NB_CRASHED) {

    for (it = serverset->getFirst(); it != serverset->getLast();
         it = serverset->getNext(it)) {
      if (IPPortServerno::cmp(it->key, server) != 0) {

        printf("Sending backup votes to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
        Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                        inbacmessagecallback, imcd);

      } else {
        addVote1(collection0, server);
      }
    }

  } else if (id == MAX_NB_CRASHED) {

    for (it = serverset->getFirst(); i < MAX_NB_CRASHED && it != serverset->getLast();
         it = serverset->getNext(it)) {
      if (IPPortServerno::cmp(it->key, server) != 0) {

        printf("Sending backup votes to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
        Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                        inbacmessagecallback, imcd);
        i++;
      }
    }
  }

  phase = 1;
  TaskEventScheduler::AddEvent(tgetThreadNo(), inbacTimeoutHandler, (InbacData*) this, 0, MSG_DELAY);
}

void InbacData::timeoutEvent1() {

  if (r1) {

    printf("*** Timeout 1 Event - Inbac ID = %d\n", inbacId);

    phase = 2;
    if (id < MAX_NB_CRASHED) {

      if (checkBackupVotes1()) {
        decision = getAndVotes1();
        decide(decision);
      } else {
        BoolPair* check = checkAllExistVotes1();
        ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
        if (check->first) {
          proposal = check->second;
          proposed = true;
          printf("***Consensus1.1 propose %s\n", proposal ? "true" : "false");
          consData->propose(proposal);
        } else {
          proposed = true;
          printf("***Consensus1.2 propose false\n");
          consData->propose(false);
        }
      }

    } else {

      addAllVotes1ToVotes0();
      if (checkAllVotes1()) {
        decision = getAndVotes1();
        decide(decision);
      } else if (cnt >= 1) {
        BoolPair *check = checkAllExistVotes1();
        ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
        if (check->first) {
          proposal = check->second;
          proposed = true;
          consData->propose(proposal);
        } else {
          proposed = true;
          consData->propose(false);
        }
      } else {
        wait = true;
        SetNode<IPPortServerno> *it;
        int i = 0;
        InbacMessageCallbackData *imcd = new InbacMessageCallbackData;
        for (it = serverset->getFirst(); it != serverset->getLast();
             it = serverset->getNext(it), i++) {

          InbacMessageRPCData *rpcdata = new InbacMessageRPCData;
          rpcdata->data = new InbacMessageRPCParm;
          rpcdata->data->type = 2;
          rpcdata->data->inbacId = inbacId;

          if (i >= MAX_NB_CRASHED && IPPortServerno::cmp(it->key, server) != 0) {
            printf("Sending Help request to %u:%u\n", it->key.ipport.ip, it->key.ipport.port);
            Rpcc->asyncRPC(it->key.ipport, INBACMESSAGE_RPCNO, 0, rpcdata,
                            inbacmessagecallback, imcd);
          }
        }

        imcd->sem.wait(INFINITE);

        wait = false;
        if (checkAllVotes1()) {
          decision = getAndVotes1();
          decide(decision);
        } else if (cnt >= 1) {
          BoolPair *check = checkAllExistVotes1();
          ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
          if (check->first) {
            proposal = check->second;
            proposed = true;
            printf("***Consensus2.1 propose %s\n", proposal ? "true" : "false");
            consData->propose(proposal);
          } else {
            proposed = true;
            printf("***Consensus2.2 propose false\n");
            consData->propose(false);
          }
        } else {
          ConsensusData * consData = new ConsensusData(serverset, server, Rpcc, inbacId);
          if (checkHelpVotes()) {
            proposal = getAndHelpVotes();
            proposed = true;
            printf("***Consensus3.1 propose %s\n", proposal ? "true" : "false");
            consData->propose(proposal);
          } else {
            proposed = true;
            printf("***Consensus3.2 propose false\n");
            consData->propose(false);
          }
        }
      }

    }

  } else {
    r1 = true;
  }
}

bool InbacData::checkAllVotes1() {
  if (collection1->getNitems() != MAX_NB_CRASHED) { return false; }
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    int size = it->key.set.getNitems();
    if (size != getNNodes()) { return false; }
  }
  return true;
}

bool InbacData::checkBackupVotes1() {
  if (collection1->getNitems() != MAX_NB_CRASHED + 1) { return false; }
  SetNode<SetPair> *it;
  bool foundF = false;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    int size = it->key.set.getNitems();
    if (size == MAX_NB_CRASHED) {
      if (foundF) { return false; } else { foundF = true; }
    } else if (size != getNNodes()) { return false; }
  }
  return true;
}

bool InbacData::getAndVotes1() {
  SetNode<SetPair> *it = collection1->getFirst();
  while ( (it->key.set.getNitems() != getNNodes() + 1)
            && (it != collection1->getLast()) ) {
    it = collection1->getNext(it);
  }
  SetNode<VotePair> *it2;
  for (it2 = it->key.set.getFirst(); it2 != it->key.set.getLast();
        it2 = it->key.set.getNext(it2)) {
    if (!it2->key.vote) { return false; }
  }
  return true;
}

BoolPair* InbacData::checkAllExistVotes1() {
  BoolPair *result = new BoolPair;
  Set<VotePair> *values = new Set<VotePair>;
  bool and1 = true;
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    SetNode<VotePair> *it2;
    for (it2 = it->key.set.getFirst(); it2 != it->key.set.getLast();
          it2 = it->key.set.getNext(it2)) {
      values->insert(it2->key);
      if (!it2->key.vote) { and1 = false; }
    }
  }
  result->first = (values->getNitems() == getNNodes());
  result->second = and1;
  return result;
}

void InbacData::addAllVotes1ToVotes0() {
  SetNode<SetPair> *it;
  for (it = collection1->getFirst(); it != collection1->getLast();
        it = collection1->getNext(it)) {
    addVote0(&(it->key.set));
  }
  VotePair *myVote = new VotePair;
  myVote->owner = server;
  myVote->vote = val;
  addVote0(*myVote);
}

bool InbacData::checkHelpVotes() {
  return (collectionHelp->getNitems() == getNNodes());
}

bool InbacData::getAndHelpVotes() {
  SetNode<VotePair> *it;
  for (it = collectionHelp->getFirst(); it != collectionHelp->getLast();
        it = collectionHelp->getNext(it)) {
    if (!it->key.vote) { return false; }
  }
  return true;
}

void InbacData::decide(bool d) {
  if (!decided) {
    decided = true;
    printf("*** Decide %s Event - Inbac ID = %d\n", d ? "true" : "false", inbacId);
    crpcdata->data->commit = d ? 0 : 1;
    CommitRPCRespData *respCom = (CommitRPCRespData*) commitRpc(crpcdata);
    removeInbacData(this);
    delete this;
  }
}

void* startInbac(void *arg_) {
  InbacDataParm *parm = (InbacDataParm*) arg_;
  InbacData *inbacData = new InbacData(parm->parm, parm->ipport, parm->rpc, parm->k, parm->commitData);
  inbacData->propose(parm->vote);
  return NULL;
}
