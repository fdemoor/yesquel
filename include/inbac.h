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

#include <utility>

#include "ipmisc.h"
#include "datastruct.h"
#include "options.h"

using namespace std;

class VoteCollRPCData : public Marshallable {
public:
  Set<pair<IPPortServerno,bool>> *data;
  int freedata;
  VoteCollRPCData()  { freedata = 0; data = new Set<pair<IPPortServerno,bool>>; }
  ~VoteCollRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

int inbacTimeoutHandler(void* arg);

class InbacData {

private:

  int id;
  int phase;
  bool proposed;
  bool decided;
  VoteCollRPCData *collections0;
  Set<pair<IPPortServerno,Set<pair<IPPortServerno,bool>>>> *collections1;
  VoteCollRPCData *collectionHelp;
  bool wait;
  bool val;
  bool decision;
  bool proposal;
  int cnt;
  int cntHelp;

public:

  InbacData(int serverId, int vote);
  int getPhase() { return phase; }
  bool hasProposed() { return proposed; }
  bool hasDecided() { return decided; }
  int getId() { return id; }

};

#endif
