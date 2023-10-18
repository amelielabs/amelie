#pragma once

//
// monotone
//
// SQL OLTP database
//

void dispatch_commit(Dispatch*, uint64_t);
void dispatch_abort(Dispatch*);
void dispatch_send(Dispatch*);
void dispatch_recv(Dispatch*, Portal*);
