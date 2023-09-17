#pragma once

//
// monotone
//
// SQL OLTP database
//

void mvcc_begin(Transaction*);
void mvcc_commit(Transaction*);
void mvcc_abort(Transaction*);
bool mvcc_write(Transaction*, LogCmd, Heap*, Locker*, Row*);
void mvcc_write_handle(Transaction*, LogCmd,
                       HandleMgr*,
                       Handle*,
                       Buf*);
