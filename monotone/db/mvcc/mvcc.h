#pragma once

//
// monotone
//
// SQL OLTP database
//

void mvcc_begin(Transaction*);
void mvcc_commit(Transaction*);
void mvcc_abort(Transaction*);
