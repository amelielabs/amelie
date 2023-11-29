#pragma once

//
// monotone
//
// SQL OLTP database
//

void transaction_begin(Transaction*);
void transaction_commit(Transaction*);
void transaction_abort(Transaction*);
