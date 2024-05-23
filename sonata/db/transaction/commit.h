#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void transaction_begin(Transaction*);
void transaction_commit(Transaction*);
void transaction_abort(Transaction*);
