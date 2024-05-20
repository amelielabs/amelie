#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

void transaction_begin(Transaction*);
void transaction_commit(Transaction*);
void transaction_abort(Transaction*);
