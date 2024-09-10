#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

void tr_begin(Tr*);
void tr_commit(Tr*);
void tr_abort(Tr*);
