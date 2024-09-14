#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

void tr_begin(Tr*);
void tr_commit(Tr*);
void tr_commit_list(TrList*, TrCache*, Tr*);
void tr_abort(Tr*);
void tr_abort_list(TrList*, TrCache*);
