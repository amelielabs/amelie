#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

void tr_begin(Tr*);
void tr_commit(Tr*);
void tr_commit_list(TrList*, TrCache*, uint64_t);
void tr_abort(Tr*);
void tr_abort_list(TrList*, TrCache*);
