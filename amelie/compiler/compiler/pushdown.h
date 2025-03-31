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

void pushdown(Compiler*, Ast*);
void pushdown_full(Compiler*, Ast*);
int  pushdown_recv(Compiler*, Ast*);
int  pushdown_recv_returning(Compiler*, bool);
