#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

void pushdown(Compiler*, Ast*);
void pushdown_first(Compiler*, Ast*);
int  pushdown_recv(Compiler*, Ast*);
int  pushdown_recv_returning(Compiler*, bool);
