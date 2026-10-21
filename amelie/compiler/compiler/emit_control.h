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

void emit_if(Compiler*, Stmt*);
void emit_for(Compiler*, Stmt*);
void emit_while(Compiler*, Stmt*);
void emit_break(Compiler*, Stmt*);
void emit_return(Compiler*, Stmt*);
