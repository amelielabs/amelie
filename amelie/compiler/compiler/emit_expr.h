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

int emit_string(Compiler*, Str*, bool);
int emit_call(Compiler*, Target*, Ast*);
int emit_expr(Compiler*, Target*, Ast*);
