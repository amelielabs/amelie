#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

int emit_string(Compiler*, Str*, bool);
int emit_expr(Compiler*, Target*, Ast*);
