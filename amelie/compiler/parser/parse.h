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

void parse_stmt_free(Stmt*);
void parse_stmt(Stmt*);
void parse_block(Parser*, Block*);
void parse(Parser*, Program*, Str*);
