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

void parse_row_list(Stmt*, Table*, Set*, Ast*);
void parse_row(Stmt*, Table*, Set*);
void parse_row_generate(Stmt*, Table*, Set*, int);
