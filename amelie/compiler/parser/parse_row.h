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

void parse_row_generate(Stmt*, Table*, Set*, int);
void parse_rows(Stmt*, Table*, Set*, Ast*, bool);
void parse_row_generate_expr(Stmt*, Table*, AstList*, int);
void parse_rows_expr(Stmt*, Table*, AstList*, Ast*, bool);
