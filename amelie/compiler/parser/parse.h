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
void parse_sql(Parser*, Local*, Columns*, Str*);
void parse_csv(Parser*, Local*, Columns*, Str*, Str*);
