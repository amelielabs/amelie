#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

void parse(Parser*, Local*, Columns*, Str*);
void parse_stmt_free(Stmt*);
