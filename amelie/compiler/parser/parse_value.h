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

int parse_value(Lex*, Local*, Json*, Column*, Value*);
int parse_value_default(Column*, Value*, uint64_t);
