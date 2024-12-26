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

void parse_vector(Lex*, Buf*);
void parse_value(Lex*, Local*, Json*, Column*, Value*);
void parse_value_default(Column*, Value*, uint64_t);
void parse_value_validate(Column*, Value*);
