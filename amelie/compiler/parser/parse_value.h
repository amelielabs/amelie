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

void parse_vector(Scope*, Buf*);
Ast* parse_value(Scope*, Column*, Value*);
void parse_value_default(Scope*, Column*, Value*, uint64_t);
void parse_value_validate(Scope*, Column*, Value*, Ast*);
