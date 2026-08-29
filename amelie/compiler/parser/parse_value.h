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

int  parse_vector(Stmt*, Buf*);
Ast* parse_value_const(Stmt*, Column*, Value*);
Ast* parse_value(Stmt*, From*, Column*, Value*);
void parse_value_data(Local*, Column*, Value*, uint8_t**);
void parse_value_string(Local*, Column*, Value*, Str*);

void parse_value_default(Column*, Value*);
void parse_value_validate(Stmt*, Column*, Value*, Ast*);
