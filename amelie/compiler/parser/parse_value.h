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

void parse_vector(Stmt*, Buf*);
Ast* parse_value(Stmt*, From*, Column*, Value*);
void parse_value_default(Stmt*, Column*, Value*);
void parse_value_validate(Stmt*, Column*, Value*, Ast*);
