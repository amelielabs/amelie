
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_rpc_insert(Parser*  self, Program* program,
                 Str*     rel_user,
                 Str*     rel,
                 uint8_t* values)
{
	self->program = program;
	(void)rel_user;
	(void)rel;
	(void)values;
}
