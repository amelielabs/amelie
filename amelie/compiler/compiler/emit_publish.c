
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

hot int
emit_publish(Compiler* self, Ast* ast)
{
	auto publish = ast_publish_of(ast);

	// push references (expressions used by the publish values)
	auto refs_count = emit_refs(self, &self->current->refs);

	intptr_t values = -1;
	if (publish->values)
		values = (intptr_t)publish->values;

	// rel
	auto data = &self->code_data->data;
	auto offset = buf_size(data);
	encode_str(data, &publish->db);
	encode_str(data, &publish->rel);

	// CPUBLISH
	op3(self, CPUBLISH, offset, values, refs_count);
	return -1;
}
