
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
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

static void
import_row(Parser* self, Columns* columns, Set* values, Csv* csv)
{
	// value, ...
	auto row = set_reserve(values);

	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// handle dropped columns as NULL values
		if (column->dropped)
		{
			value_set_null(column_value);
			continue;
		}

		// read csv value
		Str value;
		str_init(&value);
		switch (csv_next(csv, &value)) {
		case CSV_NULL:
			value_set_null(column_value);
			break;
		case CSV_VALUE:
			// parse column value
			parse_value_string(self->local, column, column_value, &value);
			break;
		case CSV_ERROR:
			error("csv read error");
			break;
		default:
			// eof, eol
			error("csv row is incomplete");
			break;
		}
		parse_value_validate(NULL, column, column_value, NULL);
	}

	// eol or eof
	Str value;
	str_init(&value);
	auto rc = csv_next(csv, &value);
	if (rc == CSV_ERROR)
		error("csv read error");
	if (rc != CSV_EOL && rc != CSV_EOF)
		error("csv row columns mismatch");
}

static void
import_insert(Parser* self, Table* table, Clone* clone, Str* content)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare insert stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_INSERT;
	stmt->ast = &ast_insert_allocate(block)->ast;
	stmt->is_return = true;

	// create insert target
	auto insert  = ast_insert_of(stmt->ast);
	stmt->ret = &insert->ret;

	// set timeline
	Timeline* timeline;
	if (clone)
		timeline = &clone->config->timeline;
	else
		timeline = table_main(table);

	auto columns = table_columns(table);
	auto target  = target_allocate();
	target->type          = TARGET_TABLE;
	target->ast           = stmt->ast;
	target->from_lock     = LOCK_SHARED_RW;
	target->from_table    = table;
	target->from_timeline = timeline;
	target->columns       = columns;
	str_set_str(&target->name, &table->config->name);
	from_add(&insert->from, target);

	// add table/clone to the access list
	if (clone)
	{
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_SELECT);
		access_add(&self->program->access, &clone->rel, LOCK_NONE, PERM_INSERT);
	} else {
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_INSERT);
	}

	// prepare result set
	insert->values = set_cache_create(self->set_cache, &self->program->sets);
	set_prepare(insert->values, columns->count, 0, NULL);

	// parse and set values

	// treat each csv row as insert row
	//
	// value, value, ...\r\n
	// ...
	//
	Csv csv;
	csv_init(&csv);
	defer(csv_free, &csv);
	csv_set(&csv, content);
	if (unlikely(csv_eof(&csv)))
		error("content is empty");
	while (! csv_eof(&csv))
		import_row(self, columns, insert->values, &csv);
}

void
parse_import(Parser* self, Program* program,
             Str*    rel_user,
             Str*    rel,
             Str*    content)
{
	Str* user = rel_user;
	if (str_empty(rel_user))
		user = &self->local->user;
	self->program = program;

	auto ref = catalog_find(&share()->db->catalog, REL_UNDEF, user, rel, true);
	switch (ref->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref);
		import_insert(self, table, NULL, content);
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(ref);
		import_insert(self, clone->table, clone, content);
		break;
	}
	default:
	{
		error("relation '{str}': unsupported relation", rel);
		break;
	}
	}
}
