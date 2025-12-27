
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

typedef struct ParseEndpoint ParseEndpoint;

enum
{
	ENDPOINT_TABLE,
	ENDPOINT_UDF
};

struct ParseEndpoint
{
	Endpoint* endpoint;
	Ast*      columns;
	Ast*      columns_tail;
	int       columns_count;
	bool      columns_has;
	Columns*  columns_target;
	Str*      content;
	int       target_type;
	Relation* target;
	Set*      values;
};

static inline void
parse_endpoint_init(ParseEndpoint* self, Endpoint* endpoint, Str* content)
{
	self->endpoint       = endpoint;
	self->columns        = NULL;
	self->columns_tail   = NULL;
	self->columns_count  = 0;
	self->columns_has    = false;
	self->columns_target = NULL;
	self->content        = content;
	self->target_type    = -1;
	self->target         = NULL;
	self->values         = NULL;
}

static inline bool
parse_endpoint_column(char** pos, char* end, Str* name)
{
	// name
	// name [,]
	auto start = *pos;
	while (*pos < end && **pos != ',')
		(*pos)++;
	str_set(name, start, *pos - start);
	if (*pos != end)
		(*pos)++;
	return !str_empty(name);
}

static inline void
parse_endpoint_set_columns(ParseEndpoint* self, Str* spec)
{
	if (str_empty(spec))
		return;
	self->columns_has = true;

	// name[, ...]
	auto pos = spec->pos;
	auto end = spec->end;

	Str name;
	while (parse_endpoint_column(&pos, end, &name))
	{
		auto column = columns_find(self->columns_target, &name);
		if (! column)
			error("column '%.*s' does not exists", str_size(&name),
			      str_of(&name));

		auto last = self->columns_tail;
		if (last && column->order <= last->column->order)
			error("column list must be ordered");

		auto ref = ast(0);
		ref->column = column;
		if (self->columns == NULL)
			self->columns = ref;
		else
			last->next = ref;
		self->columns_tail = ref;
		self->columns_count++;
	}
}

static inline void
parse_endpoint_set(ParseEndpoint* self)
{
	auto endpoint = self->endpoint;

	// find relation
	auto db       = &endpoint->db.string;
	auto relation = &endpoint->relation.string;
	auto table    = table_mgr_find(&share()->storage->catalog.table_mgr,
	                               db, relation, false);
	if (table)
	{
		self->target_type    = ENDPOINT_TABLE;
		self->target         = &table->rel;
		self->columns_target = &table->config->columns;
	} else
	{
		auto udf = udf_mgr_find(&share()->storage->catalog.udf_mgr,
		                        db, relation, false);
		if (! udf)
			error("relation '%.*s': not found", str_size(relation),
			      str_of(relation));
		self->target_type    = ENDPOINT_UDF;
		self->target         = &udf->rel;
		self->columns_target = &udf->config->args;
	}

	// set column list
	parse_endpoint_set_columns(self, opt_string_of(&endpoint->columns));
}

hot static inline void
import_row(Stmt* self, ParseEndpoint* pe)
{
	// prepare row
	auto row = set_reserve(pe->values);

	auto list = pe->columns;
	list_foreach(&pe->columns_target->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];
		auto column_separator = true;

		Ast* value = NULL;
		if (pe->columns_has)
		{
			if (list && list->column == column)
			{
				// parse column value
				value = parse_value(self, NULL, column, column_value);
				list  = list->next;
				column_separator = list != NULL;
			} else
			{
				// default value, write IDENTITY, RANDOM or DEFAULT
				parse_value_default(self, column, column_value);
				column_separator = false;
			}
		} else
		{
			// parse column value
			value = parse_value(self, NULL, column, column_value);
			column_separator = !list_is_last(&pe->columns_target->list, &column->link);
		}

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, value);

		// ,
		if (column_separator)
			stmt_expect(self, ',');
	}
}

hot static inline void
import_obj(Stmt* self, ParseEndpoint* pe)
{
	// prepare row
	auto columns = pe->columns_target;
	auto row = set_reserve(pe->values);

	auto buf = buf_create();
	defer_buf(buf);
	buf_reserve(buf, sizeof(bool) * columns->count);
	memset(buf->start, 0, sizeof(bool) * columns->count);

	auto match = (bool*)buf->start;
	auto match_count = 0;
	for (;;)
	{
		// "key"
		auto key = stmt_if(self, KSTRING);
		if (! key)
			break;

		// :
		stmt_expect(self, ':');

		// match column
		auto column = columns_find(columns, &key->string);
		if (! column)
			stmt_error(self, key, "column does not exists");

		// ensure column is not redefined
		if (unlikely(match[column->order]))
			stmt_error(self, key, "column value is redefined");
		match[column->order] = true;
		match_count++;

		// parse column value
		auto column_value = &row[column->order];
		auto value = parse_value(self, NULL, column, column_value);

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}
	if (match_count == columns->count)
		return;

	// default value, write IDENTITY, RANDOM or DEFAULT
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (match[column->order])
			continue;

		auto column_value = &row[column->order];
		parse_value_default(self, column, column_value);

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, NULL);
	}
}

hot static inline void
import_json_row(Stmt* self, ParseEndpoint* pe)
{
	// {} or []
	auto begin = stmt_if(self, '{');
	if (begin)
	{
		if (pe->columns_has)
		{
			if (unlikely(pe->columns_count > 1))
				stmt_error(self, begin, "JSON column list must have zero or one value");

			// process {} as a value for column, if one column
			if (pe->columns_count == 1)
				stmt_push(self, begin);

			import_row(self, pe);
		} else {
			import_obj(self, pe);
		}
		if (! pe->columns_count)
			stmt_expect(self, '}');
		return;
	}

	if (! stmt_if(self, '['))
		stmt_error(self, NULL, "json object or array expected");
	import_row(self, pe);
	stmt_expect(self, ']');
}

hot static inline void
import_jsonl(Stmt* self, ParseEndpoint* pe)
{
	for (;;)
	{
		// {} or []
		import_json_row(self, pe);
		if (stmt_if(self, KEOF))
			break;
	}
}

hot static inline void
import_json(Stmt* self, ParseEndpoint* pe)
{
	// [ {} or [], ...]
	stmt_expect(self, '[');
	for (;;)
	{
		// {} or []
		import_json_row(self, pe);
		// ,]
		if (stmt_if(self, ','))
			continue;
		stmt_expect(self, ']');
		break;
	}
}

hot static inline void
import_csv(Stmt* self, ParseEndpoint* pe)
{
	// ensure column list is not empty for CSV import
	if (pe->columns_has &&
	    pe->columns_count == 0)
		error("CSV import with empty column list is not supported");

	// value, ...
	for (;;)
	{
		import_row(self, pe);
		if (stmt_if(self, KEOF))
			break;
	}
}

static void
import_insert(Parser* self, ParseEndpoint* pe)
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

	auto table   = (Table*)pe->target;
	auto columns = table_columns(table);
	auto target  = target_allocate();
	target->type        = TARGET_TABLE;
	target->ast         = stmt->ast;
	target->from_access = ACCESS_RW;
	target->from_table  = table;
	target->columns     = columns;
	str_set_str(&target->name, &table->config->name);
	from_add(&insert->from, target);
	access_add(&self->program->access, &table->rel, ACCESS_RW);

	// prepare result set
	insert->values = set_cache_create(self->set_cache, &self->program->sets);
	pe->values = insert->values;
	set_prepare(insert->values, columns->count, 0, NULL);

	// parse rows according to the content type
	auto content_type = &pe->endpoint->content_type.string;
	if (str_is(content_type, "text/csv", 8))
		import_csv(stmt, pe);
	else
	if (str_is(content_type, "application/jsonl", 17))
		import_jsonl(stmt, pe);
	else
	if (str_is(content_type, "application/json", 16))
		import_json(stmt, pe);
	else
		stmt_error(stmt, NULL, "unsupported operation content type");

	// ensure there are no data left
	if (! stmt_if(stmt, KEOF))
		stmt_error(stmt, NULL, "eof expected");

	// create a list of generated columns expressions
	if (columns->count_stored > 0)
		parse_generated(stmt);
	
	// if table has resolved columns, handle insert as upsert
	// and apply the resolve expressions on conflicts
	if (columns->count_resolved > 0)
		parse_resolved(stmt);
}

static void
import_execute(Parser* self, ParseEndpoint* pe)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare execute stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_EXECUTE;
	stmt->ast = &ast_execute_allocate()->ast;
	stmt->is_return = true;

	auto execute = ast_execute_of(stmt->ast);
	stmt->ret = &execute->ret;

	// prepare arguments
	auto udf = (Udf*)pe->target;
	execute->udf  = udf;
	execute->args = set_cache_create(self->set_cache, &self->program->sets);
	pe->values = execute->args;
	set_prepare(execute->args, udf->config->args.count, 0, NULL);

	// parse rows according to the content type
	auto content_type = &pe->endpoint->content_type.string;
	if (str_is(content_type, "text/csv", 8))
		import_csv(stmt, pe);
	else
	if (str_is(content_type, "application/jsonl", 17) ||
	    str_is(content_type, "application/json", 16))
		import_json_row(stmt, pe);
	else
		stmt_error(stmt, NULL, "unsupported operation content type");

	// ensure there are no data left
	if (! stmt_if(stmt, KEOF))
		stmt_error(stmt, NULL, "eof expected");

	// set returning column
	if (udf->config->type != TYPE_NULL)
	{
		auto column = column_allocate();
		column_set_name(column, &udf->config->name);
		column_set_type(column, udf->config->type, -1);
		columns_add(&execute->ret.columns, column);
	}
}

void
parse_endpoint(Parser* self, Program* program, Endpoint* endpoint, Str* content)
{
	self->program = program;
	lex_start(&self->lex, content);

	// find relation, set columns
	ParseEndpoint pe;
	parse_endpoint_init(&pe, endpoint, content);
	parse_endpoint_set(&pe);
	switch (pe.target_type) {
	case ENDPOINT_TABLE:
		import_insert(self, &pe);
		break;
	case ENDPOINT_UDF:
		import_execute(self, &pe);
		break;
	default:
		abort();
	}
}
