
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

bool
parse_type(Scope* self, int* type, int* type_size)
{
	auto ast = scope_next_shadow(self);
	if (ast->id != KNAME)
		scope_error(self, ast, "unrecognized data type");

	*type = type_read(&ast->string, type_size);
	if (*type == -1)
		scope_error(self, ast, "unrecognized data type");

	return str_is_case(&ast->string, "serial", 6);
}

void
parse_key(Scope* self, Keys* keys)
{
	// (
	scope_expect(self, '(');
	for (;;)
	{
		// (column, ...)
		auto name = scope_expect(self, KNAME);

		// find column
		auto column = columns_find(keys->columns, &name->string);
		if (! column)
			scope_error(self, name, "column does not exists");

		// validate key type
		if ((column->type != TYPE_INT    &&
		     column->type != TYPE_STRING &&
		     column->type != TYPE_UUID   &&
		     column->type != TYPE_TIMESTAMP) ||
		    (column->type == TYPE_INT && column->type_size < 4))
			scope_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

		// force column not_null constraint
		constraints_set_not_null(&column->constraints, true);

		// create key
		auto key = key_allocate();
		key_set_ref(key, column->order);
		keys_add(keys, key);

		// ,
		if (! scope_if(self, ','))
			break;
	}

	// )
	scope_expect(self, ')');
}

static inline bool
parse_primary_key(Scope* self)
{
	// PRIMARY KEY
	if (! scope_if(self, KPRIMARY))
		return false;
	scope_expect(self, KKEY);
	return true;
}

static void
parse_default(Scope* self, Column* column, Buf* value)
{
	buf_reset(value);
	auto expr = parse_expr(self, NULL);
	if (expr->id == KNULL)
	{
		encode_null(value);
		return;
	}
	bool type_match = false;
	switch (column->type) {
	case TYPE_BOOL:
		if (expr->id != KTRUE && expr->id != KFALSE)
			break;
		encode_bool(value, expr->id == KTRUE);
		type_match = true;
		break;
	case TYPE_INT:
		if (expr->id != KINT)
			break;
		encode_integer(value, expr->integer);
		type_match = true;
		break;
	case TYPE_DOUBLE:
		if (expr->id != KINT && expr->id != KREAL)
			break;
		if (expr->id == KINT)
			encode_integer(value, expr->integer);
		else
			encode_real(value, expr->real);
		type_match = true;
		break;
	case TYPE_STRING:
		if (expr->id != KSTRING)
			break;
		encode_string(value, &expr->string);
		type_match = true;
		break;
	case TYPE_JSON:
		ast_encode(expr, self->lex, self->parser->local, value);
		type_match = true;
		break;
	case TYPE_TIMESTAMP:
	case TYPE_INTERVAL:
	case TYPE_DATE:
	case TYPE_VECTOR:
	case TYPE_UUID:
		scope_error(self, expr, "DEFAULT for this column type is not supported");
		break;
	}
	if (! type_match)
		scope_error(self, expr, "DEFAULT value must be a const and match the column type");
}

static void
parse_constraints(Scope* self, Keys* keys, Column* column)
{
	// constraints
	auto cons = &column->constraints;

	bool primary_key = false;
	bool has_default = false;
	bool done = false;
	while (! done)
	{
		auto name = scope_next(self);
		switch (name->id) {
		// NOT NULL
		case KNOT:
		{
			// NULL
			scope_expect(self, KNULL);
			constraints_set_not_null(cons, true);
			break;
		}

		// DEFAULT expr
		case KDEFAULT:
		{
			parse_default(self, column, &cons->value);
			has_default = true;
			break;
		}

		// RANDOM
		case KRANDOM:
		{
			// ensure the column has type INT
			if (column->type != TYPE_INT || column->type_size < 4)
				scope_error(self, name, "RANDOM column must be int or int64");

			constraints_set_random(cons, true);

			// [(modulo)]
			if (scope_if(self, '('))
			{
				// int
				auto value = scope_expect(self, KINT);
				// )
				scope_expect(self, ')');
				if (value->integer == 0)
					scope_error(self, value, "RANDOM modulo value cannot be zero");
				constraints_set_random_modulo(cons, value->integer);
			}

			if (cons->as_identity)
				scope_error(self, name, "cannot be used with identity column");
			break;
		}

		// PRIMARY KEY
		case KPRIMARY:
		{
			scope_expect(self, KKEY);

			if (primary_key)
				scope_error(self, name, "PRIMARY KEY defined twice");

			if (! keys)
				scope_error(self, name, "PRIMARY KEY clause is not supported in this command");

			// force not_null constraint for keys
			constraints_set_not_null(&column->constraints, true);

			// validate key type
			if ((column->type != TYPE_INT    &&
			     column->type != TYPE_STRING &&
			     column->type != TYPE_UUID   &&
			     column->type != TYPE_TIMESTAMP) ||
			    (column->type == TYPE_INT && column->type_size < 4))
				scope_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

			// create key
			auto key = key_allocate();
			key_set_ref(key, column->order);
			keys_add(keys, key);

			primary_key = true;

			if (! str_empty(&cons->as_resolved))
				scope_error(self, name, "cannot be used together with RESOLVED");
			break;
		}

		// [GENERATD ALWAYS] AS ...
		case KGENERATED:
		{
			// ALWAYS
			scope_expect(self, KALWAYS);

			// AS
			scope_expect(self, KAS);

			// fallthrough
		}

		// AS IDENTITY
		// AS (expr) STORED | RESOLVED
		case KAS:
		{
			auto identity = scope_if(self, KIDENTITY);
			if (identity)
			{
				// ensure the column has type INT64
				if (column->type != TYPE_INT || column->type_size < 4)
					scope_error(self, identity, "identity column must be int or int64");

				constraints_set_as_identity(cons, true);

				if (cons->random)
					scope_error(self, identity, "cannot be used with RANDOM");
				break;
			}

			// (
			auto lbr = scope_expect(self, '(');
			// expr
			parse_expr(self, NULL);
			// )
			auto rbr = scope_expect(self, ')');

			Str as;
			str_set(&as, self->lex->start + lbr->pos_start, rbr->pos_end - lbr->pos_start - 1);
			str_shrink(&as);
			as.pos++;
			str_shrink(&as);
			if (str_empty(&as))
				scope_error(self, lbr, "AS expression is missing");

			// STORED | RESOLVED
			auto type = scope_next(self);
			if (type->id == KSTORED) {
				constraints_set_as_stored(cons, &as);
			} else
			if (type->id == KRESOLVED)
			{
				constraints_set_as_resolved(cons, &as);
			} else {
				scope_error(self, type, "STORED or RESOLVED expected");
			}
			if (type->id == KRESOLVED && column->refs > 0)
				scope_error(self, type, "cannot be used together with PRIMARY KEY");
			break;
		}

		default:
			scope_push(self, name);
			done = true;
			break;
		}
	}

	// set DEFAULT NULL by default
	if (! has_default)
		encode_null(&cons->value);
}

static void
parse_columns(Scope* self, Columns* columns, Keys* keys)
{
	// (name type [constraints], ..., primary key())

	// (
	scope_expect(self, '(');

	for (;;)
	{
		// PRIMARY KEY (columns)
		if (parse_primary_key(self))
		{
			parse_key(self, keys);

			// )
			scope_expect(self, ')');
			break;
		}

		// name type [constraint]

		// name
		auto name = scope_expect(self, KNAME);

		// ensure column does not exists
		if (columns_find(keys->columns, &name->string))
			scope_error(self, name, "column is redefined");

		// create column
		auto column = column_allocate();
		column_set_name(column, &name->string);
		columns_add(columns, column);

		// type | SERIAL
		int type_size;
		int type;
		if (parse_type(self, &type, &type_size))
			constraints_set_as_identity(&column->constraints, true);
		column_set_type(column, type, type_size);

		// [PRIMARY KEY | NOT NULL | DEFAULT | RANDOM | AS]
		parse_constraints(self, keys, column);

		// ,
		if (scope_if(self, ','))
			continue;

		// )
		auto rbr = scope_expect(self, ')');
		if (keys->list_count == 0)
			scope_error(self, rbr, "primary key is not defined");
		break;
	}
}

static void
parse_with(Scope* self, AstTableCreate* stmt, IndexConfig* index_config)
{
	// [WITH]
	if (! scope_if(self, KWITH))
		return;

	// (
	scope_expect(self, '(');

	for (;;)
	{
		// key
		auto key = scope_expect(self, KNAME);

		unused(stmt);
		if (str_is(&key->string, "type", 4))
		{
			// =
			scope_expect(self, '=');

			// string
			auto value = scope_expect(self, KSTRING);

			// tree | hash
			if (str_is_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_is_cstr(&value->string, "hash"))
				index_config_set_type(index_config, INDEX_HASH);
			else
				scope_error(self, value, "unrecognized index type");

		} else {
			scope_error(self, key, "unrecognized parameter");
		}

		// ,
		if (scope_if(self, ','))
			continue;

		// )
		if (scope_if(self, ')'))
			break;
	}
}

void
parse_table_create(Scope* self, bool unlogged)
{
	// CREATE [UNLOGGED] TABLE [IF NOT EXISTS] name (key)
	// [PARTITIONS n]
	// [WITH()]
	auto stmt = ast_table_create_allocate();
	self->stmt->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		scope_error(self, NULL, "name expected");

	// create table config
	stmt->config = table_config_allocate();
	table_config_set_unlogged(stmt->config, unlogged);
	table_config_set_schema(stmt->config, &schema);
	table_config_set_name(stmt->config, &name);

	// create primary index config
	auto index_config = index_config_allocate(&stmt->config->columns);
	table_config_add_index(stmt->config, index_config);

	Str index_name;
	str_set_cstr(&index_name, "primary");
	index_config_set_name(index_config, &index_name);
	index_config_set_type(index_config, INDEX_TREE);
	index_config_set_unique(index_config, true);
	index_config_set_primary(index_config, true);

	// (columns)
	parse_columns(self, &stmt->config->columns, &index_config->keys);

	// [PARTITIONS]
	if (scope_if(self, KPARTITIONS))
	{
		auto n = scope_expect(self, KINT);
		stmt->partitions = n->integer;
	} else {
		stmt->partitions = opt_int_of(&config()->backends);
	}

	// [WITH]
	parse_with(self, stmt, index_config);
}

void
parse_table_drop(Scope* self)
{
	// DROP TABLE [IF EXISTS] name
	auto stmt = ast_table_drop_allocate();
	self->stmt->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		scope_error(self, NULL, "name expected");
}

void
parse_table_alter(Scope* self)
{
	// ALTER TABLE [IF EXISTS] [schema.]name ADD COLUMN name type [constraint]
	// ALTER TABLE [IF EXISTS] [schema.]name DROP COLUMN name
	// ALTER TABLE [IF EXISTS] [schema.]name RENAME TO [schema.]name
	// ALTER TABLE [IF EXISTS] [schema.]name SET IDENTITY TO value
	// ALTER TABLE [IF EXISTS] [schema.]name RENAME COLUMN name TO name
	// ALTER TABLE [IF EXISTS] [schema.]name SET COLUMN name DEFAULT const
	// ALTER TABLE [IF EXISTS] [schema.]name SET COLUMN name AS (expr) <STORED|RESOLVED>
	// ALTER TABLE [IF EXISTS] [schema.]name UNSET COLUMN name DEFAULT
	// ALTER TABLE [IF EXISTS] [schema.]name UNSET COLUMN name AS <IDENTITY|STORED|RESOLVED>
	auto stmt = ast_table_alter_allocate();
	self->stmt->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto target = parse_target(self, &stmt->schema, &stmt->name);
	if (! target)
		scope_error(self, NULL, "name expected");

	// [ADD COLUMN]
	if (scope_if(self, KADD))
	{
		scope_expect(self, KCOLUMN);

		// name type [constraint]

		// name
		auto name = scope_expect(self, KNAME);

		// create column
		stmt->column = column_allocate();
		auto column = stmt->column;
		column_set_name(column, &name->string);

		// type | SERIAL
		int type_size;
		int type;
		if (parse_type(self, &type, &type_size))
			constraints_set_as_identity(&column->constraints, true);
		column_set_type(column, type, type_size);

		// [NOT NULL | DEFAULT | RANDOM | AS]
		parse_constraints(self, NULL, column);

		// validate column
		auto cons = &stmt->column->constraints;
		if (cons->not_null)
			scope_error(self, NULL, "NOT NULL currently not supported with ALTER");

		stmt->type = TABLE_ALTER_COLUMN_ADD;
		return;
	}

	// [DROP COLUMN]
	if (scope_if(self, KDROP))
	{
		scope_expect(self, KCOLUMN);

		// name
		auto name = scope_expect(self, KNAME);
		str_set_str(&stmt->column_name, &name->string);

		stmt->type = TABLE_ALTER_COLUMN_DROP;
		return;
	}

	// [RENAME]
	if (scope_if(self, KRENAME))
	{
		// [COLUMN name TO  name]
		if (scope_if(self, KCOLUMN))
		{
			stmt->type = TABLE_ALTER_COLUMN_RENAME;

			// name
			auto name = scope_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// TO
			scope_expect(self, KTO);

			// name
			name = scope_expect(self, KNAME);
			str_set_str(&stmt->name_new, &name->string);
			return;
		}

		// TO
		scope_expect(self, KTO);

		// name
		if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
			scope_error(self, NULL, "name expected");
		stmt->type = TABLE_ALTER_RENAME;
		return;
	}

	// [SET]
	if (scope_if(self, KSET))
	{
		// SET IDENTITY TO value
		if (scope_if(self, KIDENTITY))
		{
			// TO
			scope_expect(self, KTO);

			// int
			stmt->identity = scope_expect(self, KINT);
			stmt->type = TABLE_ALTER_SET_IDENTITY;
			return;
		}

		// SET LOGGED
		if (scope_if(self, KLOGGED))
		{
			stmt->type = TABLE_ALTER_SET_UNLOGGED;
			stmt->unlogged = false;
			return;
		}

		// SET UNLOGGED
		if (scope_if(self, KUNLOGGED))
		{
			stmt->type = TABLE_ALTER_SET_UNLOGGED;
			stmt->unlogged = true;
			return;
		}

		// SET COLUMN DEFAULT
		// SET COLUMN AS
		if (scope_if(self, KCOLUMN))
		{
			// name
			auto name = scope_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT const
			if (scope_if(self, KDEFAULT))
			{
				// find table and column
				auto table  = table_mgr_find(&self->parser->db->table_mgr,
				                             &stmt->schema,
				                             &stmt->name, false);
				if (! table)
					scope_error(self, target, "table not found");
				auto column = columns_find(table_columns(table), &stmt->column_name);
				if (! column)
					scope_error(self, name, "column does not exists");

				auto value = buf_create();
				errdefer_buf(value);
				parse_default(self, column, value);

				stmt->value_buf = value;
				buf_str(value, &stmt->value);
				stmt->type = TABLE_ALTER_COLUMN_SET_DEFAULT;
				return;
			}

			// AS IDENTITY
			// AS (expr) <STORED | RESOLVED>
			if (scope_if(self, KAS))
			{
				if (scope_if(self, KIDENTITY))
				{
					stmt->type = TABLE_ALTER_COLUMN_SET_IDENTITY;
					str_set(&stmt->value, "1", 1);
					return;
				}

				// (
				auto lbr = scope_expect(self, '(');
				// expr
				parse_expr(self, NULL);
				// )
				auto rbr = scope_expect(self, ')');

				Str as;
				str_set(&as, self->lex->start + lbr->pos_start, rbr->pos_end - lbr->pos_start - 1);
				str_shrink(&as);
				as.pos++;
				str_shrink(&as);
				if (str_empty(&as))
					scope_error(self, lbr, "AS expression is missing");
				stmt->value = as;

				// STORED | RESOLVED
				auto type = scope_next(self);
				if (type->id == KSTORED)
					stmt->type = TABLE_ALTER_COLUMN_SET_STORED;
				else
				if (type->id == KRESOLVED)
					stmt->type = TABLE_ALTER_COLUMN_SET_RESOLVED;
				else
					scope_error(self, type, "STORED or RESOLVED expected");
				return;
			}
		}

		scope_error(self, NULL, "'IDENTITY | LOGGED | UNLOGGED | COLUMN DEFAULT | COLUMN AS' expected");
		return;
	}

	// [UNSET]
	if (scope_if(self, KUNSET))
	{
		// UNSET COLUMN DEFAULT
		// UNSET COLUMN AS IDENTITY
		// UNSET COLUMN AS STORED
		// UNSET COLUMN AS RESOLVED
		if (scope_if(self, KCOLUMN))
		{
			// name
			auto name = scope_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT
			if (scope_if(self, KDEFAULT))
			{
				auto value = buf_create();
				errdefer_buf(value);
				encode_null(value);
				stmt->value_buf = value;
				buf_str(value, &stmt->value);
				stmt->type = TABLE_ALTER_COLUMN_UNSET_DEFAULT;
				return;
			}

			// AS
			scope_expect(self, KAS);

			// IDENTITY | STORED | RESOLVED
			if (scope_if(self, KIDENTITY))
				stmt->type = TABLE_ALTER_COLUMN_UNSET_IDENTITY;
			else
			if (scope_if(self, KSTORED))
				stmt->type = TABLE_ALTER_COLUMN_UNSET_STORED;
			else
			if (scope_if(self, KRESOLVED))
				stmt->type = TABLE_ALTER_COLUMN_UNSET_RESOLVED;
			else
				scope_error(self, NULL, "'IDENTITY, STORED or RESOLVED' expected");
			return;
		}

		scope_expect(self, KCOLUMN);
	}

	scope_error(self, NULL, "'RENAME | ADD | DROP | SET | UNSET' expected");
}

void
parse_table_truncate(Scope* self)
{
	// TRUNCATE [IF EXISTS] [schema.]name
	auto stmt = ast_table_truncate_allocate();
	self->stmt->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		scope_error(self, NULL, "name expected");
}
