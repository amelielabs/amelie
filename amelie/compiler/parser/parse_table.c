
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
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

int
parse_type(Stmt* self, Column* column, Str* path)
{
	auto ast  = stmt_next_shadow(self);
	if (ast->id != KNAME)
		goto error;

	auto type = type_read(&ast->string);
	if (type == -1)
		goto error;
	return type;

error:
	if (column && path)
		error("%.*s.%.*s <TYPE> expected",
		      str_size(&column->name),
		      str_of(&column->name),
		      str_size(path),
		      str_of(path));
	error("%.*s <TYPE> expected", str_size(&column->name),
	      str_of(&column->name));
}

static Column*
parse_key_column(Stmt* self, Columns* columns, Str* path)
{
	Column* column = NULL;

	Str     name;
	str_init(&name);

	auto tk = stmt_next(self);
	if (tk->id == KNAME)
	{
		// column
		str_set_str(&name, &tk->string);

	} else
	if (tk->id == KNAME_COMPOUND)
	{
		// column.path
		str_split(&tk->string, &name, '.');

		// exclude column name from the path
		str_set_str(path, &tk->string);
		str_advance(path, str_size(&name) + 1);

	} else {
		return NULL;
	}

	// find column
	column = columns_find(columns, &name);
	if (! column)
		error("<%.*s> column does not exists", str_size(&name),
		      str_of(&name));

	return column;
}

void
parse_key(Stmt* self, Keys* keys)
{
	// (
	if (! stmt_if(self, '('))
		error("KEY <(> expected");

	for (;;)
	{
		Str path;
		str_init(&path);

		// (column, ...)
		// (column.path type, ...)
		auto column = parse_key_column(self, keys->columns, &path);
		if (column == NULL)
			error("KEY (<name> expected");

		// [type]
		int type;
		if (str_empty(&path))
			type = column->type;
		else
			type = parse_type(self, column, &path);

		// validate key type
		if (type != TYPE_INT &&
		    type != TYPE_STRING &&
		    type != TYPE_TIMESTAMP)
			error("<%.*s> column key can be string, int or timestamp",
			      str_size(&column->name),
			      str_of(&column->name));

		// force column not_null constraint
		constraint_set_not_null(&column->constraint, true);

		// create key
		auto key = key_allocate();
		key_set_ref(key, column->order);
		key_set_type(key, type);
		key_set_path(key, &path);
		keys_add(keys, key);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("KEY (<)> expected");
}

static bool
parse_primary_key(Stmt* self)
{
	// PRIMARY KEY
	if (! stmt_if(self, KPRIMARY))
		return false;
	if (! stmt_if(self, KKEY))
		error("PRIMARY <KEY> expected");
	return true;
}

static void
parse_constraint(Stmt* self, Keys* keys, Column* column)
{
	// constraint
	auto cons = &column->constraint;

	bool primary_key = false;
	bool has_default = false;
	bool done = false;
	while (! done)
	{
		auto name = stmt_next(self);
		switch (name->id) {
		// NOT NULL
		case KNOT:
		{
			if (! stmt_if(self, KNULL))
				error("NOT <NULL> expected");
			constraint_set_not_null(cons, true);
			break;
		}

		// DEFAULT expr
		case KDEFAULT:
		{
			buf_reset(&cons->value);
			auto expr = parse_expr(self, NULL);
			switch (expr->id) {
			case KNULL:
				encode_null(&cons->value);
				break;
			case KREAL:
				encode_real(&cons->value, expr->real);
				break;
			case KINT:
				encode_integer(&cons->value, expr->integer);
				break;
			case KSTRING:
				encode_string(&cons->value, &expr->string);
				break;
			case KTRUE:
				encode_bool(&cons->value, true);
				break;
			case KFALSE:
				encode_bool(&cons->value, false);
				break;
			default:
				error("only consts allowed as DEFAULT expression");
				break;
			}
			has_default = true;
			break;
		}

		// SERIAL
		case KSERIAL:
		{
			// ensure the column has type INT
			if (column->type != TYPE_INT)
				error("SERIAL column <%.*s> must be integer",
				      str_size(&column->name),
				      str_of(&column->name));

			constraint_set_serial(cons, true);
			break;
		}

		// RANDOM
		case KRANDOM:
		{
			// ensure the column has type INT
			if (column->type != TYPE_INT)
				error("RANDOM column <%.*s> must be integer",
				      str_size(&column->name),
				      str_of(&column->name));

			constraint_set_random(cons, true);

			// [(modulo)]
			if (stmt_if(self, '('))
			{
				// int
				auto value = stmt_if(self, KINT);
				if (! value)
					error("RANDOM (<integer> expected");
				// )
				if (! stmt_if(self, ')'))
					error("RANDOM (<)> expected");

				if (value->integer == 0)
					error("RANDOM modulo value cannot be zero");
				constraint_set_random_modulo(cons, value->integer);
			}
			break;
		}

		// PRIMARY KEY
		case KPRIMARY:
		{
			if (! stmt_if(self, KKEY))
				error("PRIMARY <KEY> expected");

			if (primary_key)
				error("PRIMARY KEY defined twice");

			if (! keys)
				error("PRIMARY KEY clause is not supported in this command");

			// force not_null constraint for keys
			constraint_set_not_null(&column->constraint, true);

			// validate key type
			if (column->type != TYPE_INT &&
			    column->type != TYPE_STRING &&
			    column->type != TYPE_TIMESTAMP)
				error("<%.*s> column key can be string, int or timestamp",
				      str_size(&column->name),
				      str_of(&column->name));

			// create key
			auto key = key_allocate();
			key_set_ref(key, column->order);
			key_set_type(key, column->type);
			keys_add(keys, key);

			primary_key = true;
			break;
		}

		// GENERATD ALWAYS AS (expr) [STORED]
		case KGENERATED:
		{
			// ALWAYS
			if (! stmt_if(self, KALWAYS))
				error("GENERATED <ALWAYS> expected");

			// AS
			if (! stmt_if(self, KAS))
				error("GENERATED ALWAYS <AS> expected");

			// fallthrough
		}

		// AS (expr) [STORED]
		case KAS:
		{
			// (
			auto lbr = stmt_if(self, '(');
			if (! lbr)
				error("AS <(> expected");

			// expr
			parse_expr(self, NULL);

			// )
			auto rbr = stmt_if(self, ')');
			if (! rbr)
				error("AS (expr <)> expected");

			// [STORED]
			stmt_if(self, KSTORED);

			Str as;
			str_set(&as, lbr->pos, rbr->pos - lbr->pos);
			str_shrink(&as);
			as.pos++;
			str_shrink(&as);
			if (str_empty(&as))
				error("AS expression is missing");
			constraint_set_as_stored(cons, &as);
			break;
		}

		default:
			stmt_push(self, name);
			done = true;
			break;
		}
	}

	// set DEFAULT NULL by default
	if (! has_default)
		encode_null(&cons->value);
}

static void
parse_agg(Stmt* self, Column* column)
{
	// (
	if (! stmt_if(self, '('))
		error("AGGREGATE <(> expected");

	// name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME)
		error("AGGREGATE (<type> expected");

	int type;
	if (str_is(&name->string, "count", 5))
		type = AGG_COUNT;
	else
	if (str_is(&name->string, "min", 3))
		type = AGG_MIN;
	else
	if (str_is(&name->string, "max", 3))
		type = AGG_MAX;
	else
	if (str_is(&name->string, "sum", 3))
		type = AGG_SUM;
	else
	if (str_is(&name->string, "avg", 3))
		type = AGG_AVG;
	else
		error("AGGREGATE '%.*s' is not supported", str_size(&name->string),
		      str_of(&name->string));

	constraint_set_aggregate(&column->constraint, type);

	// )
	if (! stmt_if(self, ')'))
		error("AGGREGATE (type<)> expected");
}

static void
parse_columns(Stmt* self, Columns* columns, Keys* keys)
{
	// (name type [not null | default | serial | primary key], ...,
	//  primary key())

	// (
	if (! stmt_if(self, '('))
		error("CREATE TABLE name <(> expected");

	for (;;)
	{
		// PRIMARY KEY ()
		if (parse_primary_key(self))
		{
			// (columns)
			parse_key(self, keys);
			goto last;
		}

		// name type [constraint]

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("(<name> expected");

		// ensure column does not exists
		if (columns_find(keys->columns, &name->string))
			error("<%.*s> column redefined", str_size(&name->string),
			      str_of(&name->string));

		// create column
		auto column = column_allocate();
		column_set_name(column, &name->string);
		columns_add(columns, column);

		// type
		int type = parse_type(self, column, NULL);
		column_set_type(column, type);

		// (aggregate)
		if (type == TYPE_AGG)
			parse_agg(self, column);

		// [PRIMARY KEY | NOT NULL | DEFAULT | SERIAL | RANDOM | AS]
		parse_constraint(self, keys, column);

		// ,
		if (stmt_if(self, ','))
			continue;
last:
		// )
		if (stmt_if(self, ')'))
			break;

		error("CREATE TABLE name (name type <,)> expected");
	}

	if (keys->list_count == 0)
		error("primary key is not defined");
}

static void
parse_with(Stmt* self, AstTableCreate* stmt, IndexConfig* index_config)
{
	// [WITH]
	if (! stmt_if(self, KWITH))
		return;

	// (
	if (! stmt_if(self, '('))
		error("WITH <(> expected");

	for (;;)
	{
		// key
		auto key = stmt_if(self, KNAME);
		if (! key)
			error("WITH (<name> expected");

		unused(stmt);
		if (str_is(&key->string, "type", 4))
		{
			// =
			if (! stmt_if(self, '='))
				error("WITH (type <=> expected");

			// string
			auto value = stmt_if(self, KSTRING);
			if (! value)
				error("WITH (type = <string>) expected");

			// tree | hash
			if (str_is_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_is_cstr(&value->string, "hash"))
				index_config_set_type(index_config, INDEX_HASH);
			else
				error("WITH: unknown primary index type");

		} else {
			error("WITH: <%.*s> unrecognized parameter",
			      str_size(&key->string), str_of(&key->string));
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (stmt_if(self, ')'))
			break;
	}
}

void
parse_table_create(Stmt* self, bool shared)
{
	// CREATE [SHARED|DISTRIBUTED] TABLE [IF NOT EXISTS] name (key)
	// [WITH()]
	auto stmt = ast_table_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("CREATE TABLE <name> expected");

	// create table config
	stmt->config = table_config_allocate();
	table_config_set_shared(stmt->config, shared);
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

	// [WITH]
	parse_with(self, stmt, index_config);
}

void
parse_table_drop(Stmt* self)
{
	// DROP TABLE [IF EXISTS] name
	auto stmt = ast_table_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("DROP TABLE <name> expected");
}

void
parse_table_alter(Stmt* self)
{
	// ALTER TABLE [IF EXISTS] [schema.]name RENAME TO [schema.]name
	// ALTER TABLE [IF EXISTS] [schema.]name SET SERIAL TO value
	// ALTER TABLE [IF EXISTS] [schema.]name COLUMN ADD name type [constraint]
	// ALTER TABLE [IF EXISTS] [schema.]name COLUMN DROP name
	// ALTER TABLE [IF EXISTS] [schema.]name COLUMN RENAME name TO name
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER TABLE <name> expected");

	// [ADD COLUMN]
	if (stmt_if(self, KADD))
	{
		if (! stmt_if(self, KCOLUMN))
			error("ALTER TABLE name ADD <COLUMN> expected");

		// name type [constraint]

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("(<name> expected");

		// create column
		stmt->column = column_allocate();
		auto column = stmt->column;
		column_set_name(column, &name->string);

		// type
		int type = parse_type(self, column, NULL);
		column_set_type(column, type);

		// (aggregate)
		if (type == TYPE_AGG)
			parse_agg(self, column);

		// [NOT NULL | DEFAULT | SERIAL | RANDOM | AS]
		parse_constraint(self, NULL, column);

		// validate column
		auto cons = &stmt->column->constraint;
		if (cons->not_null && data_is_null(cons->value.start))
			error("ALTER TABLE ADD NOT NULL requires DEFAULT value");

		stmt->type = TABLE_ALTER_COLUMN_ADD;
		return;
	}

	// [DROP COLUMN]
	if (stmt_if(self, KDROP))
	{
		if (! stmt_if(self, KCOLUMN))
			error("ALTER TABLE name DROP <COLUMN> expected");

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("ALTER TABLE name DROP COLUMN <name> expected");
		str_set_str(&stmt->column_name, &name->string);

		stmt->type = TABLE_ALTER_COLUMN_DROP;
		return;
	}

	// [SET]
	if (stmt_if(self, KSET))
	{
		// SERIAL
		if (! stmt_if(self, KSERIAL))
			error("ALTER TABLE SET <SERIAL> expected");

		// TO
		if (! stmt_if(self, KTO))
			error("ALTER TABLE SET SERIAL <TO> expected");

		// int
		stmt->serial = stmt_if(self, KINT);
		if (! stmt->serial)
			error("ALTER TABLE SET SERIAL TO <integer> expected");

		stmt->type = TABLE_ALTER_SET_SERIAL;
		return;
	}

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER TABLE <RENAME | ADD | DROP | SET> expected");

	// [COLUMN name TO  name]
	if (stmt_if(self, KCOLUMN))
	{
		stmt->type = TABLE_ALTER_COLUMN_RENAME;

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("ALTER TABLE name RENAME COLUMN <name> expected");
		str_set_str(&stmt->column_name, &name->string);

		// [TO]
		stmt_if(self, KTO);

		// name
		name = stmt_if(self, KNAME);
		if (! name)
			error("ALTER TABLE name RENAME COLUMN name TO <name> expected");
		str_set_str(&stmt->name_new, &name->string);
		return;
	}

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		error("ALTER TABLE RENAME <name> expected");
	stmt->type = TABLE_ALTER_RENAME;
}

void
parse_table_truncate(Stmt* self)
{
	// TRUNCATE [IF EXISTS] [schema.]name
	auto stmt = ast_table_truncate_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("TRUNCATE <name> expected");
}
