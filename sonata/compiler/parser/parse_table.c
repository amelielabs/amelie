
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

int
parse_type(Stmt* self, Column* column, Str* path)
{
	auto ast = stmt_next(self);
	int  type = 0;
	switch (ast->id) {
	case KTMAP:
	case KTOBJECT:
		type = TYPE_MAP;
		break;
	case KTARRAY:
		type = TYPE_ARRAY;
		break;
	case KTINT:
	case KTINTEGER:
		type = TYPE_INT;
		break;
	case KTBOOL:
	case KTBOOLEAN:
		type = TYPE_BOOL;
		break;
	case KTREAL:
		type = TYPE_REAL;
		break;
	case KTSTRING:
	case KTEXT:
		type = TYPE_STRING;
		break;
	default:
		if (column && path)
			error("%.*s.%.*s <TYPE> expected",
			      str_size(&column->name),
			      str_of(&column->name),
			      str_size(path),
			      str_of(path));
		else
			error("%.*s <TYPE> expected",
			      str_size(&column->name),
			      str_of(&column->name));
		break;
	}
	return type;
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
		str_split_or_set(&tk->string, &name, '.');

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

		// ASC | DESC
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		// validate key type
		if (type != TYPE_INT && type != TYPE_STRING)
			error("<%.*s> column key can be string or int", str_size(&column->name),
			      str_of(&column->name));

		// force column not_null constraint
		constraint_set_not_null(&column->constraint, true);

		// create key
		auto key = key_allocate();
		key_set_ref(key, column->order);
		key_set_type(key, type);
		key_set_path(key, &path);
		key_set_asc(key, asc);
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
			constraint_set_generated(cons, GENERATED_SERIAL);
			break;
		}

		// RANDOM
		case KRANDOM:
		{
			constraint_set_generated(cons, GENERATED_RANDOM);
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
				constraint_set_modulo(cons, value->integer);
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
			if (column->type != TYPE_INT && column->type != TYPE_STRING)
				error("<%.*s> column key can be string or int",
				      str_size(&column->name), str_of(&column->name));

			// create key
			auto key = key_allocate();
			key_set_ref(key, column->order);
			key_set_type(key, column->type);
			keys_add(keys, key);

			primary_key = true;
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

		// [PRIMARY KEY | NOT NULL | DEFAULT | SERIAL | RANDOM]
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
		if (str_compare_raw(&key->string, "type", 4))
		{
			// =
			if (! stmt_if(self, '='))
				error("WITH (type <=> expected");

			// string
			auto value = stmt_if(self, KSTRING);
			if (! value)
				error("WITH (type = <string>) expected");

			// tree | hash
			if (str_compare_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_compare_cstr(&value->string, "hash"))
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

static inline void
parse_validate_constraints(Columns* columns)
{
	// validate constraints
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto cons = &column->constraint;
		if (cons->generated == GENERATED_NONE)
			continue;

		if (cons->generated == GENERATED_SERIAL ||
		    cons->generated == GENERATED_RANDOM)
		{
			if (column->type != TYPE_INT)
				error("SERIAL column <%.*s> must be integer",
				      str_size(&column->name),
				      str_of(&column->name));
			continue;
		}

		if (column->key)
			error("generated columns cannot be used with keys");
	}
}

void
parse_table_create(Stmt* self)
{
	// CREATE TABLE [IF NOT EXISTS] name (key)
	// [REFERENCE] [WITH()]
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
	parse_validate_constraints(&stmt->config->columns);

	// [REFERENCE]
	if (stmt_if(self, KREFERENCE))
		table_config_set_reference(stmt->config, true);

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

		// [NOT NULL | DEFAULT | SERIAL | RANDOM | GENERATED]
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
		error("ALTER TABLE <RENAME | ADD | DROP> expected");

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
