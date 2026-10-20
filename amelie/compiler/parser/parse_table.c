
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

void
parse_key(Stmt* self, Keys* keys)
{
	// ( key, ... )
	stmt_expect(self, '(');

	for (;;)
	{
		// (column, ...)
		auto name = stmt_expect(self, KNAME);

		// find column
		auto column = columns_find(keys->columns, &name->string);
		if (! column)
			stmt_error(self, name, "column does not exists");

		// validate key type
		if ((column->type != TYPE_INT    &&
		     column->type != TYPE_STRING &&
		     column->type != TYPE_UUID   &&
		     column->type != TYPE_TIMESTAMP) ||
		    (column->type == TYPE_INT && column->size < 4))
			stmt_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

		// [ASC|DESC]
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		// dropped support
		if (column->dropped)
			stmt_error(self, name, "column marked as dropped");

		// ensure key is not redefined
		auto key = keys_find_column(keys, column->order);
		if (key)
			stmt_error(self, name, "key column is redefined");

		// create key
		keys_add(keys, column->order, asc);

		// check keys limit
		if (! local_limit(self->parser->local, LIMIT_KEYS, keys->count))
			stmt_error(self, name, "key limit reached");

		// ,
		auto ast = stmt_next(self);
		if (ast->id == ',')
			continue;

		// )
		stmt_push(self, ast);
		stmt_expect(self, ')');
		break;
	}
}

static void
parse_default(Stmt* self, Column* column, Buf* buf)
{
	if (column->type == TYPE_VECTOR)
		stmt_error(self, NULL, "DEFAULT value for vector columns is not supported");

	buf_reset(buf);

	// DEFAULT value
	Value value;
	value_init(&value);
	defer(value_free, &value);
	parse_value_const(self, column, &value);

	// encode value as row data
	auto size = value_data_size(&value, column, NULL);
	if (! size)
		return;
	buf_reserve(buf, size);
	value_data_encode(&value, column, NULL, NULL, &buf->position);
}

static void
parse_constraints(Stmt* self, TableConfig* config, Column* column)
{
	// constraints
	auto cons = &column->constraints;

	bool done = false;
	while (! done)
	{
		auto name = stmt_next(self);
		switch (name->id) {
		// NOT NULL
		case KNOT:
		{
			// NULL
			stmt_expect(self, KNULL);
			constraints_set_not_null(cons, true);
			break;
		}

		// DEFAULT expr
		case KDEFAULT:
		{
			parse_default(self, column, &cons->value);
			break;
		}

		// PRIMARY KEY [USING type]
		case KPRIMARY:
		{
			stmt_expect(self, KKEY);

			if (! config)
				stmt_error(self, name, "PRIMARY KEY clause is not supported in this command");

			if (table_config_primary(config))
				stmt_error(self, name, "PRIMARY KEY defined twice");

			// force not_null constraint for keys
			constraints_set_not_null(&column->constraints, true);

			// validate key type
			if ((column->type != TYPE_INT    &&
			     column->type != TYPE_STRING &&
			     column->type != TYPE_UUID   &&
			     column->type != TYPE_TIMESTAMP) ||
			    (column->type == TYPE_INT && column->size < 4))
				stmt_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

			// create primary index config
			auto primary = index_config_allocate(&config->columns);
			table_config_index_add(config, primary);
			Str index_name;
			str_set_cstr(&index_name, "primary");
			index_config_set_name(primary, &index_name);
			index_config_set_type(primary, INDEX_TREE);
			index_config_set_unique(primary, true);
			index_config_set_primary(primary, true);

			// create key
			keys_add(&primary->keys, column->order, true);

			// [USING type]
			parse_index_using(self, primary);
			break;
		}

		// [GENERATED ALWAYS] [AS] ...
		case KGENERATED:
		{
			// ALWAYS
			stmt_expect(self, KALWAYS);
			fallthrough;
		}

		// [AS] ...
		case KAS:
		{
			// IDENTITY
			stmt_expect(self, KIDENTITY);
			fallthrough;
		}

		// IDENTITY [(modulo])
		case KIDENTITY:
		{
			if (cons->identity)
				stmt_error(self, name, "IDENTITY defined twice");

			// ensure the column has type INT64
			auto valid = true;
			if (column->type == TYPE_INT)
				valid = column->size >= 4;
			else
			if (column->type != TYPE_UUID)
				valid = false;
			if (! valid)
				stmt_error(self, name, "identity column must be UUID, INT or INT64");

			constraints_set_identity(cons, true);

			// [(modulo)]
			if (stmt_if(self, '('))
			{
				// int
				auto value = stmt_expect(self, KINT);
				// )
				stmt_expect(self, ')');
				if (value->integer == 0)
					stmt_error(self, value, "IDENTITY modulo value cannot be zero");

				constraints_set_identity_modulo(cons, value->integer);
			}
			break;
		}

		// mark column as dropped (recovery support)
		case KDROP:
		{
			column->dropped = true;
			break;
		}

		default:
			stmt_push(self, name);
			done = true;
			break;
		}
	}

	// dropped column
	if (column->dropped && column->refs > 0)
		stmt_error(self, NULL, "dropped column cannot be a key");

	// do not allow identity columns and default values together
	if (cons->identity && !buf_empty(&cons->value))
		stmt_error(self, NULL, "identity column cannot have default value");
}

static void
parse_columns(Stmt* self, TableConfig* config)
{
	// (name type [constraints], ..., primary key)

	// (
	stmt_expect(self, '(');

	auto local = self->parser->local;
	Column* identity = NULL;
	for (;;)
	{
		auto ast = stmt_next(self);
		switch (ast->id) {
		case KPRIMARY:
		{
			// PRIMARY KEY (columns) [USING type]
			stmt_expect(self, KKEY);

			if (table_config_primary(config))
				stmt_error(self, ast, "PRIMARY KEY defined twice");

			// create primary index config
			auto primary = index_config_allocate(&config->columns);
			table_config_index_add(config, primary);
			Str index_name;
			str_set_cstr(&index_name, "primary");
			index_config_set_name(primary, &index_name);
			index_config_set_type(primary, INDEX_TREE);
			index_config_set_unique(primary, true);
			index_config_set_primary(primary, true);

			auto keys = &primary->keys;
			parse_key(self, keys);

			// force column not_null constraint
			for (auto at = 0; at < keys->count; at++)
			{
				auto key = keys_at(keys, at);
				constraints_set_not_null(&key->column->constraints, true);
			}

			parse_index_using(self, primary);
			break;
		}

		case KNAME:
		{
			// name type [constraint]

			// name
			auto name = ast;

			// ensure column does not exists
			auto columns = &config->columns;
			if (columns_find(columns, &name->string))
				stmt_error(self, name, "column is redefined");

			// create column
			auto column = column_allocate();
			column_set_name(column, &name->string);
			columns_add(columns, column);

			// check columns limit
			if (! local_limit(local, LIMIT_COLUMNS, columns->count))
				stmt_error(self, name, "columns limit reached");

			// type
			parse_type_column(self->lex, local, column);

			// [PRIMARY KEY | NOT NULL | DEFAULT | AS]
			parse_constraints(self, config, column);

			// ensure identity column only one
			auto cons = &column->constraints;
			if (cons->identity)
			{
				if (identity)
					stmt_error(self, name, "only one IDENTITY column is allowed");
				identity = column;
			}
			break;
		}
		default:
		{
			stmt_error(self, ast, "expected column name or PRIMARY KEY");
			break;
		}
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		break;
	}

	// )
	auto rbr = stmt_expect(self, ')');

	// ensure identity column is a key
	if (identity)
	{
		auto primary = table_config_primary(config);
		if (! primary)
			stmt_error(self, rbr, "PRIMARY KEY must be defined for indentity column");

		auto keys  = &primary->keys;
		auto match = false;
		for (auto at = 0; at < keys->count; at++)
		{
			auto key = keys_at(keys, at);
			if (key->column != identity)
				continue;
			match = true;
			break;
		}
		if (! match)
			stmt_error(self, rbr, "IDENTITY column must be part of PRIMARY KEY");
	}
}

void
parse_table_create(Stmt* self)
{
	// CREATE TABLE [IF NOT EXISTS] name [ON name]
	// (columns)
	// [DESCRIPTION]
	// [RETENTION]
	// [INDEX]
	// [GRANT]
	//
	auto stmt = ast_table_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// [user.]name
	Str user;
	Str name;
	parse_target(self, &user, &name);

	// create table config
	auto config = table_config_allocate();
	stmt->config = config;
	table_config_set_user(config, &user);
	table_config_set_name(config, &name);

	// (columns)
	parse_columns(self, config);

	// set table options
	auto primary = table_config_primary(config);
	for (;;)
	{
		// name value
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
		{
			stmt_push(self, name);
			break;
		}

		// RETENTION int
		if (str_is_case(&name->string, "retention", 9))
		{
			auto value = stmt_expect(self, KINT);
			if (value->integer <= 0)
				stmt_error(self, value, "invalid retention size");

			// align to the page size
			auto page_size = 2 * 1024 * 1024;
			value->integer = ((value->integer + page_size - 1) / page_size) * page_size;

			if (config->indexes_count > 0)
				stmt_error(self, value, "using indexes with retention is not supported");
			table_config_set_retention(config, value->integer);
			continue;
		}

		// DESCRIPTION string
		if (str_is_case(&name->string, "description", 11))
		{
			auto text = stmt_expect(self, KSTRING);
			table_config_set_description(config, &text->string);
			continue;
		}

		// GRANT name, ... TO user
		if (str_is_case(&name->string, "grant", 5))
		{
			parse_grant_to_inline(self, &config->grants);
			continue;
		}

		// INDEX name ...
		if (str_is_case(&name->string, "index", 5))
		{
			auto index_name = stmt_expect(self, KNAME);
			if (table_config_find(config, &index_name->string))
				stmt_error(self, index_name, "index redefined");

			// create secondary index
			auto secondary = index_config_allocate(&config->columns);
			table_config_index_add(config, secondary);
			index_config_set_name(secondary, &index_name->string);

			auto primary = table_config_primary(config);
			parse_index_create_inline(self, primary, secondary);
			continue;
		}

		// UNIQUE INDEX name ...
		if (str_is_case(&name->string, "unique", 6))
		{
			stmt_expect(self, KINDEX);
			auto index_name = stmt_expect(self, KNAME);
			if (table_config_find(config, &index_name->string))
				stmt_error(self, index_name, "index redefined");

			// create secondary index
			auto secondary = index_config_allocate(&config->columns);
			table_config_index_add(config, secondary);
			index_config_set_name(secondary, &index_name->string);
			index_config_set_unique(secondary, true);

			auto primary = table_config_primary(config);
			parse_index_create_inline(self, primary, secondary);
			continue;
		}

		stmt_error(self, name, "unrecognized option");
	}

	// configure index size
	if (primary)
		parse_index_size(self, primary);
}

void
parse_table_drop(Stmt* self)
{
	// DROP TABLE [IF EXISTS] name [CASCADE]
	auto stmt = ast_table_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	parse_target(self, &stmt->user, &stmt->name);

	// [CASCADE]
	stmt->cascade = stmt_if(self, KCASCADE) != NULL;
}

void
parse_table_alter(Stmt* self)
{
	// ALTER TABLE [IF EXISTS] name RENAME TO name
	// ALTER TABLE [IF EXISTS] name DESCRIPTION value 
	// ALTER TABLE [IF EXISTS] name ADD COLUMN [IF NOT EXISTS] name type [constraint]
	// ALTER TABLE [IF EXISTS] name DROP COLUMN [IF EXISTS] name
	// ALTER TABLE [IF EXISTS] name RENAME COLUMN [IF EXISTS] name TO name
	// ALTER TABLE [IF EXISTS] name SET COLUMN [IF EXISTS] name DEFAULT const
	// ALTER TABLE [IF EXISTS] name UNSET COLUMN [IF EXISTS] name DEFAULT
	// ALTER TABLE [IF EXISTS] name SET RETENTION value
	// ALTER TABLE [IF EXISTS] name UNSET RETENTION
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// [if exists]
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	auto target = parse_target(self, &stmt->user, &stmt->name);

	// [ADD COLUMN]
	if (stmt_if(self, KADD))
	{
		if (stmt_if(self, KCOLUMN))
		{
			// [if not exists]
			stmt->if_column_not_exists = parse_if_not_exists(self);

			// name type [constraint]

			// name
			auto name = stmt_expect(self, KNAME);

			// create column
			stmt->column = column_allocate();
			auto column = stmt->column;
			column_set_name(column, &name->string);

			// type
			parse_type_column(self->lex, self->parser->local, stmt->column);

			// [NOT NULL | DEFAULT | AS]
			parse_constraints(self, NULL, column);

			// todo: require DEFAULT for NOT NULL

			// validate column
			auto cons = &stmt->column->constraints;
			if (cons->not_null)
				stmt_error(self, NULL, "NOT NULL currently not supported with ALTER");

			if (cons->identity)
				stmt_error(self, NULL, "IDENTITY column cannot be added");

			stmt->type = TABLE_ALTER_COLUMN_ADD;
		} else {
			stmt_error(self, NULL, "COLUMN or STORAGE expected");
		}
		return;
	}

	// [DROP COLUMN]
	if (stmt_if(self, KDROP))
	{
		if (stmt_if(self, KCOLUMN))
		{
			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			stmt->type = TABLE_ALTER_COLUMN_DROP;
		} else {
			stmt_error(self, NULL, "COLUMN or STORAGE expected");
		}
		return;
	}

	// [RENAME]
	if (stmt_if(self, KRENAME))
	{
		// [COLUMN name TO  name]
		if (stmt_if(self, KCOLUMN))
		{
			stmt->type = TABLE_ALTER_COLUMN_RENAME;

			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// TO
			stmt_expect(self, KTO);

			// name
			name = stmt_expect(self, KNAME);
			str_set_str(&stmt->name_new, &name->string);
			return;
		}

		// TO
		stmt_expect(self, KTO);

		// name
		auto name_new  = stmt_expect(self, KNAME);
		stmt->name_new = name_new->string;
		stmt->type = TABLE_ALTER_RENAME;
		return;
	}

	// [DESCRIPTION]
	if (stmt_if(self, KDESCRIPTION))
	{
		auto value = stmt_expect(self, KSTRING);
		stmt->type = TABLE_ALTER_DESCRIPTION;
		stmt->description = value->string;
		return;
	}

	// [SET]
	if (stmt_if(self, KSET))
	{
		// SET COLUMN DEFAULT
		if (stmt_if(self, KCOLUMN))
		{
			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT value
			stmt_expect(self, KDEFAULT);

			// find table and column
			auto table  = catalog_find_table(&share()->db->catalog, self->parser->user,
			                                 &stmt->name, false);
			if (! table)
				stmt_error(self, target, "table not found");
			auto column = columns_find(table_columns(table), &stmt->column_name);
			if (! column)
				stmt_error(self, name, "column does not exists");

			// value
			auto value = buf_create();
			errdefer_buf(value);
			parse_default(self, column, value);

			stmt->value_buf = value;
			buf_str(value, &stmt->value);
			stmt->type = TABLE_ALTER_COLUMN_SET_DEFAULT;
			return;
		}

		// SET RETENTION
		if (stmt_if(self, KRETENTION))
		{
			auto value = stmt_expect(self, KINT);
			if (value->integer <= 0)
				stmt_error(self, value, "invalid retention size");

			// align to the page size
			auto page_size = 2 * 1024 * 1024;
			value->integer = ((value->integer + page_size - 1) / page_size) * page_size;

			stmt->type = TABLE_ALTER_RETENTION;
			stmt->retention = value->integer;
			return;
		}

		stmt_error(self, NULL, "'COLUMN DEFAULT' expected");
		return;
	}

	// [UNSET]
	if (stmt_if(self, KUNSET))
	{
		// UNSET COLUMN DEFAULT
		if (stmt_if(self, KCOLUMN))
		{
			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT
			stmt_expect(self, KDEFAULT);

			auto value = buf_create();
			errdefer_buf(value);
			stmt->value_buf = value;
			buf_str(value, &stmt->value);
			stmt->type = TABLE_ALTER_COLUMN_UNSET_DEFAULT;
			return;
		}

		// UNSET RETENTION
		if (stmt_if(self, KRETENTION))
		{
			stmt->type = TABLE_ALTER_RETENTION;
			stmt->retention = -1;
			return;
		}

		stmt_expect(self, KCOLUMN);
	}

	stmt_error(self, NULL, "'RENAME | ADD | DROP | SET | UNSET' expected");
}

void
parse_table_truncate(Stmt* self)
{
	// TRUNCATE name
	auto stmt = ast_table_truncate_allocate();
	self->ast = &stmt->ast;

	// [user.]name
	auto path = parse_target(self, &stmt->user, &stmt->name);
	auto table = catalog_find_table(&share()->db->catalog, &stmt->user, &stmt->name, false);
	if (! table)
		stmt_error(self, path, "table not found");
}
