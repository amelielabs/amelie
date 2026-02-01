
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
	// (
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
		    (column->type == TYPE_INT && column->type_size < 4))
			stmt_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

		// [ASC|DESC]
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		// force column not_null constraint
		constraints_set_not_null(&column->constraints, true);

		// create key
		auto key = key_allocate();
		key_set_ref(key, column->order);
		key_set_asc(key, asc);
		keys_add(keys, key);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	stmt_expect(self, ')');
}

static inline bool
parse_primary_key(Stmt* self)
{
	// PRIMARY KEY [USING type]
	if (! stmt_if(self, KPRIMARY))
		return false;
	stmt_expect(self, KKEY);
	parse_index_using(self, ast_table_create_of(self->ast)->config_index);
	return true;
}

static void
parse_default(Stmt* self, Column* column, Buf* value)
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
		stmt_error(self, expr, "DEFAULT for this column type is not supported");
		break;
	}
	if (! type_match)
		stmt_error(self, expr, "DEFAULT value must be a const and match the column type");
}

static void
parse_constraints(Stmt* self, Keys* keys, Column* column)
{
	// constraints
	auto cons = &column->constraints;

	bool has_primary_key = false;
	bool has_default     = false;
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
			has_default = true;
			break;
		}

		// PRIMARY KEY [USING type]
		case KPRIMARY:
		{
			stmt_expect(self, KKEY);

			if (has_primary_key)
				stmt_error(self, name, "PRIMARY KEY defined twice");

			if (! keys)
				stmt_error(self, name, "PRIMARY KEY clause is not supported in this command");

			// force not_null constraint for keys
			constraints_set_not_null(&column->constraints, true);

			// validate key type
			if ((column->type != TYPE_INT    &&
			     column->type != TYPE_STRING &&
			     column->type != TYPE_UUID   &&
			     column->type != TYPE_TIMESTAMP) ||
			    (column->type == TYPE_INT && column->type_size < 4))
				stmt_error(self, name, "supported key types are int32, int64, uuid, timestamp or text");

			// create key
			auto key = key_allocate();
			key_set_ref(key, column->order);
			keys_add(keys, key);

			has_primary_key = true;

			if (! str_empty(&cons->as_resolved))
				stmt_error(self, name, "cannot be used together with RESOLVED");

			// [USING type]
			parse_index_using(self, ast_table_create_of(self->ast)->config_index);
			break;
		}

		// [GENERATD ALWAYS] AS ...
		case KGENERATED:
		{
			// ALWAYS
			stmt_expect(self, KALWAYS);

			// AS
			stmt_expect(self, KAS);
			fallthrough;
		}

		// AS IDENTITY
		// AS IDENTITY RANDOM [(modulo])
		// AS (expr) STORED | RESOLVED
		case KAS:
		{
			auto identity = stmt_if(self, KIDENTITY);
			if (identity)
			{
				if (cons->as_identity)
					stmt_error(self, identity, "IDENTITY defined twice");

				// ensure the column has type INT64
				if (column->type != TYPE_INT || column->type_size < 4)
					stmt_error(self, identity, "identity column must be int or int64");

				constraints_set_as_identity(cons, IDENTITY_SERIAL);

				// RANDOM
				if (stmt_if(self, KRANDOM))
				{
					constraints_set_as_identity(cons, IDENTITY_RANDOM);

					// [(modulo)]
					if (stmt_if(self, '('))
					{
						// int
						auto value = stmt_expect(self, KINT);
						// )
						stmt_expect(self, ')');
						if (value->integer == 0)
							stmt_error(self, value, "RANDOM modulo value cannot be zero");

						constraints_set_as_identity_modulo(cons, value->integer);
					}
				}
				break;
			}

			// (
			auto lbr = stmt_expect(self, '(');
			// expr
			parse_expr(self, NULL);
			// )
			auto rbr = stmt_expect(self, ')');

			Str as;
			str_set(&as, self->lex->start + lbr->pos_start, rbr->pos_end - lbr->pos_start - 1);
			str_shrink(&as);
			as.pos++;
			str_shrink(&as);
			if (str_empty(&as))
				stmt_error(self, lbr, "AS expression is missing");

			// STORED | RESOLVED
			auto type = stmt_next(self);
			if (type->id == KSTORED) {
				constraints_set_as_stored(cons, &as);
			} else
			if (type->id == KRESOLVED)
			{
				constraints_set_as_resolved(cons, &as);
			} else {
				stmt_error(self, type, "STORED or RESOLVED expected");
			}
			if (type->id == KRESOLVED && column->refs > 0)
				stmt_error(self, type, "cannot be used together with PRIMARY KEY");
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
	// (name type [constraints], ..., primary key())

	// (
	stmt_expect(self, '(');

	Column* identity = NULL;
	for (;;)
	{
		// PRIMARY KEY (columns) [USING type]
		if (parse_primary_key(self))
		{
			parse_key(self, keys);
			parse_index_using(self, ast_table_create_of(self->ast)->config_index);
			break;
		}

		// name type [constraint]

		// name
		auto name = stmt_expect(self, KNAME);

		// ensure column does not exists
		if (columns_find(keys->columns, &name->string))
			stmt_error(self, name, "column is redefined");

		// create column
		auto column = column_allocate();
		column_set_name(column, &name->string);
		columns_add(columns, column);

		// SERIAL | type
		auto ast = stmt_next_shadow(self);
		if (ast->id != KNAME)
			stmt_error(self, ast, "unrecognized data type");

		int type_size;
		int type;
		if (str_is_case(&ast->string, "serial", 6))
		{
			type = TYPE_INT;
			type_size = sizeof(int64_t);
			constraints_set_as_identity(&column->constraints, IDENTITY_SERIAL);
		} else
		{
			stmt_push(self, ast);
			type = parse_type(self->lex, &type_size);
		}
		column_set_type(column, type, type_size);

		// [PRIMARY KEY | NOT NULL | DEFAULT | AS]
		parse_constraints(self, keys, column);

		// ensure identity column only one
		if (column->constraints.as_identity)
		{
			if (identity)
				stmt_error(self, name, "only one IDENTITY column is allowed");
			identity = column;
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		break;
	}

	// )
	auto rbr = stmt_expect(self, ')');

	// ensure primary key is defined
	if (keys->list_count == 0)
		stmt_error(self, rbr, "primary key is not defined");

	// ensure identity column is a key
	if (identity)
	{
		auto match = false;
		list_foreach(&keys->list)
		{
			auto key = list_at(Key, link);
			if (key->column != identity)
				continue;
			match = true;
			break;
		}
		if (! match)
			stmt_error(self, rbr, "IDENTITY column can only be used with primary key");
	}
}

void
parse_table_create(Stmt* self, bool unlogged)
{
	// CREATE [UNLOGGED] TABLE [IF NOT EXISTS] name (key)
	// [PARTITIONS n]
	auto stmt = ast_table_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// create table config
	auto config = table_config_allocate();
	stmt->config = config;
	table_config_set_unlogged(config, unlogged);
	table_config_set_db(config, self->parser->db);
	table_config_set_name(config, &name->string);
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
	table_config_set_id(config, &id);

	// create main tier config
	auto tier = tier_allocate();
	stmt->tier = tier;
	table_config_tier_add(config, tier);
	Str tier_name;
	str_set_cstr(&tier_name, "main");
	tier_set_name(tier, &tier_name);
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
	tier_set_id(tier, &id);

	// create main tier storage (use main storage)
	auto tier_storage = tier_storage_allocate();
	tier_storage_set_name(tier_storage, &tier_name);
	tier_storage_add(tier, tier_storage);

	// create primary index config
	auto config_index = index_config_allocate(&config->columns);
	stmt->config_index = config_index;
	table_config_index_add(config, config_index);

	Str index_name;
	str_set_cstr(&index_name, "primary");
	index_config_set_name(config_index, &index_name);
	index_config_set_type(config_index, INDEX_TREE);
	index_config_set_unique(config_index, true);
	index_config_set_primary(config_index, true);

	// (columns)
	parse_columns(self, &config->columns, &config_index->keys);

	// configure partitions mapping

	// [PARTITIONS]
	auto hash_partitions = opt_int_of(&config()->backends);
	if (stmt_if(self, KPARTITIONS))
	{
		auto n = stmt_expect(self, KINT);
		hash_partitions = n->integer;
	}
	if (hash_partitions < 1 || hash_partitions >= PART_MAPPING_MAX)
		stmt_error(self, NULL, "table has invalid hash partitions number");

	table_config_set_partitions(config, hash_partitions);
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
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;
}

void
parse_table_alter(Stmt* self)
{
	// ALTER TABLE [IF EXISTS] name RENAME TO name
	// ALTER TABLE [IF EXISTS] name SET IDENTITY TO value
	// ALTER TABLE [IF EXISTS] name ADD COLUMN [IF NOT EXISTS] name type [constraint]
	// ALTER TABLE [IF EXISTS] name DROP COLUMN [IF EXISTS] name
	// ALTER TABLE [IF EXISTS] name RENAME COLUMN [IF EXISTS] name TO name
	// ALTER TABLE [IF EXISTS] name SET COLUMN [IF EXISTS] name DEFAULT const
	// ALTER TABLE [IF EXISTS] name SET COLUMN [IF EXISTS] name AS (expr) <STORED|RESOLVED>
	// ALTER TABLE [IF EXISTS] name UNSET COLUMN [IF EXISTS] name DEFAULT
	// ALTER TABLE [IF EXISTS] name UNSET COLUMN [IF EXISTS] name AS <IDENTITY|STORED|RESOLVED>
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// [if exists]
	stmt->if_exists = parse_if_exists(self);

	// name
	auto target = stmt_expect(self, KNAME);
	stmt->name = target->string;

	// [ADD COLUMN]
	if (stmt_if(self, KADD))
	{
		stmt_expect(self, KCOLUMN);

		// [if not exists]
		stmt->if_column_not_exists = parse_if_not_exists(self);

		// name type [constraint]

		// name
		auto name = stmt_expect(self, KNAME);

		// create column
		stmt->column = column_allocate();
		auto column = stmt->column;
		column_set_name(column, &name->string);

		// SERIAL | type
		auto ast = stmt_next_shadow(self);
		if (ast->id != KNAME)
			stmt_error(self, ast, "unrecognized data type");

		int type_size;
		int type;
		if (str_is_case(&ast->string, "serial", 6))
		{
			type = TYPE_INT;
			type_size = sizeof(int64_t);
			constraints_set_as_identity(&column->constraints, IDENTITY_SERIAL);
		} else
		{
			stmt_push(self, ast);
			type = parse_type(self->lex, &type_size);
		}
		column_set_type(column, type, type_size);

		// [NOT NULL | DEFAULT | AS]
		parse_constraints(self, NULL, column);

		// validate column
		auto cons = &stmt->column->constraints;
		if (cons->not_null)
			stmt_error(self, NULL, "NOT NULL currently not supported with ALTER");

		stmt->type = TABLE_ALTER_COLUMN_ADD;
		return;
	}

	// [DROP COLUMN]
	if (stmt_if(self, KDROP))
	{
		stmt_expect(self, KCOLUMN);

		// [if exists]
		stmt->if_column_exists = parse_if_exists(self);

		// name
		auto name = stmt_expect(self, KNAME);
		str_set_str(&stmt->column_name, &name->string);

		stmt->type = TABLE_ALTER_COLUMN_DROP;
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

	// [SET]
	if (stmt_if(self, KSET))
	{
		// SET IDENTITY TO value
		if (stmt_if(self, KIDENTITY))
		{
			// TO
			stmt_expect(self, KTO);

			// int
			stmt->identity = stmt_expect(self, KINT);
			stmt->type = TABLE_ALTER_SET_IDENTITY;
			return;
		}

		// SET LOGGED
		if (stmt_if(self, KLOGGED))
		{
			stmt->type = TABLE_ALTER_SET_UNLOGGED;
			stmt->unlogged = false;
			return;
		}

		// SET UNLOGGED
		if (stmt_if(self, KUNLOGGED))
		{
			stmt->type = TABLE_ALTER_SET_UNLOGGED;
			stmt->unlogged = true;
			return;
		}

		// SET COLUMN DEFAULT
		// SET COLUMN AS
		if (stmt_if(self, KCOLUMN))
		{
			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT const
			if (stmt_if(self, KDEFAULT))
			{
				// find table and column
				auto table  = table_mgr_find(&share()->db->catalog.table_mgr,
				                             self->parser->db,
				                             &stmt->name, false);
				if (! table)
					stmt_error(self, target, "table not found");
				auto column = columns_find(table_columns(table), &stmt->column_name);
				if (! column)
					stmt_error(self, name, "column does not exists");

				auto value = buf_create();
				errdefer_buf(value);
				parse_default(self, column, value);

				stmt->value_buf = value;
				buf_str(value, &stmt->value);
				stmt->type = TABLE_ALTER_COLUMN_SET_DEFAULT;
				return;
			}

			// AS (expr) <STORED | RESOLVED>
			if (stmt_if(self, KAS))
			{
				// (
				auto lbr = stmt_expect(self, '(');
				// expr
				parse_expr(self, NULL);
				// )
				auto rbr = stmt_expect(self, ')');

				Str as;
				str_set(&as, self->lex->start + lbr->pos_start, rbr->pos_end - lbr->pos_start - 1);
				str_shrink(&as);
				as.pos++;
				str_shrink(&as);
				if (str_empty(&as))
					stmt_error(self, lbr, "AS expression is missing");
				stmt->value = as;

				// STORED | RESOLVED
				auto type = stmt_next(self);
				if (type->id == KSTORED)
					stmt->type = TABLE_ALTER_COLUMN_SET_STORED;
				else
				if (type->id == KRESOLVED)
					stmt->type = TABLE_ALTER_COLUMN_SET_RESOLVED;
				else
					stmt_error(self, type, "STORED or RESOLVED expected");
				return;
			}
		}

		stmt_error(self, NULL, "'LOGGED | UNLOGGED | COLUMN DEFAULT | COLUMN AS' expected");
		return;
	}

	// [UNSET]
	if (stmt_if(self, KUNSET))
	{
		// UNSET COLUMN DEFAULT
		// UNSET COLUMN AS STORED
		// UNSET COLUMN AS RESOLVED
		if (stmt_if(self, KCOLUMN))
		{
			// [if exists]
			stmt->if_column_exists = parse_if_exists(self);

			// name
			auto name = stmt_expect(self, KNAME);
			str_set_str(&stmt->column_name, &name->string);

			// DEFAULT
			if (stmt_if(self, KDEFAULT))
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
			stmt_expect(self, KAS);

			// IDENTITY | STORED | RESOLVED
			if (stmt_if(self, KSTORED))
				stmt->type = TABLE_ALTER_COLUMN_UNSET_STORED;
			else
			if (stmt_if(self, KRESOLVED))
				stmt->type = TABLE_ALTER_COLUMN_UNSET_RESOLVED;
			else
				stmt_error(self, NULL, "'STORED or RESOLVED' expected");
			return;
		}

		stmt_expect(self, KCOLUMN);
	}

	stmt_error(self, NULL, "'RENAME | ADD | DROP | SET | UNSET' expected");
}

void
parse_table_truncate(Stmt* self)
{
	// TRUNCATE [IF EXISTS] name
	auto stmt = ast_table_truncate_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;
}
