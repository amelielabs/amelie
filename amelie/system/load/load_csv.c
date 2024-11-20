
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_load.h>

#if 0
static inline bool
load_skip(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return (*pos >= end);
}

hot static inline char*
load_value(Load* self, Column* column, char* pos, char* end, uint32_t* offset)
{
	*offset = buf_size(&self->data);
	Str in;
	str_set(&in, pos, end - pos);
	Lex lex;
	lex_init(&lex, keywords);
	lex_start(&lex, &in);
	parse_value_for(&lex, self->dtr->local, &self->json, &self->data, column);
	return lex.pos;
}

hot static char*
load_row(Load* self, char* pos, char* end, uint32_t* hash)
{
	auto table   = self->table;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

	// [value[, ...]] CRLF | EOF
	auto data = &self->data;
	encode_array(data);

	// prepare column list
	auto list = (Column**)(self->columns.start);
	int  list_pos = 0;

	// set next serial value
	uint64_t serial = serial_next(&table->serial);

	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto separator = false;
		auto separator_last = false;

		uint32_t offset;
		if (self->columns_has)
		{
			if (list_pos < self->columns_count && list[list_pos]->order == column->order)
			{
				// parse and encode json value which matches column list
				pos = load_value(self, column, pos, end, &offset);

				list_pos++;
				separator = true;
			} else
			{
				// null or default
				offset = buf_size(data);

				// GENERATED
				auto cons = &column->constraint;
				if (cons->serial)
				{
					encode_integer(data, serial);
				} else
				if (cons->random)
				{
					auto value = random_generate(global()->random) % cons->random_modulo;
					encode_integer(data, value);
				} else
				{
					// use DEFAULT (NULL by default)
					buf_write(data, cons->value.start, buf_size(&cons->value));
				}
			}

			// force ] check
			if (list_pos >= self->columns_count)
			{
				separator = true;
				separator_last = true;
			}

		} else
		{
			// parse and encode json value
			pos = load_value(self, column, pos, end, &offset);

			separator = true;
			separator_last = list_is_last(&columns->list, &column->link);
		}

		// , or \r\n or eof
		if (separator)
		{
			if (separator_last)
			{
				// next row or eof
			} else
			{
				if (load_skip(&pos, end))
					error("failed to parse CSV array");
				if (unlikely(*pos != ','))
					error("array expected to have %d columns",
					      columns->list_count);
				pos++;
			}
		}

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			uint8_t* ref = data->start + offset;
			if (data_is_null(ref))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
		}

		// use primary key to calculate hash
		if (column->key)
		{
			list_foreach(&keys->list)
			{
				auto key = list_at(Key, link);
				if (key->column != column)
					continue;

				// find key path and validate data type
				uint8_t* ref = data->start + offset;
				key_find(key, &ref);

				// hash key
				*hash = key_hash(*hash, ref);
			}
		}
	}

	encode_array_end(data);
	return pos;
}

void
load_csv(Load* self)
{
	auto dtr   = self->dtr;
	auto table = self->table;

	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	// row per line

	// value[, ...] CRLF | EOF
	char* pos = buf_cstr(&self->request->content);
	char* end = pos + buf_size(&self->request->content);

	for (;;)
	{
		// eof
		if (load_skip(&pos, end))
			break;

		// [value[, ...]] CRLF | EOF
		uint32_t offset = buf_size(&self->data);
		uint32_t hash   = 0;
		pos = load_row(self, pos, end, &hash);

		// map to node
		auto route = part_map_get(&table->part_list.map, hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_LOAD);
			req->arg_buf = &self->data;
			req->arg_table = self->table;
			req->route = route;
			req_list_add(&self->req_list, req);
			map[route->order] = req;
		}

		// write offset to req->arg
		encode_integer(&req->arg, offset);
	}
}
#endif

hot void
load_csv(Load* self)
{
	(void)self;
}

