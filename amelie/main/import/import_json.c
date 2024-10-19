
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_import.h>

static inline bool
import_skip(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return (*pos == end);
}

hot static inline char*
import_value(Import* self, char* pos, char* end, uint32_t* offset)
{
	// read json value
	*offset = buf_size(&self->json.buf_data);
	Str in;
	str_set(&in, pos, end - pos);
	json_set_time(&self->json, self->dtr->local->timezone, self->dtr->local->time_us);
	json_parse(&self->json, &in, NULL);
	return self->json.pos;
}

hot static char*
import_row(Import* self, char* pos, char* end, uint32_t* hash)
{
	auto table   = self->table;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

	// [value, ...]
	if (import_skip(&pos, end) || *pos != '[')
		error("JSON array expected");
	pos++;

	auto data = &self->json.buf_data;
	encode_array(data);

	// prepare column list
	auto list = (Column**)(self->columns.start);
	int  list_pos = 0;

	// set next serial value
	uint64_t serial = serial_next(&table->serial);

	list_foreach(&columns->list)
	{
		auto     column = list_at(Column, link);
		auto     separator = false;
		auto     separator_last = false;

		uint32_t offset;
		if (self->columns_has)
		{
			if (list_pos < self->columns_count && list[list_pos]->order == column->order)
			{
				// parse and encode json value which matches column list
				pos = import_value(self, pos, end, &offset);

				list_pos++;
				separator = true;
			} else
			{
				// null or default
				offset = buf_size(&self->json.buf_data);

				// GENERATED
				auto cons = &column->constraint;
				if (cons->generated == GENERATED_SERIAL)
				{
					encode_integer(&self->json.buf_data, serial);
				} else
				if (cons->generated == GENERATED_RANDOM)
				{
					auto value = random_generate(global()->random) % cons->modulo;
					encode_integer(&self->json.buf_data, value);
				} else
				{
					// use DEFAULT (NULL by default)
					buf_write(&self->json.buf_data, cons->value.start, buf_size(&cons->value));
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
			pos = import_value(self, pos, end, &offset);

			separator = true;
			separator_last = list_is_last(&columns->list, &column->link);
		}

		// ] or ,
		if (separator)
		{
			if (unlikely(import_skip(&pos, end)))
				error("failed to parse JSON array");
			if (unlikely(*pos != ']' && *pos != ','))
				error("failed to parse JSON array");
			if (unlikely((*pos == ']' && !separator_last) ||
			             (*pos == ',' &&  separator_last)))
				error("array expected to have %d columns",
				      columns->list_count);
			pos++;
		}

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			uint8_t* ref = self->json.buf_data.start + offset;
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
				uint8_t* ref = self->json.buf_data.start + offset;
				key_find(key, &ref);

				// hash key
				*hash = key_hash(*hash, ref);
			}
		}
	}

	encode_array_end(data);
	return pos;
}

hot void
import_json(Import* self)
{
	auto dtr   = self->dtr;
	auto table = self->table;

	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	// [[value, ...], ...]
	char* pos = buf_cstr(&self->request->content);
	char* end = pos + buf_size(&self->request->content);
	if (import_skip(&pos, end) || *pos != '[')
		error("JSON array expected");
	pos++;

	for (;;)
	{
		// [value, ...]
		uint32_t offset = buf_size(&self->json.buf_data);
		uint32_t hash   = 0;
		pos = import_row(self, pos, end, &hash);

		// map to node
		auto route = part_map_get(&table->part_list.map, hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_IMPORT);
			req->arg_import = &self->json.buf_data;
			req->arg_import_table = self->table;
			req->route = route;
			req_list_add(&self->req_list, req);
			map[route->order] = req;
		}

		// write offset to req->arg
		encode_integer(&req->arg, offset);

		// ] or ,
		if (import_skip(&pos, end))
			error("failed to parse JSON array");
		if (*pos == ',') {
			pos++;
			continue;
		}
		if (*pos == ']') {
			pos++;
			break;
		}
		error("failed to parse JSON array");
	}
}
