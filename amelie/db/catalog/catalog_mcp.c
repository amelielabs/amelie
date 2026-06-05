
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
catalog_mcp_schema(Columns* columns, Buf* buf)
{
	// {}
	encode_obj(buf);

	// type
	encode_raw(buf, "type", 4);
	encode_raw(buf, "object", 6);

	// properties {}
	encode_raw(buf, "properties", 10);
	encode_obj(buf);

	list_foreach(&columns->list)
	{
		// column
		auto column = list_at(Column, link);
		encode_str(buf, &column->name);

		// {}
		encode_obj(buf);
		switch (column->type) {
		case TYPE_BOOL:
			encode_raw(buf, "type", 4);
			encode_raw(buf, "boolean", 7);
			break;
		case TYPE_INT:
			encode_raw(buf, "type", 4);
			encode_raw(buf, "integer", 7);
			break;
		case TYPE_DOUBLE:
			encode_raw(buf, "type", 4);
			encode_raw(buf, "number", 6);
			break;
		case TYPE_JSON:
			// type is omitted, assuming all supported json types
			break;
		case TYPE_STRING:
		case TYPE_DATE:
		case TYPE_TIMESTAMP:
		case TYPE_INTERVAL:
		case TYPE_UUID:
			encode_raw(buf, "type", 4);
			encode_raw(buf, "string", 6);
			break;
		case TYPE_VECTOR:
		{
			encode_raw(buf, "type", 4);
			encode_raw(buf, "array", 5);

			// items {}
			encode_raw(buf, "items", 5);
			encode_obj(buf);
			encode_raw(buf, "type", 4);
			encode_raw(buf, "number", 6);
			encode_obj_end(buf);
			break;
		}
		default:
			 abort();
		}
		encode_obj_end(buf);
	}
	encode_obj_end(buf);

	// required []
	encode_raw(buf, "required", 8);
	encode_array(buf);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		encode_str(buf, &column->name);
	}
	encode_array_end(buf);

	encode_obj_end(buf);
}

void
catalog_mcp_tools(Catalog* self, Str* user, Buf* buf)
{
	// []
	encode_array(buf);

	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_UDF)
			continue;
		if (! str_compare(user, rel->user))
			continue;
		auto udf = udf_of(rel);

		// {}
		encode_obj(buf);

		// name
		encode_raw(buf, "name", 4);
		encode_str(buf, rel->name);

		// description
		encode_raw(buf, "description", 11);
		encode_str(buf, rel->description);

		// inputSchema
		encode_raw(buf, "inputSchema", 11);
		catalog_mcp_schema(&udf->config->args, buf);

		encode_obj_end(buf);
	}

	encode_array_end(buf);
}

void
catalog_mcp_resources(Catalog* self, Str* user, Buf* buf)
{
	// []
	encode_array(buf);

	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);

		// only udfs without arguments
		if (rel->type != REL_UDF)
			continue;

		auto udf = udf_of(rel);
		if (udf->config->args.count != 0)
			continue;
		if (! str_compare(user, rel->user))
			continue;

		// {}
		encode_obj(buf);

		// uri
		encode_raw(buf, "uri", 3);
		char uri[256];
		auto uri_size = format(uri, sizeof(uri), "amelie://{str}/{str}",
		                       rel->user, rel->name);
		encode_raw(buf, uri, uri_size);

		// name
		encode_raw(buf, "name", 4);
		encode_str(buf, rel->name);

		// description
		encode_raw(buf, "description", 11);
		encode_str(buf, rel->description);

		// mimeType
		encode_raw(buf, "mimeType", 8);
		encode_raw(buf, "text/plain", 10);

		encode_obj_end(buf);
	}

	encode_array_end(buf);
}
