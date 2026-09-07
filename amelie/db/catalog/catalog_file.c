
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_read(Catalog* self, char* path)
{
	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}", path);

	// prepare eval
	Eval eval;
	eval_init(&eval, self->iface_eval, self->iface_arg);
	eval_create(&eval);
	defer(eval_free, &eval);

	// parse and execute statements
	Str command;
	while (separator_read(&sep, &command))
	{
		eval_execute(&eval, &command);
		separator_advance(&sep);
	}
}

static void
catalog_dump(Rels* rels, RelType type, Buf* buf)
{
	list_foreach(&rels->list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != type)
			continue;
		if (rel->type == REL_USER && user_of(rel)->config->superuser)
			continue;
		describe_text(rel, buf, 0);
		buf_write(buf, "\n\n", 2);
	}
}

void
catalog_write(Catalog* self, char* path)
{
	// users, tables, clones, topics, subs, udfs
	auto buf = buf_create();
	catalog_dump(&self->users, REL_USER, buf);
	catalog_dump(&self->rels, REL_TABLE, buf);
	catalog_dump(&self->rels, REL_CLONE, buf);
	catalog_dump(&self->rels, REL_TOPIC, buf);
	catalog_dump(&self->rels, REL_SUBSCRIPTION, buf);
	catalog_dump(&self->rels, REL_UDF, buf);

	// create file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	if (! buf_empty(buf))
	{
		file_write_buf(&file, buf);
		if (opt_int_of(&config()->storage_sync))
			file_sync(&file);
	}
}
