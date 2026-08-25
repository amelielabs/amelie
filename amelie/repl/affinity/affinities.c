
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
#include <amelie_affinity.h>

void
affinities_init(Affinities* self)
{
	list_init(&self->list);
}

void
affinities_free(Affinities* self)
{
	list_foreach_safe(&self->list)
	{
		auto af = list_at(Affinity, link);
		affinity_free(af);
	}
	list_init(&self->list);
}

void
affinities_open(Affinities* self)
{
	auto afs = &state()->affinity;
	if (opt_json_empty(afs))
		return;
	auto pos = opt_json_of(afs);
	if (data_is_null(pos))
		return;

	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		auto af = affinity_read(&pos);
		list_append(&self->list, &af->link);
	}
}

static inline void
affinities_save(Affinities* self)
{
	// create dump
	auto buf = buf_create();
	defer_buf(buf);

	encode_array(buf);
	list_foreach(&self->list)
	{
		auto af = list_at(Affinity, link);
		affinity_write(af, buf, 0);
	}
	encode_array_end(buf);

	// update and save state
	opt_json_set_buf(&state()->affinity, buf);
}

void
affinities_set(Affinities* self, Affinity* af)
{
	auto match = affinities_find(self, &af->name);
	if (match)
	{
		list_unlink(&match->link);
		affinity_free(match);
	}
	list_append(&self->list, &af->link);

	// update state
	affinities_save(self);
}

Affinity*
affinities_find(Affinities* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto af = list_at(Affinity, link);
		if (str_compare(&af->name, name))
			return af;
	}
	return NULL;
}
