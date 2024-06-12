#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Ht Ht;

struct Ht
{
	Row**    table;
	uint64_t count;
	uint64_t size;
	Def*     def;
	Row      deleted;
};

static inline void
ht_init(Ht* self)
{
	self->table = NULL;
	self->count = 0;
	self->size  = 0;
	self->def   = NULL;
	memset(&self->deleted, 0, sizeof(self->deleted));
}

static inline void
ht_free_rows(Ht* self)
{
	if (self->count == 0)
		return;
	auto table = self->table;
	for (uint64_t pos = 0; pos < self->size; pos++)
	{
		auto row = table[pos];
		if (!row || row == &self->deleted)
			continue;
		row_free(row);
	}
}

static inline void
ht_free(Ht* self)
{
	if (self->table)
	{
		ht_free_rows(self);
		so_free(self->table);
	}
	self->table = NULL;
	self->count = 0;
	self->size  = 0;
	self->def   = NULL;
}

static inline bool
ht_is_full(Ht* self)
{
	return self->count > self->size / 2;
}

static inline void
ht_create(Ht* self, Def* def, size_t size)
{
	size_t allocated = sizeof(Row*) * size;
	self->def   = def;
	self->size  = size;
	self->table = so_malloc(allocated);
	memset(self->table, 0, allocated);
}

hot static inline Row*
ht_set(Ht* self, Row* row)
{
	auto     table = self->table;
	uint64_t start = row->hash % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (!ref || ref == &self->deleted)
		{
			table[pos] = row;
			self->count++;
			return NULL;
		}
		if (ref->hash == row->hash && !compare(self->def, ref, row))
		{
			self->table[pos] = row;
			return ref;
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
ht_delete(Ht* self, Row* key)
{
	auto     table = self->table;
	uint64_t start = key->hash % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (! ref)
			break;
		if (ref != &self->deleted)
		{
			if (ref->hash == key->hash && !compare(self->def, ref, key))
			{
				table[pos] = &self->deleted;
				self->count--;
				return ref;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	return NULL;
}

hot static inline Row*
ht_get(Ht* self, Row* key, uint64_t* at)
{
	auto     table = self->table;
	uint64_t start = key->hash % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (! ref)
		{
			*at = pos;
			return NULL;
		}

		if (ref == &self->deleted)
		{
			*at = pos;
		} else
		{
			if (ref->hash == key->hash && !compare(self->def, ref, key))
			{
				*at = pos;
				return ref;
			}
		}

		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
ht_next(Ht* self, uint64_t* at)
{
	uint64_t pos = *at;
	for (; pos < self->size; pos++)
	{
		auto ref = self->table[pos];
		if (!ref || ref == &self->deleted)
			continue;

		// set to next
		*at = pos;
		return ref;
	}

	*at = UINT64_MAX;
	return NULL;
}
