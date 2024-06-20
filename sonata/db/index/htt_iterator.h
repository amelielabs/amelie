#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct HttIterator HttIterator;

struct HttIterator
{
	Row*     current;
	Ht*      current_ht;
	uint64_t pos;
	Htt*     ht;
};

static inline bool
htt_iterator_open(HttIterator* self, Htt* ht, RowKey* key)
{
	self->current    = NULL;
	self->current_ht = NULL;
	self->pos        = 0;
	self->ht         = ht;

	// point-lookup
	if (key)
	{
		self->current_ht = ht->current;
		self->current = ht_get(ht->current, key, &self->pos);
		if (self->current)
			return true;

		if (ht->rehashing)
		{
			self->current_ht = ht->prev;
			self->current = ht_get(ht->prev, key, &self->pos);
			if (self->current)
				return true;
		}

		return false;
	}

	// full scan
	if (ht->rehashing)
	{
		self->current_ht = self->ht->current;
		self->current = ht_next(self->current_ht, &self->pos);
		if (! self->current)
		{
			self->pos        = 0;
			self->current_ht = self->ht->prev;
			self->current    = ht_next(self->current_ht, &self->pos);
		}
	} else
	{
		if (ht->current->count == 0)
			return false;
		self->current_ht = self->ht->current;
		self->current = ht_next(self->current_ht, &self->pos);
	}
	return false;
}

static inline void
htt_iterator_open_at(HttIterator* self, Htt* ht, uint64_t pos)
{
	self->current    = ht_at(ht->current, pos)->row;
	self->current_ht = ht->current;
	self->pos        = pos;
	self->ht         = ht;
}

static inline bool
htt_iterator_has(HttIterator* self)
{
	return self->current != NULL;
}

static inline Row*
htt_iterator_at(HttIterator* self)
{
	return self->current;
}

static inline void
htt_iterator_next(HttIterator* self)
{
	if (self->current == NULL)
		return;
	auto ht = self->ht;

	self->pos++;
	self->current = ht_next(self->current_ht, &self->pos);
	if (self->current)
		return;

	if (! ht->rehashing)
		return;

	if (self->current_ht == self->ht->prev)
		return;
	self->pos        = 0;
	self->current_ht = self->ht->prev;
	self->current    = ht_next(self->current_ht, &self->pos);
}

static inline Row*
htt_iterator_replace(HttIterator* self, Row* row)
{
	uint8_t  key_data[self->ht->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->ht->keys, &hash);

	auto current = self->current;
	assert(current);
	self->current = row;
	ht_copy(self->current_ht, ht_at(self->current_ht, self->pos), key);
	return current;
}

static inline Row*
htt_iterator_delete(HttIterator* self)
{
	// keeping current as is
	auto current = self->current;
	auto current_ht = self->current_ht;
	ht_at(self->current_ht, self->pos)->row = HT_DELETED;
	current_ht->count--;
	return current;
}

static inline void
htt_iterator_reset(HttIterator* self)
{
	self->current    = NULL;
	self->current_ht = NULL;
	self->pos        = 0;
	self->ht         = NULL;
}

static inline void
htt_iterator_close(HttIterator* self)
{
	htt_iterator_reset(self);
}

static inline void
htt_iterator_init(HttIterator* self)
{
	self->current    = NULL;
	self->current_ht = NULL;
	self->pos        = 0;
	self->ht         = NULL;
}
