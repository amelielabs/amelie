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
htt_iterator_open(HttIterator* self, Htt* ht, Row* key)
{
	self->current    = NULL;
	self->current_ht = NULL;
	self->pos        = 0;
	self->ht         = ht;

	// todo: key support
	(void)key;

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
	self->current    = ht->current->table[pos];
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
	assert(self->current);
	self->current = row;
	return htt_replace_by(self->ht, self->current_ht, self->pos, row);
}

static inline Row*
htt_iterator_delete(HttIterator* self)
{
	// keeping current as is
	return htt_delete_by(self->ht, self->current_ht, self->pos);
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
