#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Htt Htt;

struct Htt
{
	Ht*      current;
	Ht*      prev;
	Ht       a, b;
	bool     rehashing;
	uint64_t rehashing_pos;
	Def*     def;
};

static inline void
htt_init(Htt* self)
{
	self->current       = NULL;
	self->prev          = NULL;
	self->rehashing     = false;
	self->rehashing_pos = 0;
	self->def           = NULL;
	ht_init(&self->a);
	ht_init(&self->b);
}

static inline void
htt_free(Htt* self)
{
	ht_free(&self->a);
	ht_free(&self->b);
}

static inline void
htt_create(Htt* self, Def* def)
{
	self->def = def;
	self->current = &self->a;
	ht_create(self->current, def, 256);
}

static inline void
htt_rehash_start(Htt* self)
{
	auto prev    = self->current;
	auto current = prev == &self->a ? &self->b : &self->a;

	// create new hash table and set as current
	ht_create(current, self->def, prev->size * 2);
	self->current       = current;
	self->prev          = prev;
	self->rehashing     = true;
	self->rehashing_pos = 0;
}

hot static inline bool
htt_rehash(Htt* self)
{
	auto prev = self->prev;
	while (self->rehashing_pos < prev->size)
	{
		auto row = prev->table[self->rehashing_pos];
		if (!row || row == &prev->deleted)
		{
			self->rehashing_pos++;
			continue;
		}
		ht_set(self->current, row);
		prev->table[self->rehashing_pos] = NULL;
		prev->count--;

		self->rehashing_pos++;
		return true;
	}

	// complete
	ht_free(prev);
	self->prev = NULL;
	self->rehashing = false;
	return false;
}

hot static inline Row*
htt_set(Htt* self, Row* row)
{
	Row* result = NULL;
	if (self->rehashing)
	{
		// return previous row
		result = ht_set(self->current, row);
		if (! result)
			result = ht_delete(self->prev, row);

		htt_rehash(self);
	} else
	{
		result = ht_set(self->current, row);
		if (ht_is_full(self->current))
		{
			htt_rehash_start(self);
			htt_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
htt_get_or_set(Htt* self, Row* row, uint64_t* ht_pos)
{
	Row* result = NULL;
	if (self->rehashing)
	{
		// get from current and previous tables
		uint64_t current_pos = 0;
		result = ht_get(self->current, row, &current_pos);
		if (! result)
		{
			uint64_t prev_pos = 0;
			result = ht_get(self->prev, row, &prev_pos);
			if (result)
			{
				// move to current
				self->prev->table[prev_pos] = NULL;
				self->prev->count--;
				self->current->table[current_pos] = result;
				self->current->count++;
			}
		}

		// set to current, if not exists
		if (! result)
		{
			self->current->table[current_pos] = row;
			self->current->count++;
		}
		*ht_pos = current_pos;

		htt_rehash(self);
	} else
	{
		// get from current table
		uint64_t current_pos = 0;
		result = ht_get(self->current, row, &current_pos);

		// set to current, if not exists
		if (! result)
		{
			self->current->table[current_pos] = row;
			self->current->count++;
		}
		*ht_pos = current_pos;

		if (ht_is_full(self->current))
		{
			htt_rehash_start(self);
			htt_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
htt_delete(Htt* self, Row* key)
{
	Row* result = NULL;
	if (self->rehashing)
	{
		result = ht_delete(self->current, key);
		if (! result)
			result = ht_delete(self->prev, key);

		htt_rehash(self);
	} else
	{
		result = ht_delete(self->current, key);
	}
	return result;
}

hot static inline Row*
htt_replace_by(Htt* self, Ht* ht, uint64_t pos, Row* with)
{
	// prev
	if (self->prev == ht)
	{
		// if hashtable points to the previous version, then
		// combine operation with rehashing
		auto table = self->prev->table;
		auto row = table[pos];
		table[pos] = NULL;
		self->prev->count--;
		ht_set(self->current, with);
		return row;
	}

	// current
	auto table = self->current->table;
	auto row = table[pos];
	table[pos] = with;
	return row;
}

hot static inline Row*
htt_delete_by(Htt* self, Ht* ht, uint64_t pos)
{
	// prev
	if (self->prev == ht)
	{
		// if hashtable points to the previous version, then
		// combine operation with rehashing
		auto table = self->prev->table;
		auto row = table[pos];
		table[pos] = NULL;
		self->prev->count--;
		return row;
	}

	// current
	auto table = self->current->table;
	auto row = table[pos];
	table[pos] = &self->current->deleted;
	self->current->count--;
	return row;
}
