
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>

void
heap_init(Heap* self, Uuid* uuid, Schema* schema)
{
	self->table      = NULL;
	self->table_size = 0;
	self->count      = 0;
	self->schema     = schema;
	self->uuid       = uuid;
	heap_commit_init(&self->commit);
}

void
heap_free(Heap* self)
{
	mn_free(self->table);
	heap_commit_free(&self->commit);
}

void
heap_create(Heap* self, int size)
{
	int allocated = sizeof(Row*) * size;
	self->table_size = size;
	self->table = mn_malloc(allocated);
	memset(self->table, 0, allocated);
}

hot static inline Row*
heap_unset(Heap* self, Row* key)
{
	uint32_t pos = key->hash % self->table_size;

	auto row  = self->table[pos];
	Row* prev = NULL;
	for (; row; row = row->prev)
	{
		if (row_compare(self->schema, row, key))
		{
			if (prev)
				prev->prev = row->prev;
			else
				self->table[pos] = row->prev;
			return row;
		}

		prev = row;
	}
	return NULL;
}

hot Row*
heap_set(Heap* self, Row* row)
{
	uint32_t pos = row->hash % self->table_size;

	// find and remove previous row
	auto prev = heap_unset(self, row);
	if (prev)
		self->count--;

	row->prev = self->table[pos];
	self->table[pos] = row;
	self->count++;
	return prev;
}

hot Row*
heap_delete(Heap* self, Row* key)
{
	// find and remove duplicate row from the chain
	auto prev = heap_unset(self, key);
	if (prev)
		self->count--;
	return prev;
}

hot Row*
heap_get(Heap* self, Row* key)
{
	uint32_t pos = key->hash % self->table_size;
	auto head = self->table[pos];
	for (;; head = head->prev)
		if (row_compare(self->schema, head, key))
			return head;
	return NULL;
}

void
heap_abort(Heap* self, Row* row, Row* prev)
{
	// remove update and put previous row back
	bool is_delete = row->is_delete;
	if (! is_delete)
	{
		auto row_ptr = heap_unset(self, row);
		assert(row_ptr == row);
	}
	row_free(row);

	if (prev)
	{
		uint32_t pos = prev->hash % self->table_size;
		prev->prev = self->table[pos];
		self->table[pos] = prev;

		// delete case with existing row
		if (is_delete)
			self->count++;
	}
}

hot void
heap_commit(Heap* self, Row* row, Row* prev, uint64_t lsn)
{
	// add row to the commit list and update lsn
	heap_commit_add(&self->commit, row, lsn);

	// remove previous version (if exists)
	if (prev)
		heap_commit_remove(&self->commit, prev);
}
