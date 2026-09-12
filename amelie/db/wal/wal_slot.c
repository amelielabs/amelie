
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
#include <amelie_wal.h>

void
wal_attach(Wal* self, WalSlot* slot)
{
	assert(! slot->active);
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->active = true;
	spinlock_unlock(&self->lock);
}

void
wal_detach(Wal* self, WalSlot* slot)
{
	if (! slot->active)
		return;
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
	slot->active = false;
}

int
wal_slots(Wal* self, uint64_t* min)
{
	spinlock_lock(&self->lock);
	auto count = self->slots_count;
	list_foreach(&self->slots)
	{
		auto slot = list_at(WalSlot, link);
		auto lsn = atomic_u64_of(&slot->lsn);
		if (lsn < *min)
			*min = lsn;
	}
	spinlock_unlock(&self->lock);
	return count;
}

void
wal_snapshot(Wal* self, WalSlot* slot, Buf* data)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	// create wal slot to ensure listed files exists
	wal_slot_set(slot, 0);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->active = true;

	// [[path, size, mode], ...]
	encode_array(data);
	char path[PATH_MAX];
	for (auto file = self->files; file; file = file->next)
	{
		format(path, sizeof(path), "wal/{u64}", file->id);
		encode_basefile(data, path);
	}
	encode_array_end(data);
}

void
wal_backup(Buf* data, char* path_base)
{
	char path_backup[PATH_MAX];
	char path[PATH_MAX];

	// read wal files
	auto    pos = data->start;
	Str     path_relative;
	int64_t size;
	int64_t mode;

	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		// [path_relative, size, mode]
		decode_basefile(&pos, &path_relative, &size, &mode);

		format(path, sizeof(path), "{s}/{str}",
		       state_directory(), &path_relative);

		format(path_backup, sizeof(path_backup), "{s}/{str}",
		       path_base, &path_relative);

		// last file
		if (data_is_array_end(pos))
			break;

		// create a hardlink
		auto rc = link(path, path_backup);
		if (rc == -1)
			error_system();
	}

	// copy last file up to captured size
	File src;
	file_init(&src);
	defer(file_close, &src);
	file_open_rdonly(&src, path);

	File dst;
	file_init(&dst);
	defer(file_close, &dst);
	file_create(&dst, path_backup);

	file_copy(&src, &dst, size);
	// todo: sync
}
