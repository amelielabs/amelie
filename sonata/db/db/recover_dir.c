
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>

static inline int
snapshot_id_of(const char* name,
               uint64_t*   storage,
               uint64_t*   lsn,
               bool*       incomplete)
{
	// <storage_id>.<lsn>[.incomplete]
	*storage = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		*storage = (*storage * 10) + *name - '0';
		name++;
	}
	if (*name != '.')
		return -1;
	name++;
	*lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		*lsn = (*lsn * 10) + *name - '0';
		name++;
	}
	if (*name == '.')
	{
		if (! strcmp(name, ".incomplete"))
			*incomplete = true;
		return -1;
	}
	*incomplete = false;
	return 0;
}

static void
closedir_guard(DIR* self)
{
	closedir(self);
}

void
recover_dir(Db* self)
{
	// read storage directory, get a list of snapshots per storage
	auto path = config_directory();
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("snapshot: directory '%s' open error", path);
	guard(closedir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		uint64_t id_storage;
		uint64_t id_lsn;
		bool     incomplete;
		if (snapshot_id_of(entry->d_name, &id_storage, &id_lsn, &incomplete) == -1)
			continue;
		if (incomplete)
		{
			log("snapshot: removing incomplete snapshot: '%s/%s'", path, entry->d_name);
			fs_unlink("%s/%s", path, entry->d_name);
			continue;
		}

		// todo: remove all snapshot files > config.snapshot

		auto storage = table_mgr_find_storage(&self->table_mgr, id_storage);
		if (storage == NULL)
		{
			log("snapshot: unknown storage for file: '%s/%s'", path, entry->d_name);
			continue;
		}
		snapshot_mgr_add(&storage->snapshot_mgr, id_lsn);
	}
}
