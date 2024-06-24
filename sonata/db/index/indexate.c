
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>

hot void
indexate(Index* self, Index* primary)
{
	auto    keys = index_keys(self);
	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;

	auto it = index_iterator(primary);
	guard(iterator_close, it);
	iterator_open(it, NULL);
	while (iterator_has(it))
	{
		auto row = iterator_at(it);
		ref_create(key, row, keys);
		auto prev = index_ingest(self, key);
		if (unlikely(prev))
			error("index unique constraint violation");
		iterator_next(it);
	}
}
