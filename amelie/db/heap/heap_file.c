
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>

uint64_t
heap_file_write(Heap* self, char* path)
{
	// create heap file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_create(&file, path);

	// prepare compession context
	auto cp = compression_create(&compression_zstd);
	defer(compression_free, cp);
	auto cp_buf = buf_create();
	defer_buf(cp_buf);

	// write header with buckets
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	self->header->compression = COMPRESSION_ZSTD;
	self->header->crc =
		global()->crc(0, &self->header->magic, size - sizeof(uint32_t));
	file_write(&file, self->header, size);

	auto page_mgr = &self->page_mgr;
	for (int i = 0; i < page_mgr->list_count; i++)
	{
		auto page = page_mgr_at(page_mgr, i);
		auto page_header = (PageHeader*)page->pointer;

		buf_reset(cp_buf);
		compression_compress(cp, cp_buf, 0, page->pointer + sizeof(PageHeader),
		                     page_header->size - sizeof(PageHeader));
		page_header->size_compressed = buf_size(cp_buf);
		if (opt_int_of(&config()->checkpoint_crc))
			page_header->crc = global()->crc(0, cp_buf->start, buf_size(cp_buf));

		file_write(&file, page_header, sizeof(PageHeader));
		file_write_buf(&file, cp_buf);
	}

	// todo: sync
	return file.size;
}

uint64_t
heap_file_read(Heap* self, char* path)
{
	// open heap file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);

	// read header
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	file_read(&file, self->header, size);

	// validate header magic
	if (self->header->magic != HEAP_MAGIC)
		error("heap: file '%s' has invalid header");

	// validate header crc
	uint32_t crc = global()->crc(0, &self->header->magic, size - sizeof(uint32_t));
	if (crc != self->header->crc)
		error("heap: file '%s' header crc mismatch");

	// validate version
	if (self->header->version != HEAP_VERSION)
		error("heap: file '%s' has incompatible version");

	// validate compression type
	if (self->header->compression > COMPRESSION_ZSTD)
		error("heap: file '%s' has incompatible compression type");

	// prepare compession context
	auto cp = compression_create(&compression_zstd);
	defer(compression_free, cp);
	auto cp_buf = buf_create();
	defer_buf(cp_buf);

	// read pages
	auto page_mgr = &self->page_mgr;
	for (auto i = 0ul; i < self->header->count; i++)
	{
		auto page = page_mgr_allocate(page_mgr);
		file_read(&file, page->pointer, sizeof(PageHeader));
		auto page_header = (PageHeader*)page->pointer;
		buf_reset(cp_buf);
		file_read_buf(&file, cp_buf, page_header->size_compressed);

		// validate crc
		if (opt_int_of(&config()->checkpoint_crc))
		{
			crc = global()->crc(0, cp_buf->start, buf_size(cp_buf));
			if (crc != page_header->crc)
				error("heap: file '%s' page crc mismatch");
		}

		// decompress page
		compression_decompress(cp, cp_buf, page->pointer + sizeof(PageHeader),
		                       page_header->size - sizeof(PageHeader));
	}

	// restore last page position
	if (self->header->count > 0)
	{
		auto page = page_mgr_at(page_mgr, self->header->count - 1);
		self->page_header = (PageHeader*)page->pointer;
		self->last = page_mgr_pointer_of(page_mgr, self->header->count - 1,
		                                 self->page_header->last);
	}

	return file.size;
}
