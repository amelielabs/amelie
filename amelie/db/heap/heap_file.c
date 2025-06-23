
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

	Iov iov;
	iov_init(&iov);
	defer(iov_free, &iov);

	// header with buckets
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	iov_add(&iov, self->header, size);

	self->header->crc = 0;
	self->header->crc_data = 0;

	auto page_mgr = &self->page_mgr;
	for (int i = 0; i < page_mgr->list_count; i++)
	{
		auto page = page_mgr_at(page_mgr, i);
		auto page_header = (PageHeader*)page->pointer;

		if (opt_int_of(&config()->checkpoint_crc))
			self->header->crc_data =
				global()->crc(self->header->crc_data, page->pointer,
				              page_header->size);

		iov_add(&iov, page->pointer, page_header->size);
	}

	// always calculate heap header crc
	self->header->crc =
		global()->crc(0, &self->header->crc_data, size - sizeof(uint32_t));

	file_writev(&file, iov_pointer(&iov), iov.iov_count);

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

	// validate version
	if (self->header->version != HEAP_VERSION)
		error("heap: file '%s' has incompatible version");

	// validate header crc
	uint32_t crc_data = 0;
	uint32_t crc = global()->crc(0, &self->header->crc_data, size - sizeof(uint32_t));
	if (crc != self->header->crc)
		error("heap: file '%s' header crc mismatch");

	// read pages
	auto page_mgr = &self->page_mgr;
	for (auto i = 0ul; i < self->header->count; i++)
	{
		auto page = page_mgr_allocate(page_mgr);
		file_read(&file, page->pointer, sizeof(PageHeader));
		auto page_header = (PageHeader*)page->pointer;
		file_read(&file, page->pointer + sizeof(PageHeader),
		          page_header->size - sizeof(PageHeader));
		if (opt_int_of(&config()->checkpoint_crc))
			crc_data = global()->crc(crc_data, page->pointer, page_header->size);
	}

	// validate data crc
	if (opt_int_of(&config()->checkpoint_crc))
		if (crc_data != self->header->crc_data)
			error("heap: file '%s' data crc mismatch");

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
