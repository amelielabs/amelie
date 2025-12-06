
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
	auto    compression_type = &config()->checkpoint_compression;
	uint8_t compression = COMPRESSION_NONE;
	if (str_is(&compression_type->string, "zstd", 4))
		compression = COMPRESSION_ZSTD;
	else
	if (! str_is(&compression_type->string, "none", 4))
		error("heap: unknown compression type");

	auto cp = compression_create(&compression_zstd);
	defer(compression_free, cp);
	auto cp_buf = buf_create();
	defer_buf(cp_buf);

	// write header with buckets
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	self->header->lsn_file = self->header->lsn;
	self->header->compression = compression;
	self->header->crc =
		runtime()->crc(0, &self->header->magic, size - sizeof(uint32_t));
	file_write(&file, self->header, size);

	auto page_mgr = &self->page_mgr;
	for (int i = 0; i < page_mgr->list_count; i++)
	{
		auto     page = page_mgr_at(page_mgr, i);
		auto     page_header = (PageHeader*)page->pointer;
		uint8_t* page_data;
		int      page_size;
		if (compression != COMPRESSION_NONE)
		{
			buf_reset(cp_buf);
			compression_compress(cp, cp_buf, 0, page->pointer + sizeof(PageHeader),
			                     page_header->size - sizeof(PageHeader));
			page_header->size_compressed = buf_size(cp_buf);
			page_data = cp_buf->start;
			page_size = buf_size(cp_buf);
		} else {
			page_data = page->pointer + sizeof(PageHeader);
			page_size = page_header->size - sizeof(PageHeader);
		}

		if (opt_int_of(&config()->checkpoint_crc))
			page_header->crc = runtime()->crc(0, page_data, page_size);

		file_write(&file, page_header, sizeof(PageHeader));
		file_write(&file, page_data, page_size);
	}

	// sync
	if (opt_int_of(&config()->checkpoint_sync))
		file_sync(&file);
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
		error("heap: file '%s' has invalid header",
		      str_of(&file.path));

	// validate header crc
	uint32_t crc = runtime()->crc(0, &self->header->magic, size - sizeof(uint32_t));
	if (crc != self->header->crc)
		error("heap: file '%s' header crc mismatch",
		      str_of(&file.path));

	// validate version
	if (self->header->version != HEAP_VERSION)
		error("heap: file '%s' has incompatible version",
		      str_of(&file.path));

	// validate compression type
	if (self->header->compression > COMPRESSION_ZSTD)
		error("heap: file '%s' has incompatible compression type",
		      str_of(&file.path));

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
		if (self->header->compression != COMPRESSION_NONE)
		{
			// read and decompress page
			buf_reset(cp_buf);
			file_read_buf(&file, cp_buf, page_header->size_compressed);

			// validate crc
			if (opt_int_of(&config()->checkpoint_crc))
			{
				crc = runtime()->crc(0, cp_buf->start, buf_size(cp_buf));
				if (crc != page_header->crc)
					error("heap: file '%s' page crc mismatch",
					      str_of(&file.path));
			}

			compression_decompress(cp, cp_buf, page->pointer + sizeof(PageHeader),
			                       page_header->size - sizeof(PageHeader));
		} else
		{
			auto page_data = page->pointer + sizeof(PageHeader);
			auto page_size = page_header->size - sizeof(PageHeader);
			file_read(&file, page_data, page_size);

			// validate crc
			if (opt_int_of(&config()->checkpoint_crc))
			{
				crc = runtime()->crc(0, page_data, page_size);
				if (crc != page_header->crc)
					error("heap: file '%s' page crc mismatch",
					      str_of(&file.path));
			}
		}
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
