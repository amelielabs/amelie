
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>

void
heap_create(Heap* self, File* file, Id* id, int state)
{
	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, id->tier);

	// prepare header (including buckets)
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	auto header = self->header;
	header->compression = encoder_compression(&ec);
	header->encryption  = encoder_encryption(&ec);
	header->crc = runtime()->crc(0, &header->magic, size - sizeof(uint32_t));

	// create heap file
	id_create(id, file, state);

	// write header
	file_write(file, header, size);

	// write pages
	auto page_mgr = &self->page_mgr;
	for (int i = 0; i < page_mgr->list_count; i++)
	{
		auto page = page_mgr_at(page_mgr, i);
		auto page_header = (PageHeader*)page->pointer;
		auto page_data = page->pointer + sizeof(PageHeader);
		auto page_size = page_header->size - sizeof(PageHeader);

		// compress and encrypt
		encoder_reset(&ec);
		encoder_add(&ec, page_data, page_size);
		encoder_encode(&ec);
		auto iov = encoder_iov(&ec);
		page_header->size_compressed = iov->iov_len;

		// calculate page crc
		if (opt_int_of(&config()->storage_crc))
			page_header->crc = encoder_iov_crc(&ec);

		page_data = iov->iov_base;
		page_size = iov->iov_len;
		file_write(file, page_header, sizeof(PageHeader));
		file_write(file, page_data, page_size);
	}
}

void
heap_open(Heap* self, Id* id, int state)
{
	// open heap file
	File file;
	file_init(&file);
	defer(file_close, &file);
	id_open(id, &file, state);

	// read header
	auto header = self->header;
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	file_read(&file, header, size);

	// validate header magic
	if (header->magic != HEAP_MAGIC)
		error("heap: file '%s' has invalid header",
		      str_of(&file.path));

	// validate header crc
	uint32_t crc = runtime()->crc(0, &header->magic, size - sizeof(uint32_t));
	if (crc != header->crc)
		error("heap: file '%s' header crc mismatch",
		      str_of(&file.path));

	// validate version
	if (header->version != HEAP_VERSION)
		error("heap: file '%s' has incompatible version",
		      str_of(&file.path));

	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, id->tier);
	encoder_set_encryption(&ec, header->encryption);
	encoder_set_compression(&ec, header->compression);

	// read pages
	auto page_mgr = &self->page_mgr;
	for (auto i = 0ul; i < header->count; i++)
	{
		auto page = page_mgr_allocate(page_mgr);
		file_read(&file, page->pointer, sizeof(PageHeader));
		auto page_header = (PageHeader*)page->pointer;

		// decode page or read page as is
		if (encoder_active(&ec))
		{
			// read into buffer
			auto buf = buf_create();
			defer_buf(buf);
			file_read_buf(&file, buf, page_header->size_compressed);

			// validate crc
			if (opt_int_of(&config()->storage_crc))
			{
				crc = runtime()->crc(0, buf->start, buf_size(buf));
				if (crc != page_header->crc)
					error("heap: file '%s' page crc mismatch",
					      str_of(&file.path));
			}

			encoder_decode(&ec, page->pointer + sizeof(PageHeader),
			               page_header->size - sizeof(PageHeader),
			               buf->start,
			               buf_size(buf));
		} else
		{
			// read into page
			auto page_data = page->pointer + sizeof(PageHeader);
			auto page_size = page_header->size - sizeof(PageHeader);
			file_read(&file, page_data, page_size);

			// validate crc
			if (opt_int_of(&config()->storage_crc))
			{
				crc = runtime()->crc(0, page_data, page_size);
				if (crc != page_header->crc)
					error("heap: file '%s' page crc mismatch",
					      str_of(&file.path));
			}
		}
	}

	// restore last page position
	if (! header->count)
		return;
	auto page = page_mgr_at(page_mgr, header->count - 1);
	self->page_header = (PageHeader*)page->pointer;
	self->last = page_mgr_pointer_of(page_mgr, header->count - 1,
	                                 self->page_header->last);
}
