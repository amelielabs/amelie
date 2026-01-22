
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
heap_create(Heap*     self,
            File*     file,
            Storage*  storage,
            Id*       id,
            int       state,
            Encoding* encoding)
{
	// prepare encoder
	Encoder encoder;
	encoder_init(&encoder);
	defer(encoder_free, &encoder);
	encoder_open(&encoder, encoding);

	// prepare header (including buckets)
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	auto header = self->header;
	if (encoder.compression)
		header->compression = encoder.compression->iface->id;
	else
		header->compression = COMPRESSION_NONE;
	if (encoder.encryption)
		header->encryption = encoder.encryption->iface->id;
	else
		header->encryption = CIPHER_NONE;
	header->crc = runtime()->crc(0, &header->magic, size - sizeof(uint32_t));

	// create heap file
	storage_create(storage, file, id, state);

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
		encoder_begin(&encoder);
		encoder_add(&encoder, page_data, page_size);
		encoder_encode(&encoder);
		page_data = encoder_iov(&encoder)->iov_base;
		page_size = encoder_iov(&encoder)->iov_len;
		page_header->size_compressed = page_size;

		// calculate page crc
		if (storage->config->crc)
			page_header->crc = runtime()->crc(0, page_data, page_size);

		file_write(file, page_header, sizeof(PageHeader));
		file_write(file, page_data, page_size);
	}
}

void
heap_open(Heap*     self,
          Storage*  storage,
          Id*       id,
          int       state,
          Encoding* encoding)
{
	// open heap file
	File file;
	file_init(&file);
	defer(file_close, &file);
	storage_open(storage, &file, id, state);

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
	Encoder encoder;
	encoder_init(&encoder);
	defer(encoder_free, &encoder);
	encoder_open(&encoder, encoding);
	encoder_set_encryption(&encoder, header->encryption);
	encoder_set_compression(&encoder, header->compression);

	// read pages
	auto page_mgr = &self->page_mgr;
	for (auto i = 0ul; i < header->count; i++)
	{
		auto page = page_mgr_allocate(page_mgr);
		file_read(&file, page->pointer, sizeof(PageHeader));
		auto page_header = (PageHeader*)page->pointer;

		// decode page or read page as is
		if (encoder_active(&encoder))
		{
			// read into buffer
			auto buf = buf_create();
			defer_buf(buf);
			file_read_buf(&file, buf, page_header->size_compressed);

			// validate crc
			if (storage->config->crc)
			{
				crc = runtime()->crc(0, buf->start, buf_size(buf));
				if (crc != page_header->crc)
					error("heap: file '%s' page crc mismatch",
					      str_of(&file.path));
			}

			encoder_decode(&encoder, page->pointer + sizeof(PageHeader),
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
			if (storage->config->crc)
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
