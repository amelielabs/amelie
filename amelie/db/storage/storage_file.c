
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

size_t
storage_create(Storage* self, char* path, uint8_t* meta, int meta_size)
{
	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));

	// prepare header
	auto header = &self->header;
	header->compression = encoder_compression(&ec);
	header->size_meta   = meta_size;
	header->count       = self->list_count;
	header->crc = runtime()->crc(0, &header->magic, sizeof(StorageHeader) - sizeof(uint32_t));
	if (meta_size > 0)
		header->crc = runtime()->crc(header->crc, meta, meta_size);

	// create storage file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_create(&file, path);

	// write header
	file_write(&file, header, sizeof(StorageHeader));

	// write meta
	if (meta_size > 0)
		file_write(&file, meta, meta_size);

	// write pages
	for (int i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		auto page_header = (PageHeader*)page->pointer;
		auto page_data   = page->pointer + sizeof(PageHeader);
		auto page_size   = page_header->size - sizeof(PageHeader);

		// compress
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
		file_write(&file, page_header, sizeof(PageHeader));
		file_write(&file, page_data, page_size);
	}

	// sync
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	return file.size;
}

size_t
storage_open(Storage* self, char* path, int type, Buf* meta)
{
	// open storage file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);

	// read header
	auto header = &self->header;
	file_read(&file, header, sizeof(StorageHeader));

	// validate header magic
	if (header->magic != STORAGE_MAGIC)
		error("storage: file '{str}' has invalid header", &file.path);

	// validate version
	if (header->version != STORAGE_VERSION)
		error("storage: file '{str}' has incompatible version", &file.path);

	// validate storage type
	if (header->type != type)
		error("storage: file '{str}' type mismatch", &file.path);

	// validate size
	uint32_t size = (sizeof(StorageHeader) + header->size_meta) - sizeof(uint32_t);
	if (file.size < size)
		error("storage: file '{str}' has invalid size", &file.path);

	// read meta and validate crc
	uint32_t crc = runtime()->crc(0, &header->magic, sizeof(StorageHeader) - sizeof(uint32_t));
	if (header->size_meta > 0)
	{
		file_read_buf(&file, meta, header->size_meta);
		crc = runtime()->crc(crc, meta->start, buf_size(meta));
	}
	if (crc != header->crc)
		error("storage: file '{str}' header crc mismatch", &file.path);

	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));
	encoder_set_compression(&ec, header->compression);

	// read pages
	for (auto i = 0ul; i < header->count; i++)
	{
		auto page = storage_add(self);
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
					error("storage: file '{str}' page crc mismatch", &file.path);
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
					error("storage: file '{str}' page crc mismatch", &file.path);
			}
		}
	}

	return file.size;
}
