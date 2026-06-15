
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
	StorageHeader header =
	{
		.crc         = 0,
		.magic       = STORAGE_MAGIC,
		.version     = STORAGE_VERSION,
		.type        = self->type,
		.compression = encoder_compression(&ec),
		.size_page   = self->size_page,
		.size_meta   = meta_size,
		.count       = self->list_count
	};
	header.crc = runtime()->crc(0, &header.magic, sizeof(header) - sizeof(uint32_t));
	if (meta_size > 0)
		header.crc = runtime()->crc(header.crc, meta, meta_size);

	// create storage file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_create(&file, path);

	// write header
	file_write(&file, &header, sizeof(header));

	// write meta
	if (meta_size > 0)
		file_write(&file, meta, meta_size);

	// write pages
	for (int i = 0; i < self->list_count; i++)
	{
		auto page      = storage_at(self, i);
		auto page_data = page->data;
		auto page_size = page->position - sizeof(Page);

		// compress
		encoder_reset(&ec);
		encoder_add(&ec, page_data, page_size);
		encoder_encode(&ec);
		auto iov = encoder_iov(&ec);
		page->size_compressed = iov->iov_len;

		// calculate page crc
		if (opt_int_of(&config()->storage_crc))
			page->crc = encoder_iov_crc(&ec);

		page_data = iov->iov_base;
		page_size = iov->iov_len;
		file_write(&file, page, sizeof(Page));
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
	StorageHeader header;
	file_read(&file, &header, sizeof(header));

	// validate header magic
	if (header.magic != STORAGE_MAGIC)
		error("storage: file '{str}' has invalid header", &file.path);

	// validate version
	if (header.version != STORAGE_VERSION)
		error("storage: file '{str}' has incompatible version", &file.path);

	// validate storage type
	if (header.type != type)
		error("storage: file '{str}' type mismatch", &file.path);

	// validate size
	uint32_t size = (sizeof(header) + header.size_meta) - sizeof(uint32_t);
	if (file.size < size)
		error("storage: file '{str}' has invalid size", &file.path);

	// read meta and validate crc
	uint32_t crc = runtime()->crc(0, &header.magic, sizeof(header) - sizeof(uint32_t));
	if (header.size_meta > 0)
	{
		file_read_buf(&file, meta, header.size_meta);
		crc = runtime()->crc(crc, meta->start, buf_size(meta));
	}
	if (crc != header.crc)
		error("storage: file '{str}' header crc mismatch", &file.path);

	// prepare storage
	self->id_first  = 0;
	self->id_seq    = 0;
	self->size_page = header.size_page;
	self->type      = type;

	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));
	encoder_set_compression(&ec, header.compression);

	// read pages
	for (auto i = 0ul; i < header.count; i++)
	{
		auto page = page_allocate(header.size_page);
		errdefer(page_free, page);
		file_read(&file, page, sizeof(Page));

		// decode page or read page as is
		if (encoder_active(&ec))
		{
			// read into buffer
			auto buf = buf_create();
			defer_buf(buf);
			file_read_buf(&file, buf, page->size_compressed);

			// validate crc
			if (opt_int_of(&config()->storage_crc))
			{
				crc = runtime()->crc(0, buf->start, buf_size(buf));
				if (crc != page->crc)
					error("storage: file '{str}' page crc mismatch", &file.path);
			}

			encoder_decode(&ec, page->data,
			               page->position - sizeof(Page),
			               buf->start,
			               buf_size(buf));
		} else
		{
			// read into page
			auto page_data = page->data;
			auto page_size = page->position - sizeof(Page);
			file_read(&file, page_data, page_size);

			// validate crc
			if (opt_int_of(&config()->storage_crc))
			{
				crc = runtime()->crc(0, page_data, page_size);
				if (crc != page->crc)
					error("storage: file '{str}' page crc mismatch", &file.path);
			}
		}

		// register page
		storage_import(self, page);
	}

	// advance sequence id for a next page
	self->id_seq++;

	return file.size;
}
