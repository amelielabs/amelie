
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

Page*
page_allocate(uint32_t size)
{
	auto prot = PROT_READ|PROT_WRITE;
	auto pointer = mmap(NULL, size, prot, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (unlikely(pointer == MAP_FAILED))
		error_system();

	Page* self = pointer;
	memset(self, 0, sizeof(Page));
	self->changed       = true;
	self->size          = size;
	self->position      = sizeof(Page);
	self->position_last = self->position;
	return self;
}

void
page_free(Page* self)
{
	vfs_munmap(self, self->size);
}

Page*
page_load(Id* id, uint64_t checkpoint)
{
	char path[PATH_MAX];
	id_path(id, path, checkpoint, false);

	// open file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);

	// read header
	Page header;
	file_read(&file, &header, sizeof(header));

	// validate version
	if (header.version != 0)
		error("storage: file '{str}' has incompatible version", &file.path);

	// validate header crc
	uint32_t crc = runtime()->crc(0, &header.crc_data, sizeof(header) - sizeof(uint32_t));
	if (crc != header.crc)
		error("storage: file '{str}' header crc mismatch", &file.path);

	// validate size
	uint64_t size;
	if (header.size_compressed > 0)
		size = sizeof(header) + header.size_compressed;
	else
		size = sizeof(header) + header.position;
	if (file.size != size)
		error("storage: file '{str}' header size mismatch", &file.path);

	// allocate page
	auto self = page_allocate(header.size);
	errdefer(page_free, self);

	memcpy(self, &header, sizeof(header));

	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));
	encoder_set_compression(&ec, header.compression);

	// decode page or read page as is
	if (encoder_active(&ec))
	{
		// read into buffer
		auto buf = buf_create();
		defer_buf(buf);
		file_read_buf(&file, buf, header.size_compressed);

		// validate crc
		if (opt_int_of(&config()->storage_crc))
		{
			crc = runtime()->crc(0, buf->start, buf_size(buf));
			if (crc != header.crc_data)
				error("storage: file '{str}' data crc mismatch", &file.path);
		}

		encoder_decode(&ec, self->data,
					   self->position - sizeof(Page),
		               buf->start,
		               buf_size(buf));
	} else
	{
		// read into page
		auto page_data = self->data;
		auto page_size = self->position - sizeof(Page);
		file_read(&file, page_data, page_size);

		// validate crc
		if (opt_int_of(&config()->storage_crc))
		{
			crc = runtime()->crc(0, page_data, page_size);
			if (crc != header.crc_data)
				error("storage: file '{str}' data crc mismatch", &file.path);
		}
	}

	return self;
}

size_t
page_save(Page* self, uint64_t checkpoint)
{
	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));

	Page header;
	memcpy(&header, self, sizeof(Page));

	char path[PATH_MAX];
	id_path(&self->id, path, checkpoint, true);

	// create file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_create(&file, path);

	// write header (incomplete)
	file_write(&file, &header, sizeof(header));

	// compress page
	auto page_data = self->data;
	auto page_size = self->position - sizeof(Page);
	encoder_add(&ec, page_data, page_size);
	encoder_encode(&ec);
	auto iov = encoder_iov(&ec);

	// write page
	page_data = iov->iov_base;
	page_size = iov->iov_len;
	file_write(&file, page_data, page_size);

	// update header
	header.compression     = encoder_compression(&ec);
	header.size_compressed = iov->iov_len;
	if (opt_int_of(&config()->storage_crc))
		header.crc_data = encoder_iov_crc(&ec);
	header.crc = runtime()->crc(0, &header.crc_data, sizeof(header) - sizeof(uint32_t));
	file_pwrite(&file, &header, sizeof(header), 0);

	// sync
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	return file.size;
}
