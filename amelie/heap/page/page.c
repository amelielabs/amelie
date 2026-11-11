
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
#include <amelie_page.h>

void
page_save(Page* self, File* file)
{
	// prepare encoder
	Encoder ec;
	encoder_init(&ec);
	defer(encoder_free, &ec);
	encoder_open(&ec, opt_string_of(&config()->storage_compression));

	Page header;
	memcpy(&header, self, sizeof(Page));

	// write header (incomplete)
	file_write(file, &header, sizeof(header));

	// compress page
	auto page_data = self->data;
	auto page_size = self->position - sizeof(Page);
	encoder_add(&ec, page_data, page_size);
	encoder_encode(&ec);
	auto iov = encoder_iov(&ec);

	// write page
	page_data = iov->iov_base;
	page_size = iov->iov_len;
	file_write(file, page_data, page_size);

	// update header
	header.compression     = encoder_compression(&ec);
	header.size_compressed = iov->iov_len;
	if (opt_int_of(&config()->storage_crc))
		header.crc_data = encoder_iov_crc(&ec);
	header.crc = runtime()->crc(0, &header.crc_data, sizeof(header) - sizeof(uint32_t));
	file_write(file, &header, sizeof(header));
}

int64_t
page_load(Page* self, File* file, int64_t offset)
{
	// read header
	Page header;
	file_pread(file, &header, sizeof(header), offset);
	offset += sizeof(header);

	// validate header crc
	uint32_t crc = runtime()->crc(0, &header.crc_data, sizeof(header) - sizeof(uint32_t));
	if (crc != header.crc)
		error("storage: file '{str}' header crc mismatch", &file->path);

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
		file_pread_buf(file, buf, header.size_compressed, offset);
		offset += header.size_compressed;

		// validate crc
		if (opt_int_of(&config()->storage_crc))
		{
			crc = runtime()->crc(0, buf->start, buf_size(buf));
			if (crc != header.crc_data)
				error("storage: file '{str}' data crc mismatch", &file->path);
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
		file_pread(file, page_data, page_size, offset);
		offset += page_size;

		// validate crc
		if (opt_int_of(&config()->storage_crc))
		{
			crc = runtime()->crc(0, page_data, page_size);
			if (crc != header.crc_data)
				error("storage: file '{str}' data crc mismatch",
				      &file->path);
		}
	}

	return offset;
}
