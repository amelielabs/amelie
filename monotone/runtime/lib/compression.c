
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <zstd.h>

void
compression_compress(Buf* buf, int compression,
                     Buf* buf_a,
                     Buf* buf_b)
{
	assert(compression == COMPRESSION_ZSTD);
	buf_reserve(buf, buf_size(buf_a) + buf_size(buf_b));

	ZSTD_CCtx* ctx = ZSTD_createCCtx();
	if (ctx == NULL)
		error("failed to create compression context");

	// a
	ZSTD_outBuffer out;
	out.dst  = buf->position;
	out.size = buf_size_unused(buf);
	out.pos  = 0;

	ZSTD_inBuffer in;
	in.src   = buf_a->start;
	in.size  = buf_size(buf_a);
	in.pos   = 0;

	ssize_t rc;
	rc = ZSTD_compressStream2(ctx, &out, &in, ZSTD_e_continue);
	assert(rc == 0);

	// b
	in.src   = buf_b->start;
	in.size  = buf_size(buf_b);
	in.pos   = 0;
	rc = ZSTD_compressStream2(ctx, &out, &in, ZSTD_e_end);
	assert(rc == 0);
	buf_advance(buf, out.pos);

	ZSTD_freeCCtx(ctx);
}

void
compression_decompress(Buf*  buf,
                       int   compression,
                       int   size_uncompressed,
                       char* data,
                       int   data_size)
{
	assert(compression == COMPRESSION_ZSTD);
	buf_reserve(buf, size_uncompressed);
	ssize_t rc;
	rc = ZSTD_decompress(buf->start, size_uncompressed, data, data_size);
	if (unlikely(ZSTD_isError(rc)))
		error("decompression failed");
	assert(rc == size_uncompressed);
}

int
compression_get(const char* name, int name_size)
{
	// off
	if (name_size == 3 && !strncmp(name, "off", 3))
		return COMPRESSION_OFF;

	// zstd
	if (name_size == 4 && !strncmp(name, "zstd", 4))
		return COMPRESSION_ZSTD;

	// unknown
	return -1;
}

bool
compression_is_set(int id)
{
	return id != COMPRESSION_OFF;
}
