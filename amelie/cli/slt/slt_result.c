
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_slt.h>
#include <openssl/evp.h>

static void
slt_result_prepare(SltResult* self, uint8_t** pos)
{
	auto buf = &self->result;
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		if (json_is_array(*pos))
		{
			slt_result_prepare(self, pos);
			continue;
		}

		auto value = (SltResultValue*)buf_emplace(&self->result_index, sizeof(SltResultValue));
		value->offset = buf_size(&self->result);
		value->size   = 0;

		if (json_is_bool(*pos))
		{
			buf_printf(buf, "%d\n", **pos == JSON_TRUE);
		} else
		if (json_is_integer(*pos))
		{
			int64_t value;
			json_read_integer(pos, &value);
			buf_printf(buf, "%" PRIi64 "\n", value);
		} else
		if (json_is_real(*pos))
		{
			double value;
			json_read_real(pos, &value);
			buf_printf(buf, "%.3f\n", value);
		} else
		if (json_is_string(*pos))
		{
			// todo: print control chars as @
			Str value;
			json_read_string(pos, &value);
			if (str_empty(&value))
				buf_printf(buf, "(empty)\n");
			else
				buf_printf(buf, "%.*s\n", (int)str_size(&value),
				           str_of(&value));
		} else
		if (json_is_null(*pos))
		{
			buf_printf(buf, "NULL\n");
		} else {
			error("unsupported type in result");
		}

		value->size = buf_size(&self->result) - value->offset;
		self->count++;
	}
}

static inline void
slt_md5(Buf* input, char output[33])
{
	auto ctx = EVP_MD_CTX_new();
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int  digest_len;
	EVP_DigestInit_ex2(ctx, EVP_md5(), NULL);
	EVP_DigestUpdate(ctx, buf_cstr(input), buf_size(input));
	EVP_DigestFinal_ex(ctx, digest, &digest_len);
	EVP_MD_CTX_free(ctx);
    const char* hex = "0123456789abcdef";
	for (unsigned int i = 0, j = 0; i < digest_len; i++)
	{
        output[j++] = hex[(digest[i] >> 4) & 0x0F];
        output[j++] = hex[(digest[i]) & 0x0F];
	}
	output[32] = 0;
}

void
slt_result_create(SltResult* self,
                  SltSort    sort,
                  int        threshold,
                  int        columns,
                  Str*       data)
{
	self->sort      = sort;
	self->threshold = threshold;
	self->columns   = columns;

	// convert json result into text
	auto json = &self->json;
	json_reset(json);
	json_parse(json, data, NULL);
	auto pos = json->buf->start;
	slt_result_prepare(self, &pos);

	// convert result into md5 string on the threshold reach
	if (self->count > self->threshold)
	{
		auto result = &self->result;
		char digest[33];
		slt_md5(result, digest);
		buf_reset(result);
		buf_printf(result, "%d values hashing to %s\n",
		           self->count, digest);
	}
}
