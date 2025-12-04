
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_slt.h>
#include <openssl/evp.h>

static void
slt_result_convert(SltResult* self, uint8_t** pos)
{
	auto buf = &self->output;
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		if (json_is_array(*pos))
		{
			slt_result_convert(self, pos);
			continue;
		}

		// write value as a string
		auto offset = buf_size(&self->output);
		if (json_is_bool(*pos))
		{
			buf_printf(buf, "%d", **pos == JSON_TRUE);
		} else
		if (json_is_integer(*pos))
		{
			int64_t value;
			json_read_integer(pos, &value);
			buf_printf(buf, "%" PRIi64, value);
		} else
		if (json_is_real(*pos))
		{
			double value;
			json_read_real(pos, &value);
			buf_printf(buf, "%.3f", value);
		} else
		if (json_is_string(*pos))
		{
			// todo: print control chars as @
			Str value;
			json_read_string(pos, &value);
			if (str_empty(&value))
				buf_printf(buf, "(empty)");
			else
				buf_printf(buf, "%.*s", (int)str_size(&value),
				           str_of(&value));
		} else
		if (json_is_null(*pos))
		{
			json_read_null(pos);
			buf_printf(buf, "NULL");
		} else {
			error("unsupported type in result");
		}

		// \0
		buf_write(buf, "\0", 1);

		// add to the index
		auto value = (SltValue*)buf_emplace(&self->output_index, sizeof(SltValue));
		value->offset = offset;
		value->size   = buf_size(&self->output) - value->offset - 1;
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

static int
slt_result_sort_value(const void* a, const void* b, void* arg)
{
	SltResult* self = arg;
	const SltValue* av = a;
	const SltValue* bv = b;
	return strcmp((char*)self->output.start + av->offset,
	              (char*)self->output.start + bv->offset);
}

static int
slt_result_sort_row(const void* a, const void* b, void* arg)
{
	SltResult* self = arg;
	const SltValue* av = a;
	const SltValue* bv = b;
	for (auto i = 0; i < self->columns; i++)
	{
		auto rc = slt_result_sort_value(&av[i], &bv[i], arg);
		if (rc != 0)
			return rc;
	}
	return 0;
}

static void
slt_result_sort(SltResult* self, SltSort sort)
{
	auto index = (SltValue*)self->output_index.start;
	switch (sort) {
	case SLT_SORT_VALUE:
		qsort_r(index, self->count, sizeof(SltValue),
		        slt_result_sort_value, self);
		break;
	case SLT_SORT_ROW:
		qsort_r(index, self->count / self->columns, sizeof(SltValue) * self->columns,
		        slt_result_sort_row, self);
		break;
	default:
		return;
	}
}

static void
slt_result_write(SltResult* self)
{
	// write result using index
	auto result = &self->result;
	auto index  = (SltValue*)self->output_index.start;
	for (auto i = 0; i < self->count; i++)
	{
		buf_write(result, self->output.start + index[i].offset,
		          index[i].size);
		buf_write(&self->result, "\n", 1);
	}

	// create md5 hash of result (for threshold and labels)
	buf_reserve(&self->result_hash, 33);
	slt_md5(result, buf_cstr(&self->result_hash));

	// use hash result on the threshold reach
	if (self->count <= self->threshold)
		return;
	buf_reset(result);
	buf_printf(&self->result, "%d values hashing to %s\n", self->count,
	           buf_cstr(&self->result_hash));
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

	// convert and index json content into strings
	auto json = &self->json;
	json_reset(json);
	json_parse(json, data, NULL);
	auto pos = json->buf->start;
	slt_result_convert(self, &pos);

	// sort strings
	if (self->count > 0)
		slt_result_sort(self, sort);

	// write result
	slt_result_write(self);
}
