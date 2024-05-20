
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>

void
json_init(Json* self)
{
	self->json      = NULL;
	self->json_size = 0;
	self->pos       = 0;
	buf_init(&self->buf);
}

void
json_free(Json* self)
{
	buf_free(&self->buf);
}

void
json_reset(Json* self)
{
	self->json      = NULL;
	self->json_size = 0;
	self->pos       = 0;
	buf_reset(&self->buf);
}

hot static inline int
json_next(Json* self, int n)
{
	if (unlikely(self->pos + n > self->json_size))
		return -1;
	self->pos += n;
	while (self->pos < self->json_size && isspace(self->json[self->pos]))
		self->pos++;
	// eof
	if (self->pos == self->json_size)
		return 0;
	return self->json[self->pos];
}

hot static inline int
json_read(Json* self, int skip)
{
	int offset;
	int token = json_next(self, skip);
	if (unlikely(token <= 0))
		return token;

	auto data = &self->buf;
	int rc;
	switch (token) {
	case '[':
	{
		// [ v, v, v ]
		offset = buf_size(data);
		encode_array32(data, 0);

		// []
		rc = json_next(self, 1);
		if (unlikely(rc <= 0))
			return -1;
		if (rc == ']') {
			self->pos++;
			return token;
		}

		int count = 0;
		for (;;)
		{
			// [v, ...]
			rc = json_read(self, 0);
			if (unlikely(rc <= 0))
				return -1;
			count++;
			// ,]
			rc = json_next(self, 0);
			if (rc == ']') {
				self->pos++;
				break;
			}
			if (rc != ',')
				return -1;
			rc = json_next(self, 1);
			if (unlikely(rc <= 0))
				return -1;
		}

		// update array size
		uint8_t* pos_size = data->start + offset;
		data_write_array32(&pos_size, count);
		return token;
	}
	case '{':
	{
		// { "key" : value, ... }
		offset = buf_size(data);
		encode_map32(data, 0);

		// {}
		rc = json_next(self, 1);
		if (unlikely(rc <= 0))
			return -1;
		if (rc == '}') {
			self->pos++;
			return token;
		}

		int count = 0;
		for (;;)
		{
			// "key"
			rc = json_read(self, 0);
			if (unlikely(rc != '\"'))
				return -1;
			// :
			rc = json_next(self, 0);
			if (unlikely(rc != ':'))
				return -1;
			// value
			rc = json_read(self, 1);
			if (unlikely(rc == -1))
				return -1;
			count++;

			// ,}
			rc = json_next(self, 0);
			if (rc == '}') {
				self->pos++;
				break;
			}
			if (rc != ',')
				return -1;
			rc = json_next(self, 1);
			if (unlikely(rc <= 0))
				return -1;
		}

		// update map size
		uint8_t* pos_size = data->start + offset;
		data_write_map32(&pos_size, count);
		return token;
	}
	case '\"':
	{
		offset = ++self->pos;
		int slash = 0;
		for (; self->pos < self->json_size; self->pos++)
		{
			if (likely(!slash) ){
				if (self->json[self->pos] == '\\') {
					slash = 1;
					continue;
				}
				if (self->json[self->pos] == '"')
					break;
				continue;
			}
			slash = 0;
		}
		self->pos++;
		if (unlikely(self->pos > self->json_size))
			return -1;
		int size = self->pos - offset - 1;
		encode_raw(data, self->json + offset, size);
		return token;
	}
	case 'n':
		if (unlikely(self->pos + 3 >= self->json_size ||
		             memcmp(self->json + self->pos, "null", 4) != 0))
			return -1;
		self->pos += 4;
		encode_null(data);
		return token;
	case 't':
		if (unlikely(self->pos + 3 >= self->json_size ||
		             memcmp(self->json + self->pos, "true", 4) != 0))
			return -1;
		self->pos += 4;
		encode_bool(data, true);
		return token;

	case 'f':
		if (unlikely(self->pos + 4 >= self->json_size ||
		             memcmp(self->json + self->pos, "false", 5) != 0))
			return -1;
		self->pos += 5;
		encode_bool(data, false);
		return token;
	case '.':
	case '-': case '0': case '1' : case '2': case '3' : case '4':
	case '5': case '6': case '7' : case '8': case '9':
	{
		int start = self->pos;
		int negative = 0;
		if (self->json[start] == '-') {
			negative = 1;
			start++;
			self->pos++;
		}
		if (unlikely(self->pos == self->json_size))
			return -1;

		// float
		if (self->json[start] == '.')
			goto fp;

		int64_t value = 0;
		while (self->pos < self->json_size)
		{
			// float
			char symbol = self->json[self->pos];
			if (symbol == '.' || 
			    symbol == 'e' || 
			    symbol == 'E') {
				self->pos = start;
				goto fp;
			}
			if (! isdigit(symbol))
				break;
			value = (value * 10) + symbol - '0';
			self->pos++;
		}
		if (negative)
			value = -value;
		encode_integer(data, value);
		return token;
fp:;
		errno = 0;
		char*  end;
		double real = strtod(self->json + self->pos, &end);
		if (errno == ERANGE)
			return -1;
		self->pos += (end - (self->json + self->pos));
		if (unlikely(self->pos > self->json_size))
			return -1;
		if (negative)
			real = -real;
		encode_real(data, real);
		return token;
	}
	default:
		return -1;
	}
	return -1;
}

hot void
json_parse(Json* self, Str* string)
{
	self->json      = str_of(string);
	self->json_size = str_size(string);
	self->pos       = 0;
	int rc;
	rc = json_read(self, 0);
	if (unlikely(rc == -1))
		error("%s", "json parse error");
	assert(self->buf.position <= self->buf.end);
}

static void
json_export_as(Buf* data, bool pretty, int deep, uint8_t** pos)
{
	char buf[256];
	int  buf_len;
	switch (**pos) {
	case SO_NULL:
		data_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case SO_TRUE:
	case SO_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case SO_REAL32:
	case SO_REAL64:
	{
		double value;
		data_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%g", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case SO_INTV0 ... SO_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data, buf, buf_len);
		break;
	}
	case SO_STRINGV0 ... SO_STRING32:
	{
		// todo: quouting
		char* value;
		int   size;
		data_read_raw(pos, &value, &size);
		buf_write(data, "\"", 1);
		buf_write(data, value, size);
		buf_write(data, "\"", 1);
		break;
	}
	case SO_ARRAYV0 ... SO_ARRAY32:
	{
		int value;
		data_read_array(pos, &value);
		buf_write(data, "[", 1);
		while (value-- > 0)
		{
			json_export_as(data, pretty, deep, pos);
			if (value > 0)
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case SO_MAPV0 ... SO_MAP32:
	{
		int value;
		data_read_map(pos, &value);
		if (pretty)
		{
			buf_write(data, "{\n", 2);
			int i;
			while (value-- > 0)
			{
				for (i = 0; i < deep + 1; i++)
					buf_write(data, "  ", 2);
				// key
				json_export_as(data, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, pretty, deep + 1, pos);
				// ,
				if (value > 0)
					buf_write(data, ",\n", 2);
				else
					buf_write(data, "\n", 1);
			}
			for (i = 0; i < deep; i++)
				buf_write(data, "  ", 2);
			buf_write(data, "}", 1);
		} else
		{
			buf_write(data, "{", 1);
			while (value-- > 0)
			{
				// key
				json_export_as(data, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, pretty, deep + 1, pos);
				// ,
				if (value > 0)
					buf_write(data, ", ", 2);
			}
			buf_write(data, "}", 1);
		}
		break;
	}
	default:
		error_data();
		break;
	}
}

void
json_export(Buf* self, uint8_t** pos)
{
	json_export_as(self, false, 0, pos);
}

void
json_export_pretty(Buf* self, uint8_t** pos)
{
	json_export_as(self, true, 0, pos);
}
