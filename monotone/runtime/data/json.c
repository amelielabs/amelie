
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>

void
json_init(Json* self)
{
	self->json      = NULL;
	self->json_size = 0;
	self->pos       = 0;
	self->buf       = &self->buf_data;
	buf_init(&self->buf_data);
}

void
json_free(Json* self)
{
	buf_free(&self->buf_data);
}

void
json_reset(Json* self)
{
	self->json      = NULL;
	self->json_size = 0;
	self->pos       = 0;
	buf_reset(self->buf);
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

	auto data = self->buf;
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
		// { "key" : value, "key", value }
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
		char* end;
		float value_fp = strtof(self->json + self->pos, &end);
		if (errno == ERANGE)
			return -1;
		self->pos += (end - (self->json + self->pos));
		if (unlikely(self->pos > self->json_size))
			return -1;
		if (negative)
			value_fp = -value_fp;
		encode_float(data, value_fp);
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
	self->buf       = &self->buf_data;
	int rc;
	rc = json_read(self, 0);
	if (unlikely(rc == -1))
		error("%s", "json parse error");
	assert(self->buf->position <= self->buf->end);
}
