
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

// Based on public domain base64 implementation written by WEI Zhicheng

static const char
base64_encode_table[] =
{
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
	'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
	'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
	'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
	'w', 'x', 'y', 'z', '0', '1', '2', '3',
	'4', '5', '6', '7', '8', '9', '+', '/',
};

static const uint8_t
base64_decode_table[] =
{
	   255, 255, 255, 255, 255, 255, 255, 255,
	   255, 255, 255, 255, 255, 255, 255, 255,
	   255, 255, 255, 255, 255, 255, 255, 255,
	   255, 255, 255, 255, 255, 255, 255, 255,
	   255, 255, 255, 255, 255, 255, 255, 255,
	   255, 255, 255,  62, 255, 255, 255,  63,
	    52,  53,  54,  55,  56,  57,  58,  59,
	    60,  61, 255, 255, 255, 255, 255, 255,
	   255,   0,   1,  2,   3,   4,   5,    6,
	     7,   8,   9,  10,  11,  12,  13,  14,
	    15,  16,  17,  18,  19,  20,  21,  22,
	    23,  24,  25, 255, 255, 255, 255, 255,
	   255,  26,  27,  28,  29,  30,  31,  32,
	    33,  34,  35,  36,  37,  38,  39,  40,
	    41,  42,  43,  44,  45,  46,  47,  48,
	    49,  50,  51, 255, 255, 255, 255, 255
};

hot void
base64_encode(Buf* self, Str* str)
{
	buf_reserve(self, (((str_size(str)) + 2) / 3) * 4 + 1);

	auto state = 0;
	auto last  = (uint8_t)0;
	auto end   = (uint8_t*)str->end;
	for (auto pos = str_u8(str); pos < end; pos++)
	{
		switch (state) {
		case 0:
			buf_append_u8(self, base64_encode_table[(*pos >> 2) & 0x3F]);
			state = 1;
			break;
		case 1:
			buf_append_u8(self, base64_encode_table[((last & 0x03) << 4) | ((*pos >> 4) & 0x0F)]);
			state = 2;
			break;
		case 2:
			buf_append_u8(self, base64_encode_table[((last & 0x0F) << 2) | ((*pos >> 6) & 0x03)]);
			buf_append_u8(self, base64_encode_table[*pos & 0x3F]);
			state = 0;
			break;
		}
		last = *pos;
	}

	switch (state) {
	case 1:
		buf_append_u8(self, base64_encode_table[(last & 0x03) << 4]);
		buf_append_u8(self, '=');
		buf_append_u8(self, '=');
		break;
	case 2:
		buf_append_u8(self, base64_encode_table[(last & 0x0F) << 2]);
		buf_append_u8(self, '=');
		break;
	}
}

hot void
base64_decode(Buf* self, Str* str)
{
	buf_reserve(self, ceil((double)str_size(str) / 4) * 3);

	auto order     = 0;
	auto out_order = 0;
	auto out       = (uint8_t*)self->position;
	auto end       = (uint8_t*)str->end;
	for (auto pos = str_u8(str); pos < end; pos++)
	{
		if (*pos == '=')
			break;

		auto c = base64_decode_table[*pos];
		if (unlikely(c == 255))
			goto error;

		switch (order & 0x03) {
		case 0:
			out[out_order] = (c << 2) & 0xFF;
			break;
		case 1:
			out[out_order++] |= (c >> 4) & 0x03;
			out[out_order] = (c & 0x0F) << 4; 
			break;
		case 2:
			out[out_order++] |= (c >> 2) & 0x0F;
			out[out_order] = (c & 0x03) << 6;
			break;
		case 3:
			out[out_order++] |= c;
			break;
		}
		order++;
	}

	buf_advance(self, out_order);
	return;
error:
	error("failed to parse base64 string");
}
