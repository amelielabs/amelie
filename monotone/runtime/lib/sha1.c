
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>

//
// Based on Public Domain Implementation by
// Steve Reid <steve@edmweb.com>
//

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

#if BYTE_ORDER == LITTLE_ENDIAN
# define blk0(i) (block->l[i] = (rol(block->l[i],24)&0xFF00FF00) \
    |(rol(block->l[i],8)&0x00FF00FF))
#else
# define blk0(i) block->l[i]
#endif
#define blk(i) (block->l[i&15] = rol(block->l[(i+13)&15]^block->l[(i+8)&15] \
    ^block->l[(i+2)&15]^block->l[i&15],1))

#define R0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define R3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define R4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);

static inline void
sha1_transform(uint32_t state[5], const uint8_t buffer[SHA1_BLOCK_LENGTH])
{
	uint32_t a, b, c, d, e;
	uint8_t  workspace[SHA1_BLOCK_LENGTH];

	union long16
	{
		uint8_t  c[64];
		uint32_t l[16];
	} *block = (union long16 *)workspace;

	memcpy(block, buffer, SHA1_BLOCK_LENGTH);

	a = state[0];
	b = state[1];
	c = state[2];
	d = state[3];
	e = state[4];

	R0(a,b,c,d,e, 0); R0(e,a,b,c,d, 1); R0(d,e,a,b,c, 2); R0(c,d,e,a,b, 3);
	R0(b,c,d,e,a, 4); R0(a,b,c,d,e, 5); R0(e,a,b,c,d, 6); R0(d,e,a,b,c, 7);
	R0(c,d,e,a,b, 8); R0(b,c,d,e,a, 9); R0(a,b,c,d,e,10); R0(e,a,b,c,d,11);
	R0(d,e,a,b,c,12); R0(c,d,e,a,b,13); R0(b,c,d,e,a,14); R0(a,b,c,d,e,15);
	R1(e,a,b,c,d,16); R1(d,e,a,b,c,17); R1(c,d,e,a,b,18); R1(b,c,d,e,a,19);
	R2(a,b,c,d,e,20); R2(e,a,b,c,d,21); R2(d,e,a,b,c,22); R2(c,d,e,a,b,23);
	R2(b,c,d,e,a,24); R2(a,b,c,d,e,25); R2(e,a,b,c,d,26); R2(d,e,a,b,c,27);
	R2(c,d,e,a,b,28); R2(b,c,d,e,a,29); R2(a,b,c,d,e,30); R2(e,a,b,c,d,31);
	R2(d,e,a,b,c,32); R2(c,d,e,a,b,33); R2(b,c,d,e,a,34); R2(a,b,c,d,e,35);
	R2(e,a,b,c,d,36); R2(d,e,a,b,c,37); R2(c,d,e,a,b,38); R2(b,c,d,e,a,39);
	R3(a,b,c,d,e,40); R3(e,a,b,c,d,41); R3(d,e,a,b,c,42); R3(c,d,e,a,b,43);
	R3(b,c,d,e,a,44); R3(a,b,c,d,e,45); R3(e,a,b,c,d,46); R3(d,e,a,b,c,47);
	R3(c,d,e,a,b,48); R3(b,c,d,e,a,49); R3(a,b,c,d,e,50); R3(e,a,b,c,d,51);
	R3(d,e,a,b,c,52); R3(c,d,e,a,b,53); R3(b,c,d,e,a,54); R3(a,b,c,d,e,55);
	R3(e,a,b,c,d,56); R3(d,e,a,b,c,57); R3(c,d,e,a,b,58); R3(b,c,d,e,a,59);
	R4(a,b,c,d,e,60); R4(e,a,b,c,d,61); R4(d,e,a,b,c,62); R4(c,d,e,a,b,63);
	R4(b,c,d,e,a,64); R4(a,b,c,d,e,65); R4(e,a,b,c,d,66); R4(d,e,a,b,c,67);
	R4(c,d,e,a,b,68); R4(b,c,d,e,a,69); R4(a,b,c,d,e,70); R4(e,a,b,c,d,71);
	R4(d,e,a,b,c,72); R4(c,d,e,a,b,73); R4(b,c,d,e,a,74); R4(a,b,c,d,e,75);
	R4(e,a,b,c,d,76); R4(d,e,a,b,c,77); R4(c,d,e,a,b,78); R4(b,c,d,e,a,79);

	state[0] += a;
	state[1] += b;
	state[2] += c;
	state[3] += d;
	state[4] += e;

	a = b = c = d = e = 0;
}

void
sha1_init(Sha1* self)
{
	self->count    = 0;
	self->state[0] = 0x67452301;
	self->state[1] = 0xEFCDAB89;
	self->state[2] = 0x98BADCFE;
	self->state[3] = 0x10325476;
	self->state[4] = 0xC3D2E1F0;
}

hot void
sha1_add(Sha1* self, uint8_t* data, size_t len)
{
	size_t i, j;
	j = (size_t)((self->count >> 3) & 63);
	self->count += ((uint64_t)len << 3);
	if ((j + len) > 63)
	{
		i = 64 - j;
		memcpy(&self->buffer[j], data, i);
		sha1_transform(self->state, self->buffer);
		for ( ; i + 63 < len; i += 64)
			sha1_transform(self->state, (uint8_t *)&data[i]);
		j = 0;
	} else {
		i = 0;
	}
	memcpy(&self->buffer[j], &data[i], len - i);
}

hot void
sha1_add_str(Sha1* self, Str* data)
{
	sha1_add(self, str_u8(data), str_size(data));
}

static inline void
sha1_pad(Sha1* self)
{
	uint8_t finalcount[8];
	unsigned int i;
	for (i = 0; i < 8; i++) {
		finalcount[i] = (uint8_t)((self->count >>
		    ((7 - (i & 7)) * 8)) & 255);
	}
	sha1_add(self, (uint8_t *)"\200", 1);
	while ((self->count & 504) != 448)
		sha1_add(self, (uint8_t *)"\0", 1);
	sha1_add(self, finalcount, 8);
}

void
sha1_end(Sha1* self, uint8_t digest[SHA1_DIGEST_LENGTH])
{
	sha1_pad(self);
	unsigned int i;
	for (i = 0; i < SHA1_DIGEST_LENGTH; i++) {
		digest[i] = (uint8_t)
		   ((self->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
	}
	memset(self, 0, sizeof(*self));
}

void
sha1_to_string(uint8_t digest[SHA1_DIGEST_LENGTH], char* buf, int buf_size)
{
	char fmt[] =
	"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
	"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x";
	snprintf(buf, buf_size, fmt,
	         digest[0],  digest[1],  digest[2],  digest[3],  digest[4],
	         digest[5],  digest[6],  digest[7],  digest[8],  digest[9],
	         digest[10], digest[11], digest[12], digest[13], digest[14],
	         digest[15], digest[16], digest[17], digest[18], digest[19]);
}

static inline uint8_t
hex_to_int(const char hex[2])
{
	uint8_t value = 0;
	int i = 0;
	while (i < 2)
	{
		uint8_t byte = hex[i];
		if (byte >= '0' && byte <= '9')
			byte = byte - '0';
		else
		if (byte >= 'a' && byte <= 'f')
			byte = byte - 'a' + 10;
		value = (value << 4) | (byte & 0xF);
		i++;
	}
	return value;
}

void
sha1_from_string(Str* string, uint8_t digest[SHA1_DIGEST_LENGTH])
{
	if (str_size(string) < SHA1_DIGEST_STRING_LENGTH - 1)
		error("incorrect sha1 string size");
	char* buf = str_of(string);
	int i = 0;
	int j = 0;
	for (; i < SHA1_DIGEST_STRING_LENGTH; j++)
	{
		digest[j] = hex_to_int(buf + i);
		i += 2;
	}
}
