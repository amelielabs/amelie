#pragma once

//
// monotone
//
// SQL OLTP database
//

enum
{
	COMPRESSION_OFF  = 0,
	COMPRESSION_ZSTD = 1
};

int  compression_get(const char*, int);
void compression_compress(Buf*, int, Buf*, Buf*);
void compression_decompress(Buf*, int, int, char*, int);
bool compression_is_set(int);
