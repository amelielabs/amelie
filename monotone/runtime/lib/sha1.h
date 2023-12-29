#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Sha1 Sha1;

#define	SHA1_BLOCK_LENGTH          (64)
#define	SHA1_DIGEST_LENGTH         (20)
#define SHA1_DIGEST_STRING_LENGTH  (SHA1_DIGEST_LENGTH * 2 + 1)

struct Sha1
{
    uint32_t state[5];
    uint64_t count;
    uint8_t  buffer[SHA1_BLOCK_LENGTH];
};

void sha1_init(Sha1*);
void sha1_add(Sha1*, uint8_t*, size_t);
void sha1_add_str(Sha1*, Str*);
void sha1_end(Sha1*, uint8_t[SHA1_DIGEST_LENGTH]);
void sha1_to_string(uint8_t[SHA1_DIGEST_LENGTH], char*, int);
void sha1_from_string(Str*, uint8_t[SHA1_DIGEST_LENGTH]);
