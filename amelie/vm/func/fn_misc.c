
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

#include <openssl/evp.h>

static void
fn_error(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	error("%.*s", str_size(&arg->string), str_of(&arg->string));
}

static void
fn_type(Call* self)
{
	auto  arg  = self->argv[0];
	char* name = NULL;
	call_validate(self);
	switch (arg->type) {
	case VALUE_INT:
		name = "int";
		break;
	case VALUE_BOOL:
		name = "int";
		break;
	case VALUE_REAL:
		name = "real";
		break;
	case VALUE_NULL:
		name = "null";
		break;
	case VALUE_STRING:
		name = "string";
		break;
	case VALUE_DATA:
		name = "data";
		break;
	case VALUE_INTERVAL:
		name = "interval";
		break;
	case VALUE_TIMESTAMP:
		name = "timestamp";
		break;
	case VALUE_SET:
		name = "set";
		break;
	case VALUE_MERGE:
		name = "merge";
		break;
	default:
		error("type(): operation type not supported");
		break;
	}
	Str string;
	str_set_cstr(&string, name);
	value_set_string(self->result, &string, NULL);
}

static void
fn_random(Call* self)
{
	call_validate(self);
	int64_t value = random_generate(global()->random);
	value_set_int(self->result, llabs(value));
}

static void
fn_random_uuid(Call* self)
{
	call_validate(self);

	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, global()->random);

	char id_sz[UUID_SZ];
	uuid_to_string(&id, id_sz, sizeof(id_sz));

	Str string;
	str_set(&string, id_sz, sizeof(id_sz) - 1);

	auto buf = buf_begin();
	encode_string(buf, &string);
	buf_end(buf);

	str_init(&string);
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static inline void
create_digest(const EVP_MD* md, Str* input, char* output)
{
	EVP_MD_CTX* ctx = EVP_MD_CTX_new();
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int  digest_len;
	EVP_DigestInit_ex2(ctx, md, NULL);
	EVP_DigestUpdate(ctx, str_of(input), str_size(input));
	EVP_DigestFinal_ex(ctx, digest, &digest_len);
	EVP_MD_CTX_free(ctx);
    const char* hex = "0123456789abcdef";
	for (unsigned int i = 0, j = 0; i < digest_len; i++)
	{
        output[j++] = hex[(digest[i] >> 4) & 0x0F];
        output[j++] = hex[(digest[i]) & 0x0F];
	}
}

static void
fn_md5(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);

	char digest[32];
	create_digest(EVP_md5(), &arg->string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_begin();
	encode_string(buf, &string);
	buf_end(buf);

	str_init(&string);
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_sha1(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);

	char digest[40];
	create_digest(EVP_sha1(), &arg->string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_begin();
	encode_string(buf, &string);
	buf_end(buf);

	str_init(&string);
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

FunctionDef fn_misc_def[] =
{
	{ "public", "error",       fn_error,       1 },
	{ "public", "type",        fn_type,        1 },
	{ "public", "random",      fn_random,      0 },
	{ "public", "random_uuid", fn_random_uuid, 0 },
	{ "public", "md5",         fn_md5,         1 },
	{ "public", "sha1",        fn_sha1,        1 },
	{  NULL,     NULL,         NULL,           0 }
};
