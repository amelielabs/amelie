
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
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

#include <openssl/evp.h>

#if 0
static void
fn_error(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);
	error("%.*s", str_size(&arg->string), str_of(&arg->string));
}

static void
fn_random(Call* self)
{
	call_validate(self, 0);
	int64_t value = random_generate(global()->random);
	value_set_int(self->result, llabs(value));
}

static void
fn_random_uuid(Call* self)
{
	call_validate(self, 0);

	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, global()->random);

	char id_sz[UUID_SZ];
	uuid_to_string(&id, id_sz, sizeof(id_sz));

	Str string;
	str_set(&string, id_sz, sizeof(id_sz) - 1);

	auto buf = buf_create();
	encode_string(buf, &string);
	str_init(&string);
	uint8_t* pos_str = buf->start;
	json_read_string(&pos_str, &string);
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
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);

	char digest[32];
	create_digest(EVP_md5(), &arg->string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_create();
	encode_string(buf, &string);
	str_init(&string);
	uint8_t* pos_str = buf->start;
	json_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_sha1(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);

	char digest[40];
	create_digest(EVP_sha1(), &arg->string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_create();
	encode_string(buf, &string);
	str_init(&string);
	uint8_t* pos_str = buf->start;
	json_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_encode(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);

	if (str_is_cstr(&argv[1]->string, "base64"))
	{
		auto buf = buf_create();
		base64_encode(buf, &argv[0]->string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else
	if (str_is_cstr(&argv[1]->string, "base64url"))
	{
		auto buf = buf_create();
		base64url_encode(buf, &argv[0]->string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else {
		error("encode(): unsupported format");
	}

}

static void
fn_decode(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);

	if (str_is_cstr(&argv[1]->string, "base64"))
	{
		auto buf = buf_create();
		base64_decode(buf, &argv[0]->string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else
	if (str_is_cstr(&argv[1]->string, "base64url"))
	{
		auto buf = buf_create();
		base64url_decode(buf, &argv[0]->string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else {
		error("decode(): unsupported format");
	}
}

static void
fn_serial(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 0, TYPE_STRING);
	auto table = table_mgr_find(&self->vm->db->table_mgr,
	                            &argv[0]->string,
	                            &argv[1]->string, true);
	value_set_int(self->result, serial_get(&table->serial));
}

static void
fn_jwt(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	call_validate_arg(self, 2, TYPE_STRING);

	// header
	Str  header_str;
	Buf* header = buf_create();
	guard_buf(header);
	if (argv[0]->type == TYPE_OBJ)
	{
		auto pos = argv[0]->data;
		json_export(header, self->vm->local->timezone, &pos);
		buf_str(header, &header_str);
	} else
	if (argv[0]->type == TYPE_STRING)
	{
		header_str = argv[0]->string;
	} else {
		error("jwt(): string or object expected");
	}

	// payload
	Str  payload_str;
	Buf* payload = buf_create();
	guard_buf(payload);
	if (argv[1]->type == TYPE_OBJ)
	{
		auto pos = argv[1]->data;
		json_export(payload, self->vm->local->timezone, &pos);
		buf_str(payload, &payload_str);
	} else
	if (argv[1]->type == TYPE_STRING)
	{
		payload_str = argv[1]->string;
	} else {
		error("jwt(): string or object expected");
	}

	// create jwt using supplied data
	JwtEncode jwt;
	jwt_encode_init(&jwt);
	guard(jwt_encode_free, &jwt);
	auto buf = jwt_encode(&jwt, &header_str, &payload_str,
	                      &argv[2]->string);
	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

FunctionDef fn_misc_def[] =
{
	{ "public", "error",        fn_error,        false },
	// sleep
	{ "public", "random",       fn_random,       false },
	{ "public", "random_uuid",  fn_random_uuid,  false },
	{ "public", "md5",          fn_md5,          false },
	{ "public", "sha1",         fn_sha1,         false },
	{ "public", "encode",       fn_encode,       false },
	{ "public", "decode",       fn_decode,       false },
	{ "public", "serial",       fn_serial,       false },
	{ "public", "jwt",          fn_jwt,          false },
	{  NULL,     NULL,          TYPE_NULL,      false }
};
#endif
