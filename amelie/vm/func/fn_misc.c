
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
#include <amelie_func.h>

#include <openssl/evp.h>

static void
fn_throw(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_STRING);
	error("{str}", &argv[0].string);
}

static void
fn_sleep(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_INT);
	coroutine_sleep(argv[0].integer);
	value_set_null(self->result);
}

static void
fn_random(Call* self)
{
	call_expect(self, 0);
	int64_t value = random_generate(&self->local->random);
	value_set_int(self->result, llabs(value));
}

static void
fn_random_uuid(Call* self)
{
	call_expect(self, 0);
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &self->local->random);
	value_set_uuid(self->result, &id);
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
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_STRING);

	char digest[32];
	create_digest(EVP_md5(), &argv[0].string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_create();
	encode_str(buf, &string);
	str_init(&string);
	uint8_t* pos_str = buf->start;
	unpack_str(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_sha1(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_STRING);

	char digest[40];
	create_digest(EVP_sha1(), &argv[0].string, digest);

	Str string;
	str_set(&string, digest, sizeof(digest));

	auto buf = buf_create();
	encode_str(buf, &string);
	str_init(&string);
	uint8_t* pos_str = buf->start;
	unpack_str(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_encode(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_arg(self, 0, TYPE_STRING);
	call_arg(self, 1, TYPE_STRING);

	if (str_is_cstr(&argv[1].string, "base64"))
	{
		auto buf = buf_create();
		base64_encode(buf, &argv[0].string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else
	if (str_is_cstr(&argv[1].string, "base64url"))
	{
		auto buf = buf_create();
		base64url_encode(buf, &argv[0].string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else {
		call_error_at(self, 1, "unsupported format");
	}

}

static void
fn_decode(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_arg(self, 0, TYPE_STRING);
	call_arg(self, 1, TYPE_STRING);

	if (str_is_cstr(&argv[1].string, "base64"))
	{
		auto buf = buf_create();
		base64_decode(buf, &argv[0].string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else
	if (str_is_cstr(&argv[1].string, "base64url"))
	{
		auto buf = buf_create();
		base64url_decode(buf, &argv[0].string);
		Str string;
		str_set_u8(&string, buf->start, buf_size(buf));
		value_set_string(self->result, &string, buf);
	} else {
		call_error_at(self, 1, "unsupported format");
	}
}

static void
fn_identity_of(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_STRING);
	auto table = catalog_find_table(&share()->db->catalog, &self->local->user,
	                                &argv[0].string, true);
	value_set_int(self->result, sequence_get(&table->seq));
}

static void
fn_jwt(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 3);
	call_arg(self, 2, TYPE_STRING);

	// header
	Str  header_str;
	Buf* header = buf_create();
	defer_buf(header);
	if (argv[0].type == TYPE_JSON)
	{
		// obj
		auto pos = argv[0].json;
		if (! data_is_obj(pos))
			call_error_at(self, 0, "string or JSON object expected");
		json_export(header, self->local->timezone, &pos);
		buf_str(header, &header_str);
	} else
	if (argv[0].type == TYPE_STRING)
	{
		header_str = argv[0].string;
	} else {
		call_error_at(self, 0, "string or JSON object expected");
	}

	// payload
	Str  payload_str;
	Buf* payload = buf_create();
	defer_buf(payload);
	if (argv[1].type == TYPE_JSON)
	{
		auto pos = argv[1].json;
		if (! data_is_obj(pos))
		call_error_at(self, 1, "string or JSON object expected");
		json_export(payload, self->local->timezone, &pos);
		buf_str(payload, &payload_str);
	} else
	if (argv[1].type == TYPE_STRING)
	{
		payload_str = argv[1].string;
	} else {
		call_error_at(self, 1, "string or JSON object expected");
	}

	// create jwt using supplied data
	JwtEncode jwt;
	jwt_encode_init(&jwt);
	defer(jwt_encode_free, &jwt);
	auto buf = jwt_encode(&jwt, &header_str, &payload_str,
	                      &argv[2].string);
	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_locks_count(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	call_arg(self, 0, TYPE_STRING);
	auto count = locks_count(&runtime()->locks, &argv[0].string);
	value_set_int(self->result, count);
}

static void
fn_breakpoint(Call* self)
{
	call_expect(self, 0);
	breakpoint(REL_BP_QUERY);
}

void
fn_misc_register(Functions* self)
{
	// error()
	Function* func;
	func = function_allocate(TYPE_NULL, "error", fn_throw);
	function_unset(func, FN_CONST);
	functions_add(self, func);

	// sleep()
	func = function_allocate(TYPE_NULL, "sleep", fn_sleep);
	function_unset(func, FN_CONST);
	functions_add(self, func);

	// random()
	func = function_allocate(TYPE_INT, "random", fn_random);
	function_unset(func, FN_CONST);
	function_set(func, FN_FRONTEND);
	functions_add(self, func);

	// random_uuid()
	func = function_allocate(TYPE_UUID, "random_uuid", fn_random_uuid);
	function_unset(func, FN_CONST);
	function_set(func, FN_FRONTEND);
	functions_add(self, func);

	// md5()
	func = function_allocate(TYPE_STRING, "md5", fn_md5);
	functions_add(self, func);

	// sha1()
	func = function_allocate(TYPE_STRING, "sha1", fn_sha1);
	functions_add(self, func);

	// encode()
	func = function_allocate(TYPE_STRING, "encode", fn_encode);
	functions_add(self, func);

	// decode()
	func = function_allocate(TYPE_STRING, "decode", fn_decode);
	functions_add(self, func);

	// identity_of()
	func = function_allocate(TYPE_INT, "identity_of", fn_identity_of);
	function_unset(func, FN_CONST);
	functions_add(self, func);

	// jwt()
	func = function_allocate(TYPE_STRING, "jwt", fn_jwt);
	functions_add(self, func);

	// locks_count()
	func = function_allocate(TYPE_INT, "locks_count", fn_locks_count);
	function_unset(func, FN_CONST);
	functions_add(self, func);

	// breakpoint()
	func = function_allocate(TYPE_NULL, "breakpoint", fn_breakpoint);
	function_unset(func, FN_CONST);
	functions_add(self, func);
}
