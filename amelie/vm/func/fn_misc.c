
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

#include <openssl/evp.h>

static void
fn_throw(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	error("%.*s", str_size(&argv[0].string), str_of(&argv[0].string));
}

static void
fn_sleep(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_INT);
	coroutine_sleep(argv[0].integer);
	value_set_null(self->result);
}

static void
fn_random(Fn* self)
{
	fn_expect(self, 0);
	int64_t value = random_generate(&runtime()->random);
	value_set_int(self->result, llabs(value));
}

static void
fn_random_uuid(Fn* self)
{
	fn_expect(self, 0);
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
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
fn_md5(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);

	char digest[32];
	create_digest(EVP_md5(), &argv[0].string, digest);

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
fn_sha1(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);

	char digest[40];
	create_digest(EVP_sha1(), &argv[0].string, digest);

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
fn_encode(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_STRING);
	fn_expect_arg(self, 1, TYPE_STRING);

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
		fn_error_arg(self, 1, "unsupported format");
	}

}

static void
fn_decode(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_STRING);
	fn_expect_arg(self, 1, TYPE_STRING);

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
		fn_error_arg(self, 1, "unsupported format");
	}
}

static void
fn_identity_of(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_STRING);
	fn_expect_arg(self, 0, TYPE_STRING);
	auto table = table_mgr_find(&share()->storage->catalog.table_mgr,
	                            &argv[0].string,
	                            &argv[1].string, true);
	value_set_int(self->result, sequence_get(&table->seq));
}

static void
fn_jwt(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 3);
	fn_expect_arg(self, 2, TYPE_STRING);

	// header
	Str  header_str;
	Buf* header = buf_create();
	defer_buf(header);
	if (argv[0].type == TYPE_JSON)
	{
		// obj
		auto pos = argv[0].json;
		if (! json_is_obj(pos))
			fn_error_arg(self, 0, "string or JSON object expected");
		json_export(header, self->local->timezone, &pos);
		buf_str(header, &header_str);
	} else
	if (argv[0].type == TYPE_STRING)
	{
		header_str = argv[0].string;
	} else {
		fn_error_arg(self, 0, "string or JSON object expected");
	}

	// payload
	Str  payload_str;
	Buf* payload = buf_create();
	defer_buf(payload);
	if (argv[1].type == TYPE_JSON)
	{
		auto pos = argv[1].json;
		if (! json_is_obj(pos))
		fn_error_arg(self, 1, "string or JSON object expected");
		json_export(payload, self->local->timezone, &pos);
		buf_str(payload, &payload_str);
	} else
	if (argv[1].type == TYPE_STRING)
	{
		payload_str = argv[1].string;
	} else {
		fn_error_arg(self, 1, "string or JSON object expected");
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

void
fn_misc_register(FunctionMgr* self)
{
	// error()
	Function* func;
	func = function_allocate(TYPE_NULL, "error", fn_throw);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// sleep()
	func = function_allocate(TYPE_NULL, "sleep", fn_sleep);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// random()
	func = function_allocate(TYPE_INT, "random", fn_random);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// random_uuid()
	func = function_allocate(TYPE_UUID, "random_uuid", fn_random_uuid);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// md5()
	func = function_allocate(TYPE_STRING, "md5", fn_md5);
	function_mgr_add(self, func);

	// sha1()
	func = function_allocate(TYPE_STRING, "sha1", fn_sha1);
	function_mgr_add(self, func);

	// encode()
	func = function_allocate(TYPE_STRING, "encode", fn_encode);
	function_mgr_add(self, func);

	// decode()
	func = function_allocate(TYPE_STRING, "decode", fn_decode);
	function_mgr_add(self, func);

	// identity_of()
	func = function_allocate(TYPE_INT, "identity_of", fn_identity_of);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// jwt()
	func = function_allocate(TYPE_STRING, "jwt", fn_jwt);
	function_mgr_add(self, func);
}
