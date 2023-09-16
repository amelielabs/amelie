
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>

void
auth_init(Auth* self)
{
	self->ro       = false;
	self->complete = false;
	for (int i = 0; i < AUTH_MAX; i++)
		str_init(&self->vars[i]);
}

void
auth_free(Auth* self)
{
	for (int i = 0; i < AUTH_MAX; i++)
		str_free(&self->vars[i]);
}

void
auth_reset(Auth* self)
{
	auth_free(self);
	auth_init(self);
}

void
auth_set(Auth* self, int type, Str* value)
{
	str_copy(&self->vars[type], value);
}

Str*
auth_get(Auth* self, int type)
{
	return &self->vars[type];
}

static inline void
auth_server_1(Auth* self, Tcp* tcp)
{
	// STEP 1 (send message on connect)

	// generate salt
	Uuid uuid;
	uuid_mgr_generate(global()->uuid_mgr, &uuid);
	char salt[UUID_SZ];
	uuid_to_string(&uuid, salt, sizeof(salt));
	str_strndup(&self->vars[AUTH_SALT], salt, sizeof(salt) - 1);

	// AUTH [version, uuid, read_only, salt]
	auto buf = msg_create(MSG_AUTH);
	encode_array(buf, 4);
	encode_string(buf, &config()->version.string);
	encode_string(buf, &config()->uuid.string);
	encode_bool(buf, var_int_of(&config()->read_only));
	encode_string(buf, &self->vars[AUTH_SALT]);
	msg_end(buf);

	tcp_send(tcp, buf);
}

static inline void
auth_server_2(Auth* self, Tcp* tcp)
{
	// STEP 2 (read id)
	auto buf = tcp_recv(tcp);

	// AUTH_REPLY [version, uuid, access, mode, user, token]
	auto msg = msg_of(buf);
	if (msg->id != MSG_AUTH_REPLY)
		error("auth: unexpected message");
	uint8_t* pos = msg->data;
	int count;
	data_read_array(&pos, &count);
	data_read_string_copy(&pos, &self->vars[AUTH_VERSION]);
	data_read_string_copy(&pos, &self->vars[AUTH_UUID]);
	data_read_string_copy(&pos, &self->vars[AUTH_ACCESS]);
	data_read_string_copy(&pos, &self->vars[AUTH_MODE]);
	data_read_string_copy(&pos, &self->vars[AUTH_USER]);
	data_read_string_copy(&pos, &self->vars[AUTH_TOKEN]);
	buf_free(buf);
}

static inline void
auth_sign(char* sign, int sign_size, Str* secret, Str* salt)
{
	// signature = sha1(secret_sha1, salt)
	uint8_t digest[SHA1_DIGEST_LENGTH];
	Sha1 sha1;
	sha1_init(&sha1);
	sha1_add_str(&sha1, secret);
	sha1_add_str(&sha1, salt);
	sha1_end(&sha1, digest);
	sha1_to_string(digest, sign, sign_size);
}

static inline bool
auth(Auth* self, UserMgr* user_mgr)
{
	// find user
	auto user_var = &self->vars[AUTH_USER];
	if (unlikely(str_empty(user_var)))
		return false;

	auto user = user_mgr_find(user_mgr, user_var);
	if (user == NULL)
		return false;

	// user has no password (grant)
	if (str_size(&user->config->secret) == 0)
		return true;

	auto token_var = &self->vars[AUTH_TOKEN];
	if (unlikely(str_empty(token_var)))
		return false;
	if (unlikely(str_size(token_var) != SHA1_DIGEST_STRING_LENGTH - 1))
		return false;

	// generate server token and compare it with one provided by the client
	char sign[SHA1_DIGEST_STRING_LENGTH];
	auth_sign(sign, sizeof(sign), &user->config->secret,
	          &self->vars[AUTH_SALT]);
	return str_compare_raw(token_var, sign, sizeof(sign) - 1);
}

static inline void
auth_server_3(Auth* self, Tcp* tcp, UserMgr* user_mgr)
{
	// STEP 3 (completion)

	// authenticate user
	self->complete = auth(self, user_mgr);

	// AUTH_COMPLETE [status]
	auto buf = msg_create(MSG_AUTH_COMPLETE);
	encode_array(buf, 1);
	encode_bool(buf, self->complete);
	msg_end(buf);

	tcp_send(tcp, buf);
}

void
auth_server(Auth* self, Tcp* tcp, UserMgr* user_mgr)
{
	// authenticate incoming remote connection

	// STEP 1 (send on connect)
	auth_server_1(self, tcp);

	// STEP 2 (read id)
	auth_server_2(self, tcp);

	// STEP 3 (authenticate and send completion status)
	auth_server_3(self, tcp, user_mgr);
}

static inline void
auth_client_1(Auth* self, Tcp* tcp)
{
	// STEP 1 (read server response)
	auto buf = tcp_recv(tcp);
	if (buf == NULL)
		error("auth: unexpected eof");

	// AUTH [version, uuid, read_only, salt]
	auto msg = msg_of(buf);
	if (msg->id != MSG_AUTH)
		error("%s", "auth: unexpected message");
	uint8_t* pos = msg->data;
	int count;
	data_read_array(&pos, &count);
	data_read_string_copy(&pos, &self->vars[AUTH_VERSION]);
	data_read_string_copy(&pos, &self->vars[AUTH_UUID]);
	data_read_bool(&pos, &self->ro);
	data_read_string_copy(&pos, &self->vars[AUTH_SALT]);
	buf_free(buf);
}

static inline void
auth_client_2(Auth* self, Tcp* tcp)
{
	// STEP 2 (reply)

	// generate client token (if secret is provded)
	auto  secret_var = &self->vars[AUTH_SECRET];
	auto  salt_var = &self->vars[AUTH_SALT];
	char  token_data[SHA1_DIGEST_STRING_LENGTH];
	char *token = NULL;
	int   token_size = 0;
	if (! str_empty(secret_var))
	{
		auth_sign(token_data, sizeof(token_data), secret_var, salt_var);
		token = token_data;
		token_size = sizeof(token_data) - 1;
	}

	// AUTH_REPLY [version, uuid, access, mode, user, token]
	auto buf = msg_create(MSG_AUTH_REPLY);
	encode_array(buf, 6);
	encode_string(buf, &config()->version.string);
	encode_string(buf, &config()->uuid.string);
	encode_string(buf, &self->vars[AUTH_ACCESS]);
	encode_string(buf, &self->vars[AUTH_MODE]);
	encode_string(buf, &self->vars[AUTH_USER]);
	encode_raw(buf, token, token_size);
	msg_end(buf);

	tcp_send(tcp, buf);
}

static inline void
auth_client_3(Auth* self, Tcp* tcp)
{
	// STEP 3 (get server decision)
	auto buf = tcp_recv(tcp);
	if (buf == NULL)
		error("auth: unexpected eof");

	// AUTH_COMPLETE [status]
	auto msg = msg_of(buf);
	if (msg->id != MSG_AUTH_COMPLETE)
		error("auth: unexpected message");
	uint8_t* pos = msg->data;
	int count;
	data_read_array(&pos, &count);
	data_read_bool(&pos, &self->complete);
	buf_free(buf);
}

void
auth_client(Auth* self, Tcp* tcp)
{
	// STEP 1 (read server response)
	auth_client_1(self, tcp);

	// STEP 2 (reply)
	auth_client_2(self, tcp);

	// STEP 3 (get server decision)
	auth_client_3(self, tcp);
}
