
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
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>

void
server_bootstrap(void)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// tls and auto are disabled by default

	// []
	encode_array(&buf);

	// {}
	encode_obj(&buf);
	// host
	encode_raw(&buf, "host", 4);
	encode_cstr(&buf, "*");
	// port
	encode_raw(&buf, "port", 4);
	encode_integer(&buf, 3485);
	encode_obj_end(&buf);

	encode_array_end(&buf);

	opt_json_set_buf(&config()->listen, &buf);
}
