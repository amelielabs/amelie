
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>

void
config_state_init(ConfigState* self)
{
	mutex_init(&self->write_lock);
	file_init(&self->file);
}

void
config_state_free(ConfigState* self)
{
	mutex_free(&self->write_lock);
}

void
config_state_open(ConfigState* self, Config* config, const char* path)
{
	// open or create config state file
	if (fs_exists("%s", path)) {
		file_open(&self->file, path);
	} else
	{
		file_create(&self->file, path);
		config_state_save(self, config);
	}

	// read and apply config state file data
	auto buf = file_import("%s", path);
	uint8_t* pos = msg_of(buf)->data;
	config_set_data(config, true, &pos);
	buf_free(buf);
}

void
config_state_close(ConfigState* self)
{
	file_close(&self->file);
}

void
config_state_save(ConfigState* self, Config* config)
{
	mutex_lock(&self->write_lock);

	Exception e;
	if (try(&e))
	{
		// prepare config
		auto buf = config_list_persistent(config);

		// update config_state file
		file_pwrite_buf(&self->file, buf, 0);
		// todo: sync

		buf_free(buf);
	}

	mutex_unlock(&self->write_lock);
	if (catch(&e))
		rethrow();
}
