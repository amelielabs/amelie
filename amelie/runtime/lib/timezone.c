
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <arpa/inet.h>

Timezone*
timezone_create(Str* name, char* path)
{
	Timezone* self = am_malloc(sizeof(Timezone));
	guard(timezone_free, self);
	memset(self, 0, sizeof(Timezone));

	str_init(&self->name);
	str_copy(&self->name, name);
	hashtable_node_init(&self->node);

	// Based on RFC 8536: The Time Zone Information Format (TZif)

	// +---------------+---+
	// |  magic    (4) |ver|
	// +---------------+---+---------------------------------------+
	// |           [unused - reserved for future use] (15)         |
	// +---------------+---------------+---------------+-----------+
	// |  isutcnt  (4) |  isstdcnt (4) |  leapcnt  (4) |
	// +---------------+---------------+---------------+
	// |  timecnt  (4) |  typecnt  (4) |  charcnt  (4) |
	// +---------------+---------------+---------------+

	// read header
	auto header = &self->header;

	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_RDONLY, 0644);
	file_read(&file, header, sizeof(TimezoneHeader));

	// check magic and version
	if (memcmp(header->magic, "TZif", 4) != 0)
		return NULL;

	// convert to host format
	header->isutcnt  = ntohl(header->isutcnt);
	header->isstdcnt = ntohl(header->isstdcnt);
	header->leapcnt  = ntohl(header->leapcnt);
	header->timecnt  = ntohl(header->timecnt);
	header->typecnt  = ntohl(header->typecnt);
	header->charcnt  = ntohl(header->charcnt);

	// read data block

	//  +---------------------------------------------------------+
	//  |  transition times          (timecnt x TIME_SIZE)        |
	//  +---------------------------------------------------------+
	//  |  transition types          (timecnt)                    |
	//  +---------------------------------------------------------+
	//  |  local time type records   (typecnt x 6)                |
	//  +---------------------------------------------------------+
	//  |  time zone designations    (charcnt)                    |
	//  +---------------------------------------------------------+
	//  |  leap-second records       (leapcnt x (TIME_SIZE + 4))  |
	//  +---------------------------------------------------------+
	//  |  standard/wall indicators  (isstdcnt)                   |
	//  +---------------------------------------------------------+
	//  |  UT/local indicators       (isutcnt)                    |
	//  +---------------------------------------------------------+

	if (self->header.timecnt > 0)
	{
		// transition times
		int transition_times_size = header->timecnt * sizeof(uint32_t);
		self->transition_times = am_malloc(transition_times_size);
		file_read(&file, self->transition_times, transition_times_size);
		for (uint32_t i = 0; i < header->timecnt; i++)
			self->transition_times[i] = ntohl(self->transition_times[i]);

		// transition types
		int transition_types_size = header->timecnt;
		self->transition_types = am_malloc(transition_types_size);
		file_read(&file, self->transition_types, transition_types_size);
	}

	// local time type records
	int times_size = sizeof(TimezoneTime) * header->typecnt;
	self->times = am_malloc(times_size);
	file_read(&file, self->times, times_size);
	for (uint32_t i = 0; i < header->typecnt; i++)
		self->times[i].utoff = ntohl(self->times[i].utoff);

	unguard();
	unguard();

	file_close(&file);
	return self;
}

void
timezone_free(Timezone* self)
{
	str_free(&self->name);
	if (self->transition_times)
		am_free(self->transition_times);
	if (self->transition_types)
		am_free(self->transition_types);
	am_free(self->times);
	am_free(self);
}

hot TimezoneTime*
timezone_match(Timezone* self, time_t time)
{
	if (self->header.timecnt == 0)
		return &self->times[0];
	int min = 0;
	int mid = 0;
	int max = self->header.timecnt - 1;
	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		if (self->transition_times[mid] == time)
		{
			min = mid;
			break;
		}
		if (self->transition_times[mid] < time)
			min = mid + 1;
		else
			max = mid - 1;
	}

	if (min > (int)self->header.timecnt)
		min = self->header.timecnt;

	if (time > self->transition_times[min])
		min++;

	auto type = self->transition_types[min - 1];
	return &self->times[type];
}

/*
tzzone_t *libtz_tzzone_at(const tzinfo_t *zi, int64_t whence) {
  int l = 1, r = zi->timecnt;
  if(r == 0) return &zi->tz[0];

  int i = (l+r)/2;
  while(i > l)
  {
    if(l >= zi->timecnt)
	{
      i = zi->timecnt;
      break;
    }

    if(l == i) break;

    if(zi->trans_times[i] == whence) { i++; break; }
    else if(zi->trans_times[i] > whence) r = i-1;
    else l = i+1;
    i = (l+r)/2;
  }
  if(i > zi->timecnt) i = zi->timecnt;

  if(whence > zi->trans_times[i]) i++;
  return &zi->tz[zi->trans_types[i-1]];
}

struct tm *
libtz_zonetime(const tzinfo_t *zi, const time_t *timep, struct tm *result, const tzzone_t **tzr) {
  struct tm *rv;
  time_t whence;
  if(!zi) return NULL;
  whence = timep ? *timep : time(NULL);
  tzzone_t *tz = libtz_tzzone_at(zi, whence);
  if(!tz) return NULL;
  whence += tz->offset;
  rv = gmtime_r(&whence, result);
  if(!rv) return NULL;
  rv->tm_isdst = tz->dst;
  if(tzr) *tzr = tz;
  return rv;
}
*/
