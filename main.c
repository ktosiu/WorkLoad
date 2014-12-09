/* MongoDB Load Generator for performance testing */
/* (c) John Page 2014 */

#include <sys/wait.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <stdlib.h>
#include <mongoc.h>
#include <time.h>
#include "launchargs.h"

#define CFG_DB "testsrv"
#define CFG_COLLECTION "tests"
#define DATA_DB "testsrv"
#define DATA_COLLECTION "data"
#define STATS_DB "testresults"
#define STATS_COLLECTION "results"

#define SEQUENCE_DB "testsrv"
#define SEQUENCE_COLL "data"
#define SEQUENCE_BATCH 10000
#define DEFAULT_URI "mongodb://localhost:27017"


int total_threads = 2;
int test_duration = 300;
int report_time=60;

void append_op_stats(bson_t *doc, char *opname, MOpStats *opstats);
static int parse_command_line(MLaunchargs *launchargs, int argc, char **argv);
static int fetch_test_params(MLaunchargs *launchargs, MTestparams *testparams);
int generate_new_record(mongoc_client_t *conn, MTestparams *testparams,
		bson_t *newrecord);
int run_load(MLaunchargs *launchargs, MTestparams *testparams);
int generate_data_fields(int numfields, int fieldlen, int pnum, bson_t *record);
long get_primary_key(mongoc_client_t *conn,
		MTestparams *testparams, bson_oid_t **goid, int existing);
int generate_update_record(mongoc_client_t *conn, MTestparams *testparams,
		bson_t *updaterec);

void debug_msg(int level, char *fmt, ...) {
	int verbosity = 99;
	if (level <= verbosity) {
		va_list args;
		va_start(args, fmt);
		vprintf(fmt, args);
		va_end(args);
	}
}

//I know this is really inefficient code but it's clean as and API to code with.
//I could come back and improve the performance later adding bson hashes.

const char *get_bson_string(const bson_t *obj, char *name) {
	bson_iter_t i;
	bson_type_t t;
	unsigned int length;

	if (bson_iter_init(&i, obj)) {
		if (bson_iter_find(&i, name)) {
			if (bson_iter_type(&i) == BSON_TYPE_UTF8) {
				return bson_iter_utf8(&i,&length);
			}
		}
	}

	debug_msg(3, "String value %s not found\n", name);
	return NULL;
}

int get_bson_int(const bson_t *obj, char *name) {
	bson_iter_t i;
	bson_type_t t;

	if (bson_iter_init(&i, obj)) {
		if (bson_iter_find(&i, name)) {
			if (bson_iter_type(&i) == BSON_TYPE_INT32) {
				return bson_iter_int32(&i);
			}
			if (bson_iter_type(&i) == BSON_TYPE_DOUBLE) {
				return (int)bson_iter_double(&i);
			}
		}
	}

	debug_msg(3, "Integer value %s not found\n", name);
	return 0;
}

int main(int argc, char **argv) {
	MLaunchargs launchargs;
	MTestparams testparams;

	srand48(1); //Predictable behaviour

	if (parse_command_line(&launchargs, argc, argv) != 0) {
		fprintf(stderr, "Exiting with Error");
		exit(1);
	}

	if (fetch_test_params(&launchargs, &testparams) != 0) {
		fprintf(stderr, "Exiting with Error");
		exit(1);
	}
	int x;
	int status;


	for (x = 0; x < total_threads; x++) {
		if (fork() == 0) {
			run_load(&launchargs, &testparams);
			exit(0);
		}
	}

	/* Reap children */
	while (wait(&status) != -1) {
		debug_msg(3, "Child Quit\n");
	}
}

int disconnect_from_mongo(mongoc_client_t *conn) {
	mongoc_client_destroy(conn);

	return 0;
}

int connect_to_mongo(char *uristr, mongoc_client_t **conn) {

	mongoc_init();

	*conn = mongoc_client_new(uristr);

	if (!*conn) {

		fprintf(stderr, "Failed to parse URI.\n");
		return 1;
	}

	return 0;
}

static int add_default_test(mongoc_client_t *conn) {
	bson_t *defaulttest;
	mongoc_collection_t *collection;
	bson_error_t error;

	defaulttest =
			BCON_NEW ( "_id", "loadtest", "numfields",BCON_INT32 (30), "fieldsize",
					BCON_INT32 (50), "inserts", BCON_INT32(100), "updates", BCON_INT32(200), "queries", BCON_INT32(300),
					"status", BCON_INT32(1),"pnum",BCON_INT32(20) );

	debug_msg(3, "Inserting Default test record\n");

	collection = mongoc_client_get_collection(conn, CFG_DB, CFG_COLLECTION);

	int rval = mongoc_collection_insert(collection, MONGOC_INSERT_NONE,
			defaulttest, NULL, &error);

	if (!rval) {
		//Race is another thread adds it
		debug_msg(3, "Error : %s\n", error.message);
		return 0;
	}

	bson_destroy(defaulttest);
	mongoc_collection_destroy(collection);

	return 0;
}

static int fetch_test_params(MLaunchargs *launchargs, MTestparams *testparams) {
	mongoc_client_t *conn;
	mongoc_cursor_t *cursor;
	mongoc_collection_t *collection;

	if (connect_to_mongo(launchargs->uri, &conn) != 0) {
		fprintf(stderr,
				"Unable to connect to test configuration server %s\n",
				launchargs->uri);
		return -1;
	}

	bson_t *query;
	const bson_t *testdetail;
	//I AM HERE
	query = BCON_NEW ( "_id", BCON_UTF8((const char*)launchargs->testid) );

	collection = mongoc_client_get_collection(conn, CFG_DB, CFG_COLLECTION);

	cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0,
			query, NULL, NULL);

	if (mongoc_cursor_next(cursor, &testdetail)) {

		size_t len;
		char *str;

		str = bson_as_json (testdetail, &len);
		printf ("%s\n", str);
		bson_free (str);



		testparams->numfields = get_bson_int(testdetail, "numfields");
		testparams->fieldsize = get_bson_int(testdetail, "fieldsize");
		testparams->inserts = get_bson_int(testdetail, "inserts");
		testparams->updates = get_bson_int(testdetail, "updates");
		testparams->queries = get_bson_int(testdetail, "queries");
		testparams->state = get_bson_int(testdetail, "status");
		testparams->pnum = get_bson_int(testdetail, "pnum");
	} else {
		debug_msg(3, "Not found test definition - using default\n");

		testparams->numfields = 20;
		testparams->fieldsize = 50;
		testparams->inserts = 100;
		testparams->updates = 200;
		testparams->queries = 300;
		testparams->state = 1;
		testparams->pnum = 50;
		if (add_default_test(conn) < 0) {
			return -1;
		}
	}
	mongoc_collection_destroy(collection);
    mongoc_cursor_destroy(cursor);
	bson_destroy(query);

	if (disconnect_from_mongo(conn) != 0) {
		fprintf(stderr, "Error disconnecting from Mongo\n");
		return -1;
	}
	return 0;
}

static int parse_command_line(MLaunchargs *launchargs, int argc, char **argv) {
	static char localhost[] = DEFAULT_URI;
	static char testid[] = "loadtest";

	opterr = 0;
	int c;

	launchargs->uri = localhost;
	launchargs->testid = testid;

	while ((c = getopt(argc, argv, "d:p:t:h:")) != -1)
		switch (c) {
		case 'd':
			test_duration = atoi(optarg);
			break;
		case 'p':
			total_threads = atoi(optarg);
			break;
		case 't':
			launchargs->testid = optarg;
			break;
		case 'h':
			launchargs->uri = optarg;
			break;
		case '?':
			if (optopt == 'h')
				fprintf(stderr, "Option -%c requires an argument.\n", optopt);
			else if (isprint(optopt))
				fprintf(stderr, "Unknown option `-%c'.\n", optopt);
			else
				fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
			return 1;
		default:
			abort();
		}

	return 0;
}


int save_stats(MLaunchargs *launchargs, bson_oid_t *thread_oid, MTestStats *stats, int seconds,int nrecs)
{
	//Save the stats to the server
	mongoc_client_t *conn;
	mongoc_cursor_t *cursor;
	mongoc_collection_t *collection;
	bson_t record;
	bson_error_t error;

	if (connect_to_mongo(launchargs->uri, &conn) != 0) {
		fprintf(stderr,
				"Unable to connect to test configuration server %s\n",
				launchargs->uri);
		return -1;
	}

	bson_init(&record);
	bson_t child;
	bson_t time_array;
	bson_append_document_begin(&record, "_id",-1,&child);
	bson_append_utf8(&child,"testid",-1,launchargs->testid,-1);
	bson_append_oid(&child,"clientid",-1,thread_oid);
	bson_append_int32(&child,"seconds",-1,seconds);
	bson_append_document_end(&record,&child);
	bson_append_int64(&record,"nrecs",-1,nrecs);
	append_op_stats(&record,"insert",&stats->inserts);
	append_op_stats(&record,"updates",&stats->updates);
	append_op_stats(&record,"queries",&stats->queries);


	collection = mongoc_client_get_collection(conn, STATS_DB,
						STATS_COLLECTION);
	if(! mongoc_collection_insert(collection,MONGOC_INSERT_NONE,&record,NULL,&error))
	{
		printf("%s\n", error.message);
		disconnect_from_mongo(conn);
		mongoc_collection_destroy(collection);
		bson_destroy(&record);
		return -1;
	}
	mongoc_collection_destroy(collection);
	bson_destroy(&record);
	disconnect_from_mongo(conn);
	return 0;
}

void append_op_stats(bson_t *doc, char *opname, MOpStats *opstats) {
	bson_t child;
	bson_t time_array;

	bson_append_document_begin(doc, opname, -1, &child);

	bson_append_int32(&child, "total_ops", -1, opstats->total_ops);
	bson_append_int32(&child, "total_time", -1, opstats->total_time);
	bson_append_int32(&child, "longest_time", -1, opstats->longest_time);
	bson_append_int32(&child, "long_ops", -1, opstats->long_ops);
	//Add the array of times
	bson_append_array_begin(&child, "times", -1, &time_array);
	for (int t = 0; t < 100; t++) {
		char sbuf[16];
		snprintf(sbuf, 16, "%d", t);
		bson_append_int32(&time_array, sbuf, -1, opstats->time_profile[t]);
	}
	bson_append_array_end(&child, &time_array);

	bson_append_document_end(doc, &child);
}

void log_stats(struct timeval *before_time, struct timeval *after_time,MOpStats *opstats)
{
	unsigned long before_millis;
	unsigned long after_millis;
	unsigned long taken;

	before_millis = (before_time->tv_sec * 1000) + (before_time->tv_usec  / 1000);
	after_millis = (after_time->tv_sec * 1000) + (after_time->tv_usec  / 1000);

	taken =  after_millis - before_millis;
	opstats->total_ops++;
	opstats->total_time = opstats->total_time + taken;
	if(taken > opstats->longest_time) opstats->longest_time = taken;
	if(taken > 1000 )
	{
		opstats->long_ops++;
	} else {
		int batch = taken/10;
		opstats->time_profile[batch]++;
	}
}


//This is a single thread
//It needs to add,update and query
//And every so often check for further instructions
//or test updates

int run_load(MLaunchargs *launchargs, MTestparams *testparams) {
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	time_t lastcheck = 0;
	bson_error_t error;
	MTestStats stats;
	struct timeval before_time;
	struct timeval  after_time;
	static bson_oid_t thread_oid;
	time_t start_time;
	bson_oid_init(&thread_oid,NULL);


	start_time = time(NULL);

	memset(&stats,0,sizeof(MTestStats));

	if (connect_to_mongo(launchargs->uri, &conn) != 0) {
		fprintf(stderr,
				"Unable to connect to test configuration server %s \n",
				launchargs->uri);
		return -1;
	}

	//Ensure indexes

	collection = mongoc_client_get_collection(conn, DATA_DB,
						DATA_COLLECTION);

	bson_t *index1;
	bson_t *index2;
	bson_t *index3;
	bson_t *index4;
	index1 = BCON_NEW (  "f1", BCON_INT32(1));
	index2 = BCON_NEW (  "f3", BCON_INT32(1));
	index3 = BCON_NEW (  "f5", BCON_INT32(1),"f1",BCON_INT32(1));
	index4 = BCON_NEW (  "f7", BCON_INT32(1),"f1",BCON_INT32(1));
	mongoc_collection_create_index(collection,index1,NULL,NULL);
	mongoc_collection_create_index(collection,index2,NULL,NULL);
	mongoc_collection_create_index(collection,index3,NULL,NULL);
	mongoc_collection_create_index(collection,index4,NULL,NULL);
	bson_destroy(index1);
	bson_destroy(index2);
	bson_destroy(index3);
	bson_destroy(index4);

//Every so often get new test info
	long checktime = 1; //Check every 10 seconds
	int timeslice = 0;
	while (time(NULL) - start_time < test_duration) {
		int inserts = testparams->inserts;
		int updates = testparams->updates;
		int queries = testparams->queries;
		int totalops = inserts + updates + queries;
		//If our status is 2 - exit

		time_t nowtime;

		nowtime = time(NULL);
		if (nowtime - lastcheck > checktime) {

			if (fetch_test_params(launchargs, testparams) != 0) {
				fprintf(stderr, "Exiting with Error");
				exit(1);
			}
			//Read test spec again

			//Save out current stats to the server
			long nrecs;
			collection = mongoc_client_get_collection(conn, DATA_DB,
								DATA_COLLECTION);
			bson_t query;
			bson_init(&query);

			nrecs = mongoc_collection_count(collection,MONGOC_QUERY_NONE,&query,0,0,NULL,NULL);

			save_stats(launchargs,&thread_oid,&stats,timeslice,nrecs);
			timeslice=timeslice+checktime; //Keep them in sync
			memset(&stats,0,sizeof(MTestStats));
			mongoc_collection_destroy(collection);
			bson_destroy(&query);
			lastcheck = nowtime;
		}

		if (testparams->state == 2) {

			debug_msg(3, "Quitting as testing terminated\n");
			exit(0);
		}

		if (testparams->state == 0) {
			//Sleep
			checktime = 1; /* When sleeping check every 5 seconds */
			debug_msg(3, "Test paused\n");
			sleep(1);
			continue;
		}

		checktime = report_time;

		int op = lrand48() % totalops;

		//debug_msg(3,"op = %d, i = %d, u = %d, q = %d\n",op,inserts,updates,queries);
		if (op < inserts) {
			bson_t newrecord;
			generate_new_record(conn, testparams, &newrecord);

			collection = mongoc_client_get_collection(conn, DATA_DB,
					DATA_COLLECTION);

			gettimeofday(&before_time,NULL);
			int rval = mongoc_collection_insert(collection, MONGOC_INSERT_NONE, &newrecord, NULL, &error);
			gettimeofday(&after_time,NULL);


			if (!rval) {
				debug_msg(3, "Error : %s\n", error.message);
				bson_destroy(&newrecord);
				mongoc_collection_destroy(collection);
				return -1;
			}

			log_stats(&before_time,&after_time,&stats.inserts);

			mongoc_collection_destroy(collection);
			bson_destroy(&newrecord);

		} else if (op < inserts + updates) {
			bson_t updaterecord;
			bson_t cond;

			bson_oid_t *goid;
			long key = get_primary_key(conn, testparams, &goid,1);

			bson_t child;
			bson_init(&cond);
			bson_append_document_begin(&cond, "_id",3,&child);
			bson_append_oid(&child,"o",1,goid);
			bson_append_utf8(&child,"t",1,"This is a string, not a small string either, it's in here to make the index larger",-1);
			bson_append_int64(&child,"seq",3,key);
			bson_append_document_end(&cond,&child);

			generate_update_record(conn, testparams, &updaterecord);

			collection = mongoc_client_get_collection(conn, DATA_DB,
					DATA_COLLECTION);

			gettimeofday(&before_time,NULL);
			if (!mongoc_collection_update(collection, MONGOC_UPDATE_NONE, &cond,
					&updaterecord, NULL, &error)) {
				printf("%s\n", error.message);
				bson_destroy(&cond);
				bson_destroy(&updaterecord);
				mongoc_collection_destroy(collection);
				return -1;
			}
			gettimeofday(&after_time,NULL);
			log_stats(&before_time,&after_time,&stats.updates);
			mongoc_collection_destroy(collection);
			bson_destroy(&cond);
			bson_destroy(&updaterecord);
		} else {
			bson_t cond;
			const bson_t *result;
			mongoc_cursor_t *cursor;
			bson_oid_t *goid;

			long key = get_primary_key(conn, testparams, &goid, 1);

			collection = mongoc_client_get_collection(conn, DATA_DB,
					DATA_COLLECTION);

			bson_t child;
					bson_init(&cond);
					bson_append_document_begin(&cond, "_id",3,&child);
					bson_append_oid(&child,"o",1,goid);
					bson_append_utf8(&child,"t",1,"This is a string, not a small string either, it's in here to make the index larger",-1);
					bson_append_int64(&child,"seq",3,key);
					bson_append_document_end(&cond,&child);

					gettimeofday(&before_time,NULL);

			cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0,
					0, &cond, NULL, NULL);

			if (mongoc_cursor_next(cursor, &result)) {
				//Got one - not doing anything with it though
				gettimeofday(&after_time,NULL);
				log_stats(&before_time,&after_time,&stats.queries);
			}
			mongoc_collection_destroy(collection);

			bson_destroy(&cond);
			mongoc_cursor_destroy(cursor);
		}
	}
	return 0;
}

long get_primary_key(mongoc_client_t *conn,
		MTestparams *testparams, bson_oid_t **oid, int existing) {

	static bson_oid_t fixed_oid;
	static long seqno = -1;

	long key;

	if(seqno == -1)
	{
		bson_oid_init(&fixed_oid,NULL);
		seqno = 1;
	}

	*oid = &fixed_oid;

	if(existing == 0)
	{
		seqno++;
		key = seqno;
	} else {
		key = lrand48() % seqno;
	}

	long rkey=0;
	for(int x=0;x<64;x++)
	{
		int bit = (key & 1<<x)?1:0;
		rkey = rkey | bit;
		rkey = rkey << 1;
	}
	return rkey;
}

/* Thist will produce a uniform distribution - if we want something else play with random() */
int generate_text_value(char *buffer, int maxlen, int minlen, int cardinality) {
	long value;
	int length;
	char c;
	int x;
	int lengths[] =
			{ 1, 3, 4, 5, 4, 2, 3, 4, 3, 5, 4, 1, 5, 4, 3, 4, 5, 2, 4, 4 };

	/* Generate a random test string of the appropriate length for a given set size */
	value = 0;
	int rounds = 20;
	int cr = cardinality / rounds;
	for(int r=0;r<rounds;r++)
	{
		value = value + (lrand48() %cr);
	}

	if (maxlen > minlen) {
		length = (value % (maxlen - minlen)) + minlen;
	} else {
		length = minlen;
	}
	c = value % 26;
	int wlenidx = value % 20;
	int wlen = lengths[wlenidx] + 2;

	for (x = 0; x < length; x++) {
		if (x % wlen == wlen - 1) {
			buffer[x] = ' ';
			wlenidx = (wlenidx + 1) % 20;
			wlen = lengths[wlenidx] + 2;
		} else {
			buffer[x] = c + 'a';
			c = c * c + 9;
			c = c % 26;
		}
	}
	buffer[x] = '\0';

	return value;
}

int generate_int_value(int min, int max) {
	long value;
    //Normalised distribution
    //rounding errors for small ranges

	int l = max - min;
	int rounds = 20;

	if(min >= max) return min;

	l = l / rounds;
	for(int r=0;r<rounds;r++)
	{
		value = value + ( lrand48() % l);
	}

	value = value + min;
	return value;
}

int generate_update_record(mongoc_client_t *conn, MTestparams *testparams,
		bson_t *updaterec) {
	bson_t child;

	bson_init(updaterec);
	bson_append_document_begin(updaterec, "$set",4,&child);
	generate_data_fields(testparams->numfields, testparams->fieldsize,
			testparams->pnum, &child);
	bson_append_document_end(updaterec,&child);


	return 0;
}

//Create a single BSON Document ready to append

int generate_new_record(mongoc_client_t *conn, MTestparams *testparams,
		bson_t *newrecord) {

	char *stringbuf;
	bson_init(newrecord);
	bson_t child;
	bson_oid_t *goid;

	// Add a Primary Key
	long key = get_primary_key(conn,testparams,&goid,0);

	bson_append_document_begin(newrecord, "_id",3,&child);
	bson_append_oid(&child,"o",1,goid);
	bson_append_utf8(&child,"t",1,"This is a string, not a small string either, it's in here to make the index larger",-1);
	bson_append_int64(&child,"seq",3,key);
	bson_append_document_end(newrecord,&child);


	generate_data_fields(testparams->numfields, testparams->fieldsize,
			testparams->pnum, newrecord);

	return 0;
}

int generate_data_fields(int numfields, int fieldlen, int pnum, bson_t *record) {
	int x;
	char textbuf[64];
	char fname[20];

	for (x = 0; x < numfields; x++) {

		if (x % 10 < pnum / 10) {

			sprintf(fname, "f%x", x);
			bson_append_int32(record, fname, -1,generate_int_value(100, 100000));
		} else {
			sprintf(fname, "f%x", x);
			generate_text_value(textbuf, fieldlen, fieldlen, 1000);
			bson_append_utf8(record, fname,-1, textbuf,-1);
		}
	}
	/*char *str;
	size_t len;

	str = bson_as_json (record, &len);
	printf ("%s\n", str);
	bson_free (str);*/

	return 0;
}

