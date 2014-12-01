/*
 * launchargs.h
 *
 *  Created on: 9 Jan 2014
 *      Author: jlp
 */

#ifndef LAUNCHARGS_H_
#define LAUNCHARGS_H_

typedef struct mongo_loadsrv_launchargs {
   char *testid;
   char *uri;
} MLaunchargs;


typedef struct mongo_loadsrv_testparams {
	char *testid;
	int numfields;
	int fieldsize;
	int numtoedit;
	int percentkeyqueries;
	int inserts;
	int updates;
	int queries;
	int pnum;
	int state; /* 0 - Stop , 1 - Pause, 2 - Run */
} MTestparams;


#endif /* LAUNCHARGS_H_ */
