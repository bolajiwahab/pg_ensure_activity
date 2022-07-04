/*-------------------------------------------------------------------------
 *
 * pg_ensure_activity.c
 *      Ensure an activity on a PostgreSQL primary cluster.
 *      This keeps replication lag at bay according to the
 *      interval set by GUC variable pg_ensure_activity.interval
 *
 *      Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_ensure_activity/pg_ensure_activity.c
 *
 *-------------------------------------------------------------------------
*/

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "commands/async.h"
#include "utils/guc.h"
#include <time.h>


PG_MODULE_MAGIC;

void        _PG_init(void);
void        pg_ensure_activity_main(Datum);

/* GUC variables */
static int pg_ensure_activity_interval = 3600; // Default hourly
static char *pg_ensure_activity_role = NULL; // Default to cluster owner
static char *pg_ensure_activity_status = "on"; // Default to on
static char *pg_ensure_activity_dbname = NULL;


void
pg_ensure_activity_main(Datum main_arg)
{
	StringInfoData buf;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We are now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(pg_ensure_activity_dbname, pg_ensure_activity_role, 0);

    /* Set our application name */
    pgstat_report_appname("pg_ensure_activity");

	elog(LOG, "%s started",
		 MyBgworkerEntry->bgw_name);

	initStringInfo(&buf);
    ;

	/*
	 * Main loop: do this until SIGTERM is received and processed by
	 * ProcessInterrupts.
	 */
	for (;;)
	{
		int		ret;
		int		res;
		bool	isnull;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 pg_ensure_activity_interval * 1000L,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we are about to run, and also the transaction
		 * start time.  Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 */

        /* Check status first */
        if (strcmp(pg_ensure_activity_status, "off") == 0) {
            continue;
        }

		/* Set statement start time */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

        resetStringInfo(&buf);
        appendStringInfo(&buf, "SELECT (setting = 'on')::int FROM pg_catalog.pg_settings WHERE name = 'track_commit_timestamp'");

		/* We can now execute queries via SPI */
		ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_SELECT) {
            elog(FATAL, "Cannot determine if the configuration parameter 'track_commit_timestamp' is set. It is required by pg_ensure_activity");
        }

		if (SPI_processed != 1)
			elog(FATAL, "not a singleton result. Expecting a single tuple");

    	res = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc,
											  1, &isnull));
		if (isnull)
			elog(FATAL, "null result, a value is expected");

		if (res == 0) {
            elog(FATAL, "Make sure the configuration parameter 'track_commit_timestamp' is set as it is required by pg_ensure_activity");
        }

		// Make sure the stat table exists
        resetStringInfo(&buf);
		#if (PG_VERSION_NUM < 130000)
			appendStringInfo(&buf, "CREATE TABLE IF NOT EXISTS public.pg_ensure_activity_stats ("
								"app text NOT NULL PRIMARY KEY,"
								"total_runs integer NOT NULL,"
								"last_xact_id bigint NOT NULL,"
								"last_run timestamp with time zone NOT NULL)"
							);
		#else
			appendStringInfo(&buf, "CREATE TABLE IF NOT EXISTS public.pg_ensure_activity_stats ("
								"app text NOT NULL PRIMARY KEY,"
								"total_runs integer NOT NULL,"
								"last_xact_id xid8 NOT NULL,"
								"last_run timestamp with time zone NOT NULL)"
							);
    	#endif

		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to set up pg_ensure_activity_stats table");

        // time stats
		time_t epoch = time(NULL);
		char current_time[26];
		struct tm* tm_info;
		tm_info = localtime(&epoch);
    	strftime(current_time, 26, "%Y-%m-%d %H:%M:%S", tm_info);

        resetStringInfo(&buf);
		#if (PG_VERSION_NUM < 130000)
        	appendStringInfo(&buf, "SELECT pg_catalog.txid_current() WHERE (pg_catalog.pg_last_committed_xact()).timestamp+$$%ds$$::INTERVAL < now()",
								pg_ensure_activity_interval);
		#else
        	appendStringInfo(&buf, "SELECT pg_catalog.pg_current_xact_id() WHERE (pg_catalog.pg_last_committed_xact()).timestamp+$$%ds$$::INTERVAL < now()",
								pg_ensure_activity_interval);
    	#endif

		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT) {
            elog(FATAL, "Unable to ensure activity on the cluster");
        }

		if (SPI_processed == 1) {
			res = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc,
												1, &isnull));

			resetStringInfo(&buf);
			#if (PG_VERSION_NUM < 130000)
				appendStringInfo(&buf, "INSERT INTO public.pg_ensure_activity_stats AS pe"
								       "(app, total_runs, last_xact_id, last_run) VALUES('pg_ensure_activity', 1, %d, '%s')"
								       "ON CONFLICT (app) DO UPDATE SET total_runs = pe.total_runs + 1, last_xact_id =%d, last_run = '%s'",
								       res, current_time, res, current_time);
			#else
				appendStringInfo(&buf, "INSERT INTO public.pg_ensure_activity_stats AS pe"
				    				   "(app, total_runs, last_xact_id, last_run) VALUES('pg_ensure_activity', 1, %d::text::xid8, '%s')"
					     			   "ON CONFLICT (app) DO UPDATE SET total_runs = pe.total_runs + 1, last_xact_id =%d::text::xid8, last_run = '%s'",
						    		   res, current_time, res, current_time);
			#endif


			SetCurrentStatementStartTimestamp();
			ret = SPI_execute(buf.data, false, 0);
		}

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
    }
}

/*
 * Entrypoint of this module.
 *
*/

void _PG_init(void)
{
    BackgroundWorker    worker;

	/* get the configuration */
    DefineCustomIntVariable("pg_ensure_activity.interval",
                            "How often should an activity be ensured on a PostgreSQL cluster (in seconds)",
                            NULL,
                            &pg_ensure_activity_interval,
                            3600,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_ensure_activity.dbname",
                               "The specific database in the cluster to run pg_ensure_activity background worker on",
                               NULL,
                               &pg_ensure_activity_dbname,
                               "postgres",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomStringVariable("pg_ensure_activity.role",
                               "The role to be used by pg_ensure_activity background worker",
                               NULL,
                               &pg_ensure_activity_role,
                               NULL,
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomStringVariable("pg_ensure_activity.status",
                               "Set status of pg_ensure_activity background worker",
                               NULL,
                               &pg_ensure_activity_status,
                               "on",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

	// MarkGUCPrefixReserved("pg_ensure_activity"); //why is it not working even with guc.h?

    /* register the worker processes */
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_ensure_activity background worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_ensure_activity");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    // worker.bgw_main = worker_spi_main;
    sprintf(worker.bgw_library_name, "pg_ensure_activity");
	sprintf(worker.bgw_function_name, "pg_ensure_activity_main");
    worker.bgw_notify_pid = 0;
    worker.bgw_restart_time = 60; // restart every 1 minute after a crash //BGW_NEVER_RESTART;
    worker.bgw_main_arg = CStringGetDatum(pg_ensure_activity_dbname);
    RegisterBackgroundWorker(&worker);
}
