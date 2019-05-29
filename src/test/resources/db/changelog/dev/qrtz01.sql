DROP TABLE IF EXISTS  qrtz_fired_triggers;
DROP TABLE IF EXISTS  QRTZ_PAUSED_TRIGGER_GRPS;
DROP TABLE IF EXISTS  QRTZ_SCHEDULER_STATE;
DROP TABLE IF EXISTS  QRTZ_LOCKS;
DROP TABLE IF EXISTS  qrtz_simple_triggers;
DROP TABLE IF EXISTS  qrtz_cron_triggers;
DROP TABLE IF EXISTS  qrtz_simprop_triggers;
DROP TABLE IF EXISTS  QRTZ_BLOB_TRIGGERS;
DROP TABLE IF EXISTS  qrtz_triggers;
DROP TABLE IF EXISTS  qrtz_job_details;
DROP TABLE IF EXISTS  qrtz_calendars;

CREATE TABLE qrtz_job_details
(
    SCHED_NAME VARCHAR NOT NULL,
    JOB_NAME  VARCHAR NOT NULL,
    JOB_GROUP VARCHAR NOT NULL,
    DESCRIPTION VARCHAR NULL,
    JOB_CLASS_NAME   VARCHAR NOT NULL,
    IS_DURABLE BOOL NOT NULL,
    IS_NONCONCURRENT BOOL NOT NULL,
    IS_UPDATE_DATA BOOL NOT NULL,
    REQUESTS_RECOVERY BOOL NOT NULL,
    JOB_DATA JSONB NULL,
    PRIMARY KEY (SCHED_NAME,JOB_NAME,JOB_GROUP)
);

CREATE TABLE qrtz_triggers
(
    SCHED_NAME VARCHAR NOT NULL,
    TRIGGER_NAME VARCHAR NOT NULL,
    TRIGGER_GROUP VARCHAR NOT NULL,
    JOB_NAME  VARCHAR NOT NULL,
    JOB_GROUP VARCHAR NOT NULL,
    DESCRIPTION VARCHAR NULL,
    NEXT_FIRE_TIME BIGINT NULL,
    PREV_FIRE_TIME BIGINT NULL,
    PRIORITY INTEGER NULL,
    TRIGGER_STATE VARCHAR NOT NULL,
    TRIGGER_TYPE VARCHAR NOT NULL,
    START_TIME BIGINT NOT NULL,
    END_TIME BIGINT NULL,
    CALENDAR_NAME VARCHAR NULL,
    MISFIRE_INSTR SMALLINT NULL,
    JOB_DATA JSONB NULL,
    PRIMARY KEY (SCHED_NAME,TRIGGER_NAME,TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME,JOB_NAME,JOB_GROUP)
        REFERENCES QRTZ_JOB_DETAILS(SCHED_NAME,JOB_NAME,JOB_GROUP)
);


CREATE TABLE qrtz_fired_triggers
(
    SCHED_NAME VARCHAR NOT NULL,
    ENTRY_ID VARCHAR NOT NULL,
    TRIGGER_NAME VARCHAR NOT NULL,
    TRIGGER_GROUP VARCHAR NOT NULL,
    INSTANCE_NAME VARCHAR NOT NULL,
    FIRED_TIME BIGINT NOT NULL,
    SCHED_TIME BIGINT NOT NULL,
    PRIORITY INTEGER NOT NULL,
    STATE VARCHAR NOT NULL,
    JOB_NAME VARCHAR NULL,
    JOB_GROUP VARCHAR NULL,
    IS_NONCONCURRENT BOOL NULL,
    REQUESTS_RECOVERY BOOL NULL,
    PRIMARY KEY (SCHED_NAME,ENTRY_ID)
);