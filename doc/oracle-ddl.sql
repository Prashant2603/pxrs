-- PXRS Oracle DDL
-- Run against your Oracle database before using OracleRegistryStore

CREATE TABLE pxrs_partitions (
    partition_id    NUMBER(10)    PRIMARY KEY,
    owner_id        VARCHAR2(255) DEFAULT '',
    last_checkpoint NUMBER(19)    DEFAULT 0,
    version_epoch   NUMBER(19)    DEFAULT 0,
    last_heartbeat  NUMBER(19)    DEFAULT 0
);

CREATE TABLE pxrs_consumers (
    consumer_id   VARCHAR2(255) PRIMARY KEY,
    registered_at NUMBER(19)    NOT NULL,
    last_seen     NUMBER(19)    NOT NULL
);
