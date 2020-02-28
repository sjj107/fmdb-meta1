-- 删除所有的表
DROP TABLE IF EXISTS MANIFEST;
DROP TABLE IF EXISTS COLUMNS;
DROP TABLE IF EXISTS PARTITIONS;
DROP TABLE IF EXISTS PARTITION_PARAMS;
DROP TABLE IF EXISTS INDEX_PARAMS;
DROP TABLE IF EXISTS IDXS;
DROP TABLE IF EXISTS TABLE_PARAMS;
DROP TABLE IF EXISTS TBLS;
DROP TABLE IF EXISTS TBL_TYPE;
DROP TABLE IF EXISTS FMDB_CT;
DROP TABLE IF EXISTS DBS;
-- 创建db表
CREATE TABLE IF NOT EXISTS DBS
(
    DB_ID VARCHAR(17) not null UNIQUE,
    NAME  VARCHAR(128) not null UNIQUE,
    PRIMARY KEY (DB_ID)
);
-- insert into DBS
-- VALUES ('1', 'fhorc');
-- //插入db数据，目前就一个
-- INSERT INTO DBS
-- VALUES ('dd','fborc');

-- 创建表类型表
CREATE TABLE IF NOT EXISTS TBL_TYPE
(
    TYPE_ID   VARCHAR(17) not null UNIQUE,
    TYPE_NAME VARCHAR(17) not null UNIQUE,
    PRIMARY KEY (TYPE_ID)
);
-- 插入数据
insert into TBL_TYPE
VALUES ('1', 'table'),
       ('2', 'index');;


-- 创建TBLS表
CREATE TABLE IF NOT EXISTS TBLS
(
    TBL_ID       VARCHAR(17) not null unique,
    DB_ID        VARCHAR(17) not null,
    TBL_NAME     VARCHAR(128) not null,
    TYPE_ID      VARCHAR(17) not null,
    PRIMARYKEYS  VARCHAR(128),
    SORTFIELDS   TEXT,
    SORTTYPE     VARCHAR(7),
    COMPRESSTYPE VARCHAR(17),
    ORCSIZE      INT,
    PRIMARY KEY (DB_ID, TBL_NAME),
    FOREIGN KEY (TYPE_ID) REFERENCES TBL_TYPE (TYPE_ID),
    FOREIGN KEY (DB_ID) REFERENCES DBS (DB_ID)
);


-- 创建表属性表
CREATE TABLE IF NOT EXISTS TABLE_PARAMS
(
    TBL_ID      VARCHAR(17),
    PARAM_KEY   VARCHAR(128),
    PARAM_VALUE VARCHAR(128),
    PRIMARY KEY (TBL_ID,
                 PARAM_KEY),
    FOREIGN KEY (TBL_ID) REFERENCES TBLS (TBL_ID)
);


-- 创建IDXS表
create table if not exists IDXS
(
    INDEX_ID    VARCHAR(17) not null unique,
    DB_ID       VARCHAR(17) not null,
    INDEX_NAME  VARCHAR(128) not null unique,
    ORIG_TBL_ID VARCHAR(17) not null,
    primary key (DB_ID, INDEX_NAME),
    FOREIGN KEY (ORIG_TBL_ID) REFERENCES TBLS (TBL_ID),
    FOREIGN KEY (DB_ID) REFERENCES DBS (DB_ID)
);


-- 创建索引属性表
CREATE TABLE IF NOT EXISTS INDEX_PARAMS
(
    INDEX_ID    VARCHAR(17),
    PARAM_KEY   VARCHAR(128),
    PARAM_VALUE VARCHAR(128),
    PRIMARY KEY (INDEX_ID,
                 PARAM_KEY),
    FOREIGN KEY (INDEX_ID) REFERENCES IDXS (INDEX_ID)
);

--创建用户自定义数据类型表
CREATE TABLE IF NOT EXISTS FMDB_CT
(
    CT_NAME   VARCHAR(17),
    WRITER    VARCHAR(128) not null,
    READ      VARCHAR(128) not null,
    BASE_TYPE VARCHAR(17) not null,
    IS_UDCT   BOOLEAN not null,
    PRIMARY KEY (CT_NAME)
);
-- 插入数据
insert into FMDB_CT
VALUES ('boolean', 'writer', 'reader', 'boolean', false),
       ('tinyint', 'writer', 'reader', 'tinyint', false),
       ('smallint', 'writer', 'reader', 'smallint', false),
       ('int', 'writer', 'reader', 'int', false),
       ('bigint', 'writer', 'reader', 'bigint', false),
       ('float', 'writer', 'reader', 'float', false),
       ('double', 'writer', 'reader', 'double', false),
       ('string', 'writer', 'reader', 'string', false),
       ('date', 'writer', 'reader', 'date', false),
       ('timestamp', 'writer', 'reader', 'timestamp', false),
       ('binary', 'writer', 'reader', 'binary', false),
       ('decimal', 'writer', 'reader', 'decimal', false),
       ('varchar', 'writer', 'reader', 'varchar', false),
       ('char', 'writer', 'reader', 'char', false),
       ('bsid', 'org.apache.orc.impl.writer.MACTreeWriter', 'org.apache.orc.impl.MACTreeReader', 'string', true),
       ('mac', 'org.apache.orc.impl.writer.MACTreeWriter', 'org.apache.orc.impl.MACTreeReader', 'string', true),
       ('mobile', 'org.apache.orc.impl.writer.TelTreeWriter', 'org.apache.orc.impl.TelTreeReader', 'string', true),
       ('ip', 'org.apache.orc.impl.writer.IPTreeWriter', 'org.apache.orc.impl.IPTreeReader', 'string', true),
       ('idcard', 'org.apache.orc.impl.writer.IDCARDTreeWriter', 'org.apache.orc.impl.IDCARDTreeReader', 'string', true),
       ('email', 'org.apache.orc.impl.writer.EmailTreeWriter', 'org.apache.orc.impl.EmailTreeReader', 'string', true),
       ('carno', 'org.apache.orc.impl.writer.CarNoTreeWriter', 'org.apache.orc.impl.CarNoTreeReader', 'string', true),
       ('civilcarno', 'org.apache.orc.impl.writer.CarNumTreeWriter', 'org.apache.orc.impl.CarNumTreeReader', 'string', true);

-- 创建字段表
CREATE TABLE IF NOT EXISTS COLUMNS
(
    COL_ID       VARCHAR(17) NOT NULL,
    COMMENT      VARCHAR(128),
    COLUMN_NAME  VARCHAR(128) NOT NULL,
    TYPE_NAME    VARCHAR(17) NOT NULL,
    INTEGER_IDX  INT NOT NULL,
    ISNUL        boolean NOT NULL,
    PRECISION    INT,
    SCALE        INT,
    TBL_ID       VARCHAR(17) NOT NULL,
    STORAGE_TYPE VARCHAR(17) NOT NULL,
    BASE_TYPE  VARCHAR(17) NOT NULL,
    PRIMARY KEY (COL_ID),
    FOREIGN KEY (TBL_ID) references TBLS (TBL_ID),
    FOREIGN KEY (TYPE_NAME) references FMDB_CT (CT_NAME)
);

--分区表
CREATE TABLE IF NOT EXISTS PARTITIONS
(
    PART_ID   VARCHAR(17) NOT NULL,
    COL_ID    VARCHAR(17),
    PART_TYPE VARCHAR(7),
    TTL       INT,
    PRIMARY KEY (PART_ID)
);
--分区参数
CREATE TABLE IF NOT EXISTS PARTITION_PARAMS
(
    PART_ID     VARCHAR(17),
    PARAM_KEY   VARCHAR(128),
    PARAM_VALUE VARCHAR(128),
    PRIMARY KEY (PART_ID,
                 PARAM_KEY)
);

--manifest表，用于存放orc元数据信息
CREATE TABLE IF NOT EXISTS MANIFEST
(
    DB_NAME    VARCHAR(128),
    TBL_NAME   VARCHAR(128),
    PART_NAME  VARCHAR(128),
    ORC_NAME   TEXT,
    COL_NAME   TEXT,
    MIN        TEXT,
    MAX        TEXT,
    NOTNULLNUM INT8,
    HASNULL    BOOLEAN,
    NULLNUM    INT8,
    STAT       BOOLEAN DEFAULT (false),
    primary key (DB_NAME, TBL_NAME, PART_NAME, ORC_NAME, COL_NAME)
);

--数据量统计
-- CREATE TABLE IF NOT EXISTS TBL_COUNT
-- (
--     DB_NAME   TEXT,
--     TBL_NAME  TEXT,
--     PART_NAME TEXT,
--     TBLCOUNT  INT8,
--     primary key (DB_NAME, TBL_NAME, PART_NAME)
-- );

--表中每列统计
CREATE TABLE IF NOT EXISTS TBL_COLUMN_COUNT
 (
    DB_NAME   VARCHAR(28),
    TBL_NAME  VARCHAR(128) not null,
    COLUMN_NAME VARCHAR(128) not null,
    COL_COUNT  INT8
 );
