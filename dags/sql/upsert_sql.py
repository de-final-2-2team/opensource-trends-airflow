REPOSITORY_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    NODE_ID	varchar(50),
    REPO_NM	STRING,
    FULL_NM	STRING,
    OWNER_ID STRING,
    OWNER_NM STRING NULL,
    CREATED_AT varchar(30),
    UPDATED_AT varchar(30),
    PUSHED_AT STRING,
    LANG_NM	STRING NULL,
    STARGAZERS_CNT STRING,
    WATCHERS_CNT INT,
    FORKS_CNT INT,
    OPEN_ISSUE_CNT INT,
    SCORE FLOAT,
    COLLECTED_AT STRING,
    LIC_ID	string NULL
);

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO REPOSITORY_TB AS target
USING {{ params.table }} AS source
ON target.NODE_ID = source.NODE_ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.REPO_NM = source.REPO_NM,
        target.FULL_NM = source.FULL_NM,
        target.OWNER_ID = source.OWNER_ID,
        target.OWNER_NM = source.OWNER_NM,
        target.CREATED_AT = source.CREATED_AT,
        target.UPDATED_AT = source.UPDATED_AT,
        target.PUSHED_AT = source.PUSHED_AT,
        target.LANG_NM = source.LANG_NM,
        target.STARGAZERS_CNT = source.STARGAZERS_CNT,
        target.WATCHERS_CNT = source.WATCHERS_CNT,
        target.FORKS_CNT = source.FORKS_CNT,
        target.OPEN_ISSUE_CNT = source.OPEN_ISSUE_CNT,
        target.SCORE = source.SCORE,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.LIC_ID = source.LIC_ID
WHEN NOT MATCHED THEN
    INSERT (NODE_ID, REPO_NM, FULL_NM, OWNER_ID, OWNER_NM, CREATED_AT, UPDATED_AT, PUSHED_AT, LANG_NM, STARGAZERS_CNT, WATCHERS_CNT, FORKS_CNT, OPEN_ISSUE_CNT, SCORE, COLLECTED_AT, LIC_ID)
    VALUES (source.NODE_ID, source.REPO_NM, source.FULL_NM, source.OWNER_ID, source.OWNER_NM, source.CREATED_AT, source.UPDATED_AT, source.PUSHED_AT, source.LANG_NM, source.STARGAZERS_CNT, source.WATCHERS_CNT, source.FORKS_CNT, source.OPEN_ISSUE_CNT, source.SCORE, source.COLLECTED_AT, source.LIC_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


RELEASE_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    ID STRING NOT NULL,
    REL_NM STRING NOT NULL,
    COLLECTED_AT STRING NOT NULL,
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO RELEASE_TB AS target
USING {{ params.table }} AS source
ON target.ID = source.ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.REL_NM = source.REL_NM,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (ID, REL_NM, COLLECTED_AT, REPO_ID)
    VALUES (source.ID, source.REL_NM, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


PROJECT_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    ID VARCHAR(50) NOT NULL,
    PROJ_NM VARCHAR(50) NOT NULL,
    BODY STRING NULL,
    PROJ_NO INT NOT NULL,
    PROJ_ST VARCHAR(10) NOT NULL,
    CREATED_AT VARCHAR(30) NOT NULL,
    UPDATED_AT VARCHAR(30) NOT NULL,
    COLLECTED_AT STRING NOT NULL,
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO PROJECT_TB AS target
USING {{ params.table }} AS source
ON target.ID = source.ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.PROJ_NM = source.PROJ_NM,
        target.BODY = source.BODY,
        target.PROJ_NO = source.PROJ_NO,
        target.PROJ_ST = source.PROJ_ST,
        target.CREATED_AT = source.CREATED_AT,
        target.UPDATED_AT = source.UPDATED_AT,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (ID, PROJ_NM, BODY, PROJ_NO, PROJ_ST, CREATED_AT, UPDATED_AT, COLLECTED_AT, REPO_ID)
    VALUES (source.ID, source.PROJ_NM, source.BODY, source.PROJ_NO, source.PROJ_ST, source.CREATED_AT, source.UPDATED_AT, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


LICENSE_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    LIC_KEY VARCHAR(40),
    LIC_NM VARCHAR(50),
    SPDX_ID VARCHAR(100),
    COLLECTED_AT VARCHAR(25)
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO LICENSE_TB AS target
USING {{ params.table }} AS source
ON target.LIC_KEY = source.LIC_KEY
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.LIC_NM = source.LIC_NM,
        target.SPDX_ID = source.SPDX_ID,
        target.COLLECTED_AT = source.COLLECTED_AT
WHEN NOT MATCHED THEN
    INSERT (LIC_KEY, LIC_NM, SPDX_ID, COLLECTED_AT)
    VALUES (source.LIC_KEY, source.LIC_NM, source.SPDX_ID, source.COLLECTED_AT);

DROP TABLE IF EXISTS {{ params.table }};
"""


LANGUAGE_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    LANG_NM STRING NULL,
    LANG_BYTE INT NOT NULL,
    COLLECTED_AT STRING NOT NULL,
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;


MERGE INTO LANGUAGE_TB AS target
USING {{ params.table }} AS source
ON target.REPO_ID = source.REPO_ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.LANG_BYTE = source.LANG_BYTE,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.LANG_NM = source.LANG_NM
WHEN NOT MATCHED THEN
    INSERT (LANG_NM, LANG_BYTE, COLLECTED_AT, REPO_ID)
    VALUES (source.LANG_NM, source.LANG_BYTE, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


ISSUE_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    ID STRING NOT NULL,
    URL VARCHAR(200) NOT NULL,
    TITLE STRING NOT NULL,
    USER_ID VARCHAR(50) NOT NULL,
    USER_NM VARCHAR(50) NOT NULL,
    CREATED_AT VARCHAR(30) NOT NULL,
    UPDATED_AT VARCHAR(30),
    CLOSED_AT VARCHAR(30),
    COLLECTED_AT VARCHAR(30) NOT NULL,
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO ISSUE_TB AS target
USING {{ params.table }} AS source
ON target.ID = source.ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.URL = source.URL,
        target.TITLE = source.TITLE,
        target.USER_ID = source.USER_ID,
        target.USER_NM = source.USER_NM,
        target.CREATED_AT = source.CREATED_AT,
        target.UPDATED_AT = source.UPDATED_AT,
        target.CLOSED_AT = source.CLOSED_AT,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (ID, URL, TITLE, USER_ID, USER_NM, CREATED_AT, UPDATED_AT, CLOSED_AT, COLLECTED_AT, REPO_ID)
    VALUES (source.ID, source.URL, source.TITLE, source.USER_ID, source.USER_NM, source.CREATED_AT, source.UPDATED_AT, source.CLOSED_AT, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


FORK_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    ID STRING NOT NULL,
    FORK_NM STRING NOT NULL,
    OWNER_ID STRING,
    OWNER_NM STRING,
    URL VARCHAR(200) NOT NULL,
    CREATED_AT VARCHAR(30) NOT NULL,
    UPDATED_AT VARCHAR(30),
    COLLECTED_AT VARCHAR(30),
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO FORK_TB AS target
USING {{ params.table }} AS source
ON target.ID = source.ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.FORK_NM = source.FORK_NM,
        target.OWNER_ID = source.OWNER_ID,
        target.OWNER_NM = source.OWNER_NM,
        target.URL = source.URL,
        target.CREATED_AT = source.CREATED_AT,
        target.UPDATED_AT = source.UPDATED_AT,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (ID, FORK_NM, OWNER_ID, OWNER_NM, URL, CREATED_AT, UPDATED_AT, COLLECTED_AT, REPO_ID)
    VALUES (source.ID, source.FORK_NM, source.OWNER_ID, source.OWNER_NM
"""


COMMIT_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    ID STRING NOT NULL,
    URL VARCHAR(200),
    AUTHOR_ID VARCHAR(50),
    AUTHOR_NM VARCHAR(50),
    MASSAGE STRING NULL,
    COLLECTED_AT VARCHAR(30),
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO COMMIT_TB AS target
USING {{ params.table }} AS source
ON target.ID = source.ID
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.URL = source.URL,
        target.AUTHOR_ID = source.AUTHOR_ID,
        target.AUTHOR_NM = source.AUTHOR_NM,
        target.MASSAGE = source.MASSAGE,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (ID, URL, AUTHOR_ID, AUTHOR_NM, MASSAGE, COLLECTED_AT, REPO_ID)
    VALUES (source.ID, source.URL, source.AUTHOR_ID, source.AUTHOR_NM, source.MASSAGE, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


PR_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    URL VARCHAR(200) NOT NULL,
    TITLE STRING NOT NULL,
    USER_ID VARCHAR(50) NOT NULL,
    USER_NM VARCHAR(50) NOT NULL,
    STATE VARCHAR(10) NULL,
    CREATED_AT VARCHAR(30) NOT NULL,
    UPDATED_AT VARCHAR(30) NULL,
    CLOSED_AT VARCHAR(30) NULL,
    COLLECTED_AT VARCHAR(30) NULL,
    REPO_ID STRING NOT NULL
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO PR_TB AS target
USING {{ params.table }} AS source
ON (
    target.USER_ID = source.USER_ID
    AND target.CREATED_AT = source.CREATED_AT
    AND target.REPO_ID = source.REPO_ID
)
WHEN MATCHED AND target.COLLECTED_AT < source.COLLECTED_AT THEN
    UPDATE SET
        target.TITLE = source.TITLE,
        target.USER_ID = source.USER_ID,
        target.USER_NM = source.USER_NM,
        target.STATE = source.STATE,
        target.CREATED_AT = source.CREATED_AT,
        target.UPDATED_AT = source.UPDATED_AT,
        target.CLOSED_AT = source.CLOSED_AT,
        target.COLLECTED_AT = source.COLLECTED_AT,
        target.REPO_ID = source.REPO_ID
WHEN NOT MATCHED THEN
    INSERT (URL, TITLE, USER_ID, USER_NM, STATE, CREATED_AT, UPDATED_AT, CLOSED_AT, COLLECTED_AT, REPO_ID)
    VALUES (source.URL, source.TITLE, source.USER_ID, source.USER_NM, source.STATE, source.CREATED_AT, source.UPDATED_AT, source.CLOSED_AT, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""


CM_ACT_TB = """
CREATE OR REPLACE TEMPORARY TABLE {{ params.table }}
    (
    AUTHOR_NM STRING NULL,
    AUTHOR_TYPE STRING NULL,
    TOTAL_CNT INTEGER NULL,
    WEEK_UTC INTEGER NULL,
    ADD_CNT INTEGER NULL,
    DEL_CNT INTEGER NULL,
    COMMIT_CNT INTEGER,
    COLLECTED_AT STRING,
    REPO_ID STRING
    );

COPY INTO {{ params.table }}
from 's3://{{ params.bucket }}/{{ params.filename }}'
credentials=(AWS_KEY_ID='{{ params.AWS_KEY_ID}}', AWS_SECRET_KEY='{{ params.AWS_SECRET_KEY }}')
FILE_FORMAT = (type='parquet')
match_by_column_name = case_insensitive;

MERGE INTO CM_ACT_TB AS target
USING {{ params.table }} AS source
ON (
    target.AUTHOR_NM = source.AUTHOR_NM
    AND target.AUTHOR_TYPE = source.AUTHOR_TYPE
    AND target.TOTAL_CNT = source.TOTAL_CNT
    AND target.WEEK_UTC = source.WEEK_UTC
    AND target.ADD_CNT = source.ADD_CNT
    AND target.DEL_CNT = source.DEL_CNT
    AND target.COMMIT_CNT = source.COMMIT_CNT
    AND target.REPO_ID = source.REPO_ID
)
WHEN NOT MATCHED THEN
    INSERT (AUTHOR_NM, AUTHOR_TYPE, TOTAL_CNT, WEEK_UTC, ADD_CNT, DEL_CNT, COMMIT_CNT, COLLECTED_AT, REPO_ID)
    VALUES (source.AUTHOR_NM, source.AUTHOR_TYPE, source.TOTAL_CNT, source.WEEK_UTC, source.ADD_CNT, source.DEL_CNT, source.COMMIT_CNT, source.COLLECTED_AT, source.REPO_ID);

DROP TABLE IF EXISTS {{ params.table }};
"""
