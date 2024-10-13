USE metadata;
GO
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Darren Price
Created: 2024-05-24
Name: usp_CreateServerlessBaseConfig
Description: Parameterized CETAS query that takes in 3 parameters and has 2 main outcomes:
1. Creates any database, creates master key and scoped credential that don't already exist
2. Creates any schema that does't already exist
3. Creates any external data sources and external file formats that don't already exist
===================================================================================
Change History

Date		Name			Description
2024-05-24	Darren Price	Initial Version
2024-07-24	Darren Price	Added collation option
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
2024-10-04	Darren Price	Added parameters for datalake container naming
===================================================================================
*/
CREATE OR ALTER   PROCEDURE [Config].[usp_CreateServerlessBaseConfig] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_DATABASE_COLLATION nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_STORAGE_ACCOUNT_NAME nvarchar(50) = NULL,
	@PARAM_CONTAINER_NAME_RAW VARCHAR(150) = 'raw',
    @PARAM_CONTAINER_NAME_ENRICHED VARCHAR(150) = 'enriched',
	@PARAM_CONTAINER_NAME_CURATED VARCHAR(150) = 'curated'
)
AS

BEGIN
    DECLARE @sqlDatabase nvarchar(MAX);
    DECLARE @sqlMasterKey nvarchar(MAX);
    DECLARE @sqlManagedIdentity nvarchar(MAX);
    DECLARE @sqlSchema nvarchar(MAX);
    DECLARE @sqlExternalDataSourceMetadata nvarchar(MAX);
    DECLARE @sqlExternalDataSourceRaw nvarchar(MAX);
    DECLARE @sqlExternalDataSourceEnriched nvarchar(MAX);
    DECLARE @sqlExternalDataSourceCurated nvarchar(MAX);
    DECLARE @sqlExternalFileFormatCSV nvarchar(MAX);
    DECLARE @sqlExternalFileFormatParquet nvarchar(MAX);
    DECLARE @sqlExternalFileFormatDelta nvarchar(MAX);

    SET @sqlDatabase = 
    N'
    USE [master];

    IF NOT EXISTS (SELECT [name] FROM sys.databases WHERE [name] = '''+ @PARAM_TARGET_DATABASE_NAME +''')
    BEGIN
        EXECUTE(''CREATE DATABASE ['+ @PARAM_TARGET_DATABASE_NAME +'] COLLATE '+ @PARAM_TARGET_DATABASE_COLLATION +';'');
    END;
    '

    SET @sqlMasterKey = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT symmetric_key_id FROM sys.symmetric_keys WHERE symmetric_key_id = 101)
    BEGIN
        CREATE MASTER KEY;
    END;
    '

    SET @sqlManagedIdentity = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] from sys.database_scoped_credentials WHERE [name] = ''cred_managed_identity'')
    BEGIN
        CREATE DATABASE SCOPED CREDENTIAL [cred_managed_identity]
        WITH IDENTITY = ''MANAGED IDENTITY'';
    END;
    '

    SET @sqlSchema = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name] = '''+ @PARAM_TARGET_SCHEMA_NAME +''')
    BEGIN
        EXECUTE(''CREATE SCHEMA ['+ @PARAM_TARGET_SCHEMA_NAME +']'');
    END;
    '

    SET @sqlExternalDataSourceMetadata = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_data_sources WHERE [name] = ''exds_gen2_metadata'')
    BEGIN
        CREATE EXTERNAL DATA SOURCE [exds_gen2_metadata]
        WITH (
            LOCATION = N''https://'+ @PARAM_STORAGE_ACCOUNT_NAME +'.dfs.core.windows.net/metadata'',
            CREDENTIAL = [cred_managed_identity]
        );
    END;
    '
    SET @sqlExternalDataSourceRaw = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_data_sources WHERE [name] = ''exds_gen2_'+ @PARAM_CONTAINER_NAME_RAW +''')
    BEGIN
        CREATE EXTERNAL DATA SOURCE [exds_gen2_'+ @PARAM_CONTAINER_NAME_RAW +']
        WITH (
            LOCATION = N''https://'+ @PARAM_STORAGE_ACCOUNT_NAME +'.dfs.core.windows.net/'+ @PARAM_CONTAINER_NAME_RAW +''',
            CREDENTIAL = [cred_managed_identity]
        );
    END;
    '
    SET @sqlExternalDataSourceEnriched = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_data_sources WHERE [name] = ''exds_gen2_'+ @PARAM_CONTAINER_NAME_ENRICHED +''')
    BEGIN
        CREATE EXTERNAL DATA SOURCE [exds_gen2_'+ @PARAM_CONTAINER_NAME_ENRICHED +']
        WITH (
            LOCATION = N''https://'+ @PARAM_STORAGE_ACCOUNT_NAME +'.dfs.core.windows.net/'+ @PARAM_CONTAINER_NAME_ENRICHED +''',
            CREDENTIAL = [cred_managed_identity]
        );
    END;
    '
    SET @sqlExternalDataSourceCurated = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_data_sources WHERE [name] = ''exds_gen2_'+ @PARAM_CONTAINER_NAME_CURATED +''')
    BEGIN
        CREATE EXTERNAL DATA SOURCE [exds_gen2_'+ @PARAM_CONTAINER_NAME_CURATED +']
        WITH (
            LOCATION = N''https://'+ @PARAM_STORAGE_ACCOUNT_NAME +'.dfs.core.windows.net/'+ @PARAM_CONTAINER_NAME_CURATED +''',
            CREDENTIAL = [cred_managed_identity]
        );
    END;
    '

    SET @sqlExternalFileFormatCSV = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_file_formats WHERE [name] = ''exff_csv'')
    BEGIN
        CREATE EXTERNAL FILE FORMAT [exff_csv]
        WITH (
            FORMAT_TYPE = DELIMITEDTEXT,
            FORMAT_OPTIONS (FIELD_TERMINATOR = '',''),
            DATA_COMPRESSION = ''org.apache.hadoop.io.compress.GzipCodec''
        );
    END;
    '
    SET @sqlExternalFileFormatParquet = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_file_formats WHERE [name] = ''exff_parquet'')
    BEGIN
        CREATE EXTERNAL FILE FORMAT [exff_parquet]
        WITH (
            FORMAT_TYPE = PARQUET,
            DATA_COMPRESSION = ''org.apache.hadoop.io.compress.SnappyCodec''
        );
    END;
    '
    SET @sqlExternalFileFormatDelta = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.external_file_formats WHERE [name] = ''exff_delta'')
    BEGIN
        CREATE EXTERNAL FILE FORMAT [exff_delta]
        WITH (
            FORMAT_TYPE = DELTA
        );
    END;
    '

    EXECUTE sp_executesql @sqlDatabase;
    EXECUTE sp_executesql @sqlMasterKey;
    EXECUTE sp_executesql @sqlManagedIdentity;
    EXECUTE sp_executesql @sqlSchema;
    EXECUTE sp_executesql @sqlExternalDataSourceMetadata;
    EXECUTE sp_executesql @sqlExternalDataSourceRaw;
    EXECUTE sp_executesql @sqlExternalDataSourceEnriched;
    EXECUTE sp_executesql @sqlExternalDataSourceCurated;
    EXECUTE sp_executesql @sqlExternalFileFormatCSV;
    EXECUTE sp_executesql @sqlExternalFileFormatParquet;
    EXECUTE sp_executesql @sqlExternalFileFormatDelta;

END
GO
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Darren Price
Created: 2024-05-24
Name: usp_CreateExternalTable
Description: Parameterized CETAS query that takes in 7 parameters and has 2 outcome:
1. Creates required schema if it doesn't exist.
2. Creates an external table in a serverless lakehouse database (@PARAM_TARGET_DATABASE_NAME) with the specified 
lakehouse schema (@PARAM_TARGET_SCHEMA_NAME) and external table name (@PARAM_TARGET_TABLE_NAME).
To do so, it's using the specified external data source (@EXTERNAL_DATA_SOURCE)
and external file format (@EXTERNAL_FILE_FORMAT) and create columns config from (@PARAM_SQL_QUERY)
===================================================================================
Change History

Date		Name			Description
2024-05-24	Darren Price	Initial Version
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
===================================================================================
*/
CREATE OR ALTER PROCEDURE [Config].[usp_CreateExternalTable] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_TABLE_NAME nvarchar(50) = NULL,
    @PARAM_ADLS_LOCATION_PATH nvarchar(500) = NULL,
    @PARAM_EXTERNAL_DATA_SOURCE nvarchar(50) = NULL,
    @PARAM_EXTERNAL_FILE_FORMAT nvarchar(50) = NULL,
    @PARAM_SQL_QUERY nvarchar(MAX) = NULL
)
AS

BEGIN
    DECLARE @sqlSchema nvarchar(MAX);
    DECLARE @sqlExternalTable nvarchar(MAX);

    SET @sqlSchema = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name] = '''+ @PARAM_TARGET_SCHEMA_NAME +''')
    BEGIN
        EXECUTE(''CREATE SCHEMA ['+ @PARAM_TARGET_SCHEMA_NAME +']'');
    END;
    '

    SET @sqlExternalTable = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_TABLE_NAME +']'') AND type in (N''U''))
    BEGIN
        DROP EXTERNAL TABLE ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_TABLE_NAME +'];  
    END
    CREATE EXTERNAL TABLE ['+ @PARAM_TARGET_SCHEMA_NAME +'].[' + @PARAM_TARGET_TABLE_NAME +'] (
    '+ @PARAM_SQL_QUERY +'
    )
    WITH (
        LOCATION = '''+ @PARAM_ADLS_LOCATION_PATH +''',
        DATA_SOURCE = '+ @PARAM_EXTERNAL_DATA_SOURCE +',  
        FILE_FORMAT = '+ @PARAM_EXTERNAL_FILE_FORMAT +'
    );
    '

    EXECUTE sp_executesql @sqlSchema;
    EXECUTE sp_executesql @sqlExternalTable;

END
GO
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Andrei Dumitru
Created: 2024-03-25
Name: usp_CreateExternalTableAsSelect
Description: Parameterized CETAS query that takes in 7 parameters and has 4 outcomes:
1. Creates required schema if it doesn't exist.
2. Calculates the SQL logic (@SQL_QUERY) provided at the time of the run.
3. Saves the result as parquet in the specified location (@PARAM_ADLS_LOCATION_PATH).
4. Creates an external table in a serverless lakehouse database (@PARAM_TARGET_DATABASE_NAME) with the specified 
lakehouse schema (@PARAM_TARGET_SCHEMA_NAME) and external table name (@PARAM_TARGET_TABLE_NAME).
To do so, it's using the specified external data source (@EXTERNAL_DATA_SOURCE)
and external file format (@EXTERNAL_FILE_FORMAT).
===================================================================================
Change History

Date		Name			Description
2024-03-25	Andrei Dumitru	Initial Version
2024-03-28	Darren Price	Renamed prodecure and parameters
2024-05-20	Darren Price	Added create schema config
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
===================================================================================
*/
CREATE OR ALTER PROCEDURE [Config].[usp_CreateExternalTableAsSelect] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_TABLE_NAME nvarchar(50) = NULL,
    @PARAM_ADLS_LOCATION_PATH nvarchar(500) = NULL,
    @PARAM_EXTERNAL_DATA_SOURCE nvarchar(50) = NULL,
    @PARAM_EXTERNAL_FILE_FORMAT nvarchar(50) = NULL,
    @PARAM_SQL_QUERY nvarchar(MAX) = NULL
)
AS

BEGIN
    DECLARE @sqlSchema nvarchar(MAX);
    DECLARE @sqlExternalTable nvarchar(MAX);

    SET @sqlSchema = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name] = '''+ @PARAM_TARGET_SCHEMA_NAME +''')
    BEGIN
        EXECUTE(''CREATE SCHEMA ['+ @PARAM_TARGET_SCHEMA_NAME +']'');
    END;
    '

    SET @sqlExternalTable = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_TABLE_NAME +']'') AND type in (N''U''))
    BEGIN
        DROP EXTERNAL TABLE ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_TABLE_NAME +'];  
    END
    CREATE EXTERNAL TABLE ['+ @PARAM_TARGET_SCHEMA_NAME +'].[' + @PARAM_TARGET_TABLE_NAME +']

    WITH (
        LOCATION = '''+ @PARAM_ADLS_LOCATION_PATH +''',
        DATA_SOURCE = '+ @PARAM_EXTERNAL_DATA_SOURCE +',  
        FILE_FORMAT = '+ @PARAM_EXTERNAL_FILE_FORMAT +'
    ) 
    AS

    '+ @PARAM_SQL_QUERY +';
    '

    EXECUTE sp_executesql @sqlSchema;
    EXECUTE sp_executesql @sqlExternalTable;

END
GO
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Darren Price
Created: 2024-05-24
Name: usp_ExecuteStoredProcedure
Description: Parameterized CETAS query that takes in 3 parameters and has 1 main outcomes:
1. Executes provided stored procedure (@PARAM_STORED_PROCEDURE_NAME)
on provided database @PARAM_TARGET_DATABASE_NAME and schema @PARAM_TARGET_SCHEMA_NAME
===================================================================================
Change History

Date		Name			Description
2024-05-31	Darren Price	Initial Version
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
===================================================================================
*/
CREATE OR ALTER PROCEDURE [Config].[usp_ExecuteStoredProcedure] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_STORED_PROCEDURE_NAME nvarchar(150) = NULL
)
AS

BEGIN
    DECLARE @sqlExecute nvarchar(MAX);

    SET @sqlExecute = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    EXECUTE ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_STORED_PROCEDURE_NAME +'];
    '

    EXECUTE sp_executesql @sqlExecute;

END
GO
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Darren Price
Created: 2024-06-20
Name: usp_CreateOrAlterView
Description: Parameterized create or alter view query that takes in 4 parameters and has 2 outcomes:
1. Creates required schema if it doesn't exist.
2. Creates or Alters a view in the serverless lakehouse database (@PARAM_TARGET_DATABASE_NAME) with the specified 
lakehouse schema (@PARAM_TARGET_SCHEMA_NAME) and view name (@PARAM_TARGET_VIEW_NAME).
(@PARAM_SQL_QUERY) holds the required config to create
===================================================================================
Change History

Date		Name			Description
2024-06-20	Darren Price	Initial Version
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
===================================================================================
*/
CREATE OR ALTER PROCEDURE [Config].[usp_CreateOrAlterView] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_VIEW_NAME nvarchar(50) = NULL,
    @PARAM_SQL_QUERY nvarchar(MAX) = NULL
)
AS

BEGIN
    DECLARE @sqlSchema nvarchar(MAX);
    DECLARE @sqlCreateOrAlterView nvarchar(MAX);

    SET @sqlSchema = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name] = '''+ @PARAM_TARGET_SCHEMA_NAME +''')
    BEGIN
        EXECUTE(''CREATE SCHEMA ['+ @PARAM_TARGET_SCHEMA_NAME +']'');
    END;
    '

    SET @sqlCreateOrAlterView = 
    N'
    CREATE OR ALTER VIEW ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_VIEW_NAME +'] AS
    '+ @PARAM_SQL_QUERY +';
    '

    EXECUTE sp_executesql @sqlSchema;
    EXECUTE (N'USE ['+ @PARAM_TARGET_DATABASE_NAME +']; EXECUTE sp_executesql N'''+ @sqlCreateOrAlterView +'''');

END
GO
-- ===============================================================================================
-- ===============================================================================================
-- ===============================================================================================
-- ===============================================================================================
/*
===================================================================================
Author: Darren Price
Created: 2024-07-12
Name: usp_CreateOrAlterViewOpenrowset
Description: Parameterized create or alter view query that takes in 12 parameters and has 2 outcomes:
1. Creates required schema if it doesn't exist.
2. Creates or Alters an openrowset view in the serverless lakehouse database (@PARAM_TARGET_DATABASE_NAME) with the specified 
lakehouse schema (@PARAM_TARGET_SCHEMA_NAME).
===================================================================================
Change History

Date		Name			Description
2024-07-12	Darren Price	Initial Version
2024-07-25	Darren Price	Updated syntax to add ; termination & replace EXEC with EXECUTE
===================================================================================
*/
CREATE OR ALTER PROCEDURE [Config].[usp_CreateOrAlterViewOpenrowset] (
    -- Add the parameters for the stored procedure here
    @PARAM_TARGET_DATABASE_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_SCHEMA_NAME nvarchar(50) = NULL,
    @PARAM_TARGET_VIEW_NAME nvarchar(50) = NULL,
    @PARAM_ADLS_LOCATION_PATH nvarchar(500) = NULL,
    @PARAM_EXTERNAL_DATA_SOURCE nvarchar(50) = NULL,
    @PARAM_FORMAT nvarchar(50) = 'CSV',
    @PARAM_CSV_PARSER_VERSION nvarchar(10) = '2.0',
    @PARAM_CSV_FIELDTERMINATOR nvarchar(10) = ',',
    @PARAM_CSV_HEADER_ROW bit = 0,
    @PARAM_CSV_FIRSTROW nvarchar(10) = '1',
    @PARAM_CSV_FIELDQUOTE nvarchar(10) = '"',
    @PARAM_CSV_ESCAPECHAR nvarchar(10) = NULL
)
AS

BEGIN
    DECLARE @sqlSchema nvarchar(MAX);
    DECLARE @sqlCreateOrAlterViewCSV nvarchar(MAX);
    DECLARE @sqlCreateOrAlterViewParquet nvarchar(MAX);
    DECLARE @sqlCreateOrAlterViewDelta nvarchar(MAX);
    DECLARE @csvHeaderRow nvarchar(MAX);
    DECLARE @csvFirstRow nvarchar(MAX);

    SET @csvHeaderRow = CASE WHEN @PARAM_CSV_HEADER_ROW = 1 THEN 'TRUE' ELSE 'FALSE' END;
    SET @csvFirstRow = CASE WHEN @PARAM_CSV_HEADER_ROW = 1 THEN '1' ELSE @PARAM_CSV_FIRSTROW END;

    SET @sqlSchema = 
    N'
    USE ['+ @PARAM_TARGET_DATABASE_NAME +'];

    IF NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name] = '''+ @PARAM_TARGET_SCHEMA_NAME +''')
    BEGIN
        EXECUTE(''CREATE SCHEMA ['+ @PARAM_TARGET_SCHEMA_NAME +']'');
    END;
    '

    SET @sqlCreateOrAlterViewCSV = 
    N'
    CREATE OR ALTER VIEW ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_VIEW_NAME +'] AS
    SELECT *
    FROM OPENROWSET (
        BULK '''''+ @PARAM_ADLS_LOCATION_PATH +''''',
        DATA_SOURCE = '''''+ @PARAM_EXTERNAL_DATA_SOURCE +''''',
        FORMAT = '''''+ @PARAM_FORMAT +''''',
        PARSER_VERSION = '''''+ @PARAM_CSV_PARSER_VERSION +''''',
        FIELDTERMINATOR = '''''+ @PARAM_CSV_FIELDTERMINATOR +''''',
        HEADER_ROW = '+ @csvHeaderRow +',
        FIRSTROW = '+ @csvFirstRow +',
        FIELDQUOTE = '''''+ @PARAM_CSV_FIELDQUOTE +''''',
        ESCAPECHAR = '''''+ @PARAM_CSV_ESCAPECHAR +'''''
    ) AS [result];
    '

    SET @sqlCreateOrAlterViewParquet = 
    N'
    CREATE OR ALTER VIEW ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_VIEW_NAME +'] AS
    SELECT *
    FROM OPENROWSET (
        BULK ('''''+ @PARAM_ADLS_LOCATION_PATH +'''''),
        DATA_SOURCE = '''''+ @PARAM_EXTERNAL_DATA_SOURCE +''''',
        FORMAT = '''''+ @PARAM_FORMAT +'''''
    ) AS [result];
    '

    SET @sqlCreateOrAlterViewDelta = 
    N'
    CREATE OR ALTER VIEW ['+ @PARAM_TARGET_SCHEMA_NAME +'].['+ @PARAM_TARGET_VIEW_NAME +'] AS
    SELECT *
    FROM OPENROWSET (
        BULK ('''''+ @PARAM_ADLS_LOCATION_PATH +'''''),
        DATA_SOURCE = '''''+ @PARAM_EXTERNAL_DATA_SOURCE +''''',
        FORMAT = '''''+ @PARAM_FORMAT +'''''
    ) AS [result];
    '

    EXECUTE sp_executesql @sqlSchema;
    IF @PARAM_FORMAT = 'CSV'
        EXECUTE (N'USE ['+ @PARAM_TARGET_DATABASE_NAME +'];EXECUTE sp_executesql N'''+ @sqlCreateOrAlterViewCSV +''';');
    IF @PARAM_FORMAT = 'Parquet'
        EXECUTE (N'USE ['+ @PARAM_TARGET_DATABASE_NAME +'];EXECUTE sp_executesql N'''+ @sqlCreateOrAlterViewParquet +''';');
    IF @PARAM_FORMAT = 'Delta'
        EXECUTE (N'USE ['+ @PARAM_TARGET_DATABASE_NAME +'];EXECUTE sp_executesql N'''+ @sqlCreateOrAlterViewDelta +''';');

END
GO
-- ===============================================================================================
-- ===============================================================================================