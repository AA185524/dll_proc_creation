CREATE PROCEDURE [product].[product_master_proc] 
@pipeline_name [VARCHAR](100),
@pipeline_run_id [VARCHAR](100),
@pipeline_trigger_name [VARCHAR](100),
@pipeline_trigger_id [VARCHAR](100),
@pipeline_trigger_type [VARCHAR](100),
@pipeline_trigger_date_time_utc [DATETIME2],
AS
BEGIN TRY
--LOAD-TYPE: Incremental temp2trans
WITH gen_hashkey as (
    SELECT
    [create_timestamp] [varchar](35),
    [unspsc_code] [varchar](8),
    [last_update_id] [varchar](30),
    [last_update_timestamp] [varchar](35),
    [future_corporation_code] [varchar](4),
    [product_key] [decimal](38, 0),
    [product_type_key] [decimal](38, 0),
    [product_class_id] [varchar](4),
    [corporate_product_line_code] [varchar](4),
    [originating_company_code] [varchar](10),
    [generic_category_code] [varchar](2),
    [product_category_code] [varchar](2),
    [product_id] [varchar](30),
    [product_unformatted_id] [varchar](30),
    [sponsor_organization_key] [decimal](38, 0),
    [create_id] [varchar](30),
    FROM    [trans_product_gsdb_gsdb].[market_product_relationship_temp]
),
rn as (
    SELECT  *, ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY 
                 last_update_timestamp DESC,
				  infa_operation_time DESC,
                infa_sortable_sequence  DESC
        ) as _ELT_ROWNUMBERED
    FROM    gen_hashkey
),
data as (
    SELECT  *
    FROM    rn
    WHERE _ELT_ROWNUMBERED = 1
)
MERGE INTO    [trans_product_gsdb_gsdb].[market_product_relationship] tgt
USING (
    SELECT  *
    FROM    data
) src
ON ( src.[hash_key] = tgt.[hash_key] )
WHEN MATCHED THEN 
UPDATE SET
    [tgt].[create_timestamp] = [src].[create_timestamp],
    [tgt].[unspsc_code] = [src].[unspsc_code],
    [tgt].[last_update_id] = [src].[last_update_id],
    [tgt].[last_update_timestamp] = [src].[last_update_timestamp],
    [tgt].[future_corporation_code] = [src].[future_corporation_code],
    [tgt].[product_key] = [src].[product_key],
    [tgt].[product_type_key] = [src].[product_type_key],
    [tgt].[product_class_id] = [src].[product_class_id],
    [tgt].[corporate_product_line_code] = [src].[corporate_product_line_code],
    [tgt].[originating_company_code] = [src].[originating_company_code],
    [tgt].[generic_category_code] = [src].[generic_category_code],
    [tgt].[product_category_code] = [src].[product_category_code],
    [tgt].[product_id] = [src].[product_id],
    [tgt].[product_unformatted_id] = [src].[product_unformatted_id],
    [tgt].[sponsor_organization_key] = [src].[sponsor_organization_key],
    [tgt].[create_id] = [src].[create_id],
        [tgt].[ingest_partition] = [src].[ingest_partition],
        [tgt].[ingest_channel] = [src].[ingest_channel],
        [tgt].[file_path] = [src].[file_path],
        [tgt].[root_path] = [src].[root_path],
        [tgt].[trans_load_date_time_utc] = GETDATE(),
        [tgt].[adle_transaction_code] = [src].[infa_operation_type],
		[tgt].[infa_operation_time]=[src].[infa_operation_time],
	    [tgt].[infa_sortable_sequence]=[src].[infa_sortable_sequence],
        [tgt].[pipeline_name] = @pipeline_name,
        [tgt].[pipeline_run_id] = @pipeline_run_id,
        [tgt].[pipeline_trigger_name] = @pipeline_trigger_name,
        [tgt].[pipeline_trigger_id] = @pipeline_trigger_id,
        [tgt].[pipeline_trigger_type] = @pipeline_trigger_type,
        [tgt].[pipeline_trigger_date_time_utc] = @pipeline_trigger_date_time_utc
WHEN NOT MATCHED THEN 
    INSERT (
    [create_timestamp],
    [unspsc_code],
    [last_update_id],
    [last_update_timestamp],
    [future_corporation_code],
    [product_key],
    [product_type_key],
    [product_class_id],
    [corporate_product_line_code],
    [originating_company_code],
    [generic_category_code],
    [product_category_code],
    [product_id],
    [product_unformatted_id],
    [sponsor_organization_key],
    [create_id],
)VALUES(
    [src].[create_timestamp],
    [src].[unspsc_code],
    [src].[last_update_id],
    [src].[last_update_timestamp],
    [src].[future_corporation_code],
    [src].[product_key],
    [src].[product_type_key],
    [src].[product_class_id],
    [src].[corporate_product_line_code],
    [src].[originating_company_code],
    [src].[generic_category_code],
    [src].[product_category_code],
    [src].[product_id],
    [src].[product_unformatted_id],
    [src].[sponsor_organization_key],
    [src].[create_id],
        [src].[hash_key]
    );
END TRY
BEGIN CATCH
    DECLARE @db_name VARCHAR(200),
        @schema_name VARCHAR(200),
        @error_nbr INT,
        @error_severity INT,
        @error_state INT,
        @stored_proc_name VARCHAR(200),
        @error_message VARCHAR(8000),
        @created_date_time DATETIME2

    SET @db_name=DB_NAME()
    SET @schema_name=SUBSTRING (@pipeline_name, CHARINDEX('2', @pipeline_name) + 1, LEN(@pipeline_name) - CHARINDEX('2', @pipeline_name) - 3 )
    SET @error_nbr=ERROR_NUMBER()
    SET @error_severity=ERROR_SEVERITY()
    SET @error_state=ERROR_STATE()
    SET @stored_proc_name=ERROR_PROCEDURE()
    SET @error_message=ERROR_MESSAGE()
    SET @created_date_time=GETDATE()

    EXECUTE [adle_platform_orchestration].[elt_error_log_proc]
        @db_name,
        'ERROR',
        @schema_name,
        @error_nbr,
        @error_severity,
        @error_state,
        @stored_proc_name,
        'PROC',
        @error_message,
        @created_date_time,
        @pipeline_name,
        @pipeline_run_id,
        @pipeline_trigger_name,
        @pipeline_trigger_id,
        @pipeline_trigger_type,
        @pipeline_trigger_date_time_utc
        ;
    THROW;
END CATCH
;
GO
