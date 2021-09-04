\set ON_ERROR_STOP on
\set rundate `echo \'$RunDate\'::date`
\echo rundate = :rundate
BEGIN TRANSACTION;
--updating 'INPROGRESS' ETL batchids status to  'FAILED'
update ami.etl_load set load_status = 'FAILED' ,description='Dimension Load Failed'
from (select
param_val
from ami_stage.amigp_parameters where param_name like '$$tgt_tab%' and
sess_name='s_SLWRS_WMS_DETAILS_Upsert') sess where table_name=sess.param_val and load_status = 'INPROGRESS'
and src_file_name='wms_slwrs_details.txt' ; 
--inserting Audit details for Current batch 
INSERT INTO ami.etl_load(
  process_seq, etl_batch_id, worker_id, batch_size, rundate, process_type , schema_name, table_name, src_file_name, load_start_time, load_status, etl_user,description, file_read_end_time, file_type )
  select 1 process_seq
     , (select nextval('ami.etl_load_batch_id_seq')::bigint) etl_batch_id
         , null worker_id
         , null batch_size
     , :rundate rundate
         , 'DIM' process_type
         , 'slwrs_app_owner' schema_name
         , 'SLWR_WMS_DETAILS' table_name
         , 'wms_slwrs_details.txt' src_file_name
         , current_timestamp load_start_time
         , 'INPROGRESS' load_status
         , current_user etl_user
         , 'Dimension Load Started' description
         , current_timestamp file_read_end_time
         , 'TXT' file_type
;
\set tch_sql_stmnt `echo "\COPY (select 'SUCCESS') TO '${SQLTOUCHFILE}';"`
:tch_sql_stmnt

COMMIT;
	