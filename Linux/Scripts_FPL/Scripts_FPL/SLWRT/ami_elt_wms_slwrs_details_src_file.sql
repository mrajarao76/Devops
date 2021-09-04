\set ON_ERROR_STOP on

BEGIN TRANSACTION ;
\echo creating temporary table 'temp_wms_slwrs_details_src' from source query

create temporary table temp_wms_slwrs_details_src as 
    SELECT stg.cd_wr,
	NULLIF(TRIM(stg.cd_status),'') cd_status,
	stg.dt_created,
	stg.dt_wr_complete,
	stg.dt_699_complete,
	stg.dt_complete,
	stg.dt_cancelled,
	NULLIF(TRIM(stg.mgr_area_code),'') mgr_area_code,
	NULLIF(TRIM(stg.srv_ctr_code),'') srv_ctr_code,
	NULLIF(TRIM(stg.cd_crewhq),'') cd_crewhq,
	NULLIF(TRIM(stg.id_oper_assigned),'') id_oper_assigned,
	NULLIF(TRIM(stg.cd_crew_completed),'') cd_crew_completed,
	NULLIF(TRIM(stg.cd_job),'') cd_job,
	CASE WHEN TRIM(stg.actn_tkn)='' THEN NULL ELSE quote_ident(TRIM(stg.actn_tkn)) END AS actn_tkn,  --NULLIF(TRIM(stg.actn_tkn),'') actn_tkn,
	CASE WHEN TRIM(stg.fac_addr)='' THEN NULL ELSE quote_ident(TRIM(stg.fac_addr)) END AS fac_addr,--NULLIF(TRIM(stg.fac_addr),'') fac_addr,
	NULLIF(TRIM(stg.latitude),'') latitude,
	NULLIF(TRIM(stg.longitude),'') longitude,
	NULLIF(TRIM(stg.dvc_coor),'') dvc_coor,
	NULLIF(TRIM(stg.fdr_num),'') fdr_num,
	stg.id_coor,
	NULLIF(TRIM(stg.cust_acct_num),'') cust_acct_num,
	NULLIF(TRIM(stg.smrtlght_serl_num),'') smrtlght_serl_num,
	NULLIF(TRIM(stg.ami_dvc_name),'') ami_dvc_name,
	NULLIF(TRIM(stg.smrtlght_cmpt_id),'') smrtlght_cmpt,
	NULLIF(TRIM(stg.nic_mac_addr),'') nic_mac_addr,
	NULLIF(TRIM(stg.dvc_util_id),'') dvc_util_id,
	NULLIF(TRIM(stg.no_ext_sys_id),'') no_ext_sys_id,
	NULLIF(TRIM(stg.tckt_key),'') tckt_key,
	stg.tckt_num,
	NULLIF(TRIM(stg.tckt_tp),'') tckt_tp,
	CASE WHEN TRIM(stg.trbl_rptd)='' THEN NULL ELSE quote_ident(TRIM(stg.trbl_rptd)) END AS trbl_rptd,--NULLIF(TRIM(stg.trbl_rptd),'') trbl_rptd,
	CASE WHEN TRIM(stg.tckt_cmnt)='' THEN NULL ELSE quote_ident(TRIM(stg.tckt_cmnt)) END AS tckt_cmnt,--NULLIF(TRIM(stg.tckt_cmnt),'') tckt_cmnt,
	'I' etl_prcs_type,
	to_timestamp(current_timestamp,'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone etl_prcs_dttm,
	'Y' prcs_flag,
	NULLIF(TRIM(stg.cd_crew),'') cd_crew,
	stg.ts_update,
	stg.dt_tckt_created,
    NULLIF(TRIM(stg.cd_assoc_dist),'') cd_assoc_dist,
    stg.cd_assoc_wr,
    (select etl_batch_id from ami.etl_load where table_name='SLWR_WMS_DETAILS' and src_file_name='wms_slwrs_details.txt' and load_status='INPROGRESS') etl_batch_id		
    FROM ami_stage.slwr_wms_details_stg stg LEFT JOIN ami_stage.slwr_wms_details_ora ora
	ON ora.cd_wr = stg.cd_wr
    WHERE ora.cd_wr IS NULL
    UNION
    SELECT ora.cd_wr,
	ora.cd_status,
	ora.dt_created,
	ora.dt_wr_complete,
	ora.dt_699_complete,
	ora.dt_complete,
	ora.dt_cancelled,
	ora.mgr_area_code,
	ora.srv_ctr_code,
	ora.cd_crewhq,
	ora.id_oper_assigned,
	ora.cd_crew_completed,
	ora.cd_job,
	CASE WHEN TRIM(ora.actn_tkn)='' THEN NULL ELSE quote_ident(TRIM(ora.actn_tkn)) END AS actn_tkn,--ora.actn_tkn,
	CASE WHEN TRIM(ora.fac_addr)='' THEN NULL ELSE quote_ident(TRIM(ora.fac_addr)) END AS fac_addr,--ora.fac_addr,
	ora.latitude,
	ora.longitude,
	ora.dvc_coor,
	ora.fdr_num,
	ora.id_coor,
	ora.cust_acct_num,
	ora.smrtlght_serl_num,
	ora.ami_dvc_name,
	ora.smrtlght_cmpt_id,
	ora.nic_mac_addr,
	ora.dvc_util_id,
	ora.no_ext_sys_id,
	ora.tckt_key,
	ora.tckt_num,
	ora.tckt_tp,
	CASE WHEN TRIM(ora.trbl_rptd)='' THEN NULL ELSE quote_ident(TRIM(ora.trbl_rptd)) END AS trbl_rptd,--ora.trbl_rptd,
	CASE WHEN TRIM(ora.tckt_cmnt)='' THEN NULL ELSE quote_ident(TRIM(ora.tckt_cmnt)) END AS tckt_cmnt, --ora.tckt_cmnt,
	'D' etl_prcs_type,
	to_timestamp(current_timestamp,'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone etl_prcs_dttm,
	'Y' prcs_flag,
	ora.cd_crew,
	ora.ts_update,
	ora.dt_tckt_created,
    ora.cd_assoc_dist,
    ora.cd_assoc_wr,
   (select etl_batch_id from ami.etl_load where table_name='SLWR_WMS_DETAILS' and src_file_name='wms_slwrs_details.txt' and load_status='INPROGRESS') etl_batch_id	
   FROM ami_stage.slwr_wms_details_ora ora LEFT JOIN ami_stage.slwr_wms_details_stg stg 
   ON ora.cd_wr = stg.cd_wr
   WHERE stg.cd_wr IS NULL
   UNION
   SELECT stg.cd_wr,
	NULLIF(TRIM(stg.cd_status),'') cd_status,
	stg.dt_created,
	stg.dt_wr_complete,
	stg.dt_699_complete,
	stg.dt_complete,
	stg.dt_cancelled,
	NULLIF(TRIM(stg.mgr_area_code),'') mgr_area_code,
	NULLIF(TRIM(stg.srv_ctr_code),'') srv_ctr_code,
	NULLIF(TRIM(stg.cd_crewhq),'') cd_crewhq,
	NULLIF(TRIM(stg.id_oper_assigned),'') id_oper_assigned,
	NULLIF(TRIM(stg.cd_crew_completed),'') cd_crew_completed,
	NULLIF(TRIM(stg.cd_job),'') cd_job,
	CASE WHEN TRIM(stg.actn_tkn)='' THEN NULL ELSE quote_ident(TRIM(stg.actn_tkn)) END AS actn_tkn,--NULLIF(TRIM(stg.actn_tkn),'') actn_tkn,
	CASE WHEN TRIM(stg.fac_addr)='' THEN NULL ELSE quote_ident(TRIM(stg.fac_addr)) END AS fac_addr,--NULLIF(TRIM(stg.fac_addr),'') fac_addr,
	NULLIF(TRIM(stg.latitude),'') latitude,
	NULLIF(TRIM(stg.longitude),'') longitude,
	NULLIF(TRIM(stg.dvc_coor),'') dvc_coor,
	NULLIF(TRIM(stg.fdr_num),'') fdr_num,
	stg.id_coor,
	NULLIF(TRIM(stg.cust_acct_num),'') cust_acct_num,
	NULLIF(TRIM(stg.smrtlght_serl_num),'') smrtlght_serl_num,
	NULLIF(TRIM(stg.ami_dvc_name),'') ami_dvc_name,
	NULLIF(TRIM(stg.smrtlght_cmpt_id),'') smrtlght_cmpt,
	NULLIF(TRIM(stg.nic_mac_addr),'') nic_mac_addr,
	NULLIF(TRIM(stg.dvc_util_id),'') dvc_util_id,
	NULLIF(TRIM(stg.no_ext_sys_id),'') no_ext_sys_id,
	NULLIF(TRIM(stg.tckt_key),'') tckt_key,
	stg.tckt_num,
	NULLIF(TRIM(stg.tckt_tp),'') tckt_tp,
	CASE WHEN TRIM(stg.trbl_rptd)='' THEN NULL ELSE quote_ident(TRIM(stg.trbl_rptd)) END AS trbl_rptd,--NULLIF(TRIM(stg.trbl_rptd),'') trbl_rptd,
	CASE WHEN TRIM(stg.tckt_cmnt)='' THEN NULL ELSE quote_ident(stg.tckt_cmnt) END AS tckt_cmnt,--NULLIF(TRIM(stg.tckt_cmnt),'') tckt_cmnt,
	'U' etl_prcs_type,
	to_timestamp(current_timestamp,'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone etl_prcs_dttm,
	'Y' prcs_flag,
	NULLIF(TRIM(stg.cd_crew),'') cd_crew,
	stg.ts_update,
	stg.dt_tckt_created,
    NULLIF(TRIM(stg.cd_assoc_dist),'') cd_assoc_dist,
    stg.cd_assoc_wr,
   (select etl_batch_id from ami.etl_load where table_name='SLWR_WMS_DETAILS' and src_file_name='wms_slwrs_details.txt' and load_status='INPROGRESS') etl_batch_id
   FROM ami_stage.slwr_wms_details_stg stg JOIN ami_stage.slwr_wms_details_ora ora
	ON ora.cd_wr = stg.cd_wr
   WHERE COALESCE(NULLIF(TRIM(stg.cd_status),''),'*') <> COALESCE(ora.cd_status,'*')
	OR COALESCE(stg.dt_created,'9999-12-31') <> COALESCE(ora.dt_created,'9999-12-31')
	OR COALESCE(stg.dt_wr_complete,'9999-12-31') <> COALESCE(ora.dt_wr_complete,'9999-12-31')
	OR COALESCE(stg.dt_699_complete,'9999-12-31') <> COALESCE(ora.dt_699_complete,'9999-12-31')
	OR COALESCE(stg.dt_complete,'9999-12-31') <> COALESCE(ora.dt_complete,'9999-12-31')
	OR COALESCE(stg.dt_cancelled,'9999-12-31') <> COALESCE(ora.dt_cancelled,'9999-12-31')
	OR COALESCE(NULLIF(TRIM(stg.mgr_area_code),''),'*') <> COALESCE(ora.mgr_area_code,'*')
	OR COALESCE(NULLIF(TRIM(stg.srv_ctr_code),''),'*') <> COALESCE(ora.srv_ctr_code,'*')
	OR COALESCE(NULLIF(TRIM(stg.cd_crewhq),''),'*') <> COALESCE(ora.cd_crewhq,'*')
	OR COALESCE(NULLIF(TRIM(stg.id_oper_assigned),''),'*') <> COALESCE(ora.id_oper_assigned,'*')
	OR COALESCE(NULLIF(TRIM(stg.cd_crew_completed),''),'*') <> COALESCE(ora.cd_crew_completed,'*')
	OR COALESCE(NULLIF(TRIM(stg.cd_job),''),'*') <> COALESCE(ora.cd_job,'*')
	OR COALESCE(NULLIF(TRIM(stg.actn_tkn),''),'*') <> COALESCE(ora.actn_tkn,'*')
	OR COALESCE(NULLIF(TRIM(stg.fac_addr),''),'*') <> COALESCE(ora.fac_addr,'*')
	OR COALESCE(NULLIF(TRIM(stg.latitude),''),'*') <> COALESCE(ora.latitude,'*')
	OR COALESCE(NULLIF(TRIM(stg.longitude),''),'*') <> COALESCE(ora.longitude,'*')
	OR COALESCE(NULLIF(TRIM(stg.dvc_coor),''),'*') <> COALESCE(ora.dvc_coor,'*')
	OR COALESCE(NULLIF(TRIM(stg.fdr_num),''),'*') <> COALESCE(ora.fdr_num,'*')
	OR COALESCE(stg.id_coor,-123) <> COALESCE(ora.id_coor,-123)
	OR COALESCE(NULLIF(TRIM(stg.cust_acct_num),''),'*') <> COALESCE(ora.cust_acct_num,'*')
	OR COALESCE(NULLIF(TRIM(stg.smrtlght_serl_num),''),'*') <> COALESCE(ora.smrtlght_serl_num,'*')
	OR COALESCE(NULLIF(TRIM(stg.ami_dvc_name),''),'*') <> COALESCE(ora.ami_dvc_name,'*')
	OR COALESCE(NULLIF(TRIM(stg.smrtlght_cmpt_id),''),'*') <> COALESCE(ora.smrtlght_cmpt_id,'*')
	OR COALESCE(NULLIF(TRIM(stg.nic_mac_addr),''),'*') <> COALESCE(ora.nic_mac_addr,'*')
	OR COALESCE(NULLIF(TRIM(stg.dvc_util_id),''),'*') <> COALESCE(ora.dvc_util_id,'*')
	OR COALESCE(NULLIF(TRIM(stg.no_ext_sys_id),''),'*') <> COALESCE(ora.no_ext_sys_id,'*')
	OR COALESCE(NULLIF(TRIM(stg.tckt_key),''),'*') <> COALESCE(ora.tckt_key,'*')
	OR COALESCE(stg.tckt_num,-123) <> COALESCE(ora.tckt_num,-123)
	OR COALESCE(NULLIF(TRIM(stg.tckt_tp),''),'*') <> COALESCE(ora.tckt_tp,'*')
	OR COALESCE(NULLIF(TRIM(stg.trbl_rptd),''),'*') <> COALESCE(ora.trbl_rptd,'*')
	OR COALESCE(NULLIF(TRIM(stg.tckt_cmnt),''),'*') <> COALESCE(ora.tckt_cmnt,'*')
	OR COALESCE(NULLIF(TRIM(stg.cd_crew),''),'*') <> COALESCE(ora.cd_crew,'*')
	OR COALESCE(stg.ts_update,'9999-12-31') <> COALESCE(ora.ts_update,'9999-12-31')
	OR COALESCE(stg.dt_tckt_created,'9999-12-31') <> COALESCE(ora.dt_tckt_created,'9999-12-31')
	OR COALESCE(NULLIF(TRIM(stg.cd_assoc_dist),''),'*') <> COALESCE(ora.cd_assoc_dist,'*')
	OR COALESCE(stg.cd_assoc_wr,-123) <> COALESCE(ora.cd_assoc_wr,-123);
	
\echo writing data from temp table to file 
\copy (select * from temp_wms_slwrs_details_src) to '/informatica/InfaFiles/stage/ami/WMS/slwrs/wms_slwrs_details.txt' delimiter '|' NULL '' ;


COMMIT;	
	
