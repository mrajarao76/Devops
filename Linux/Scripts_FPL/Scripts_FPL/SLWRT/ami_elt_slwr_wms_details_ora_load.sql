\set on_error_stop on
\set etl_batch_id `echo \'$ETLBatchID\'::int`

BEGIN TRANSACTION;

\echo truncating table ami_stage.slwr_wms_details_ora ...

select ami_stage.truncate('ami_stage.slwr_wms_details_ora');

\echo inserting into table ami_stage.slwr_wms_details_ora ...

insert into ami_stage.slwr_wms_details_ora
select
(case when ext.isInt_CD_WR = 'f' then NULL else ext.CD_WR::integer end),
CD_STATUS,
(case when ext.DT_CREATED_isdate = 'f' then NULL else ext.DT_CREATED::timestamp without time zone end),
(case when ext.DT_WR_COMPLETE_isdate = 'f' then NULL else ext.DT_WR_COMPLETE::timestamp without time zone end),
(case when ext.DT_699_COMPLETE_isdate = 'f' then NULL else ext.DT_699_COMPLETE::timestamp without time zone end),
(case when ext.DT_COMPLETE_isdate = 'f' then NULL else ext.DT_COMPLETE::timestamp without time zone end),
(case when ext.DT_CANCELLED_isdate = 'f' then NULL else ext.DT_CANCELLED::timestamp without time zone end),
MGR_AREA_CODE,
SRV_CTR_CODE,
CD_CREWHQ,
ID_OPER_ASSIGNED,
CD_CREW_COMPLETED,
CD_JOB,
ACTN_TKN,
FAC_ADDR,
LATITUDE,
LONGITUDE,
DVC_COOR,
FDR_NUM,
(case when ext.isInt_ID_COOR = 'f' then NULL else ext.ID_COOR::integer end),
CUST_ACCT_NUM,
SMRTLGHT_SERL_NUM,
AMI_DVC_NAME,
SMRTLGHT_CMPT_ID,
NIC_MAC_ADDR,
DVC_UTIL_ID,
NO_EXT_SYS_ID,
TCKT_KEY,
(case when ext.isInt_TCKT_NUM = 'f' then NULL else ext.TCKT_NUM::integer end),
TCKT_TP,
TRBL_RPTD,
TCKT_CMNT,
ETL_BATCH_ID::bigint,
ETL_PRCS_TYPE,
(case when ext.ETL_PRCS_DTTM_isdate = 'f' then NULL else ext.ETL_PRCS_DTTM::timestamp without time zone end),
PRCS_FLAG,
CD_CREW_ASSIGN,
(case when ext.TS_UPDATE_isdate = 'f' then NULL else ext.TS_UPDATE::timestamp without time zone end),
(case when ext.DT_TCKT_CREATED_isdate = 'f' then NULL else ext.DT_TCKT_CREATED::timestamp without time zone end),
CD_ASSOC_DIST,
(case when ext.isInt_CD_ASSOC_WR = 'f' then NULL else ext.CD_ASSOC_WR::integer end)

from
(select 
CD_WR::integer,
CD_STATUS,
DT_CREATED::timestamp without time zone,
DT_WR_COMPLETE::timestamp without time zone,
DT_699_COMPLETE::timestamp without time zone,
DT_COMPLETE::timestamp without time zone,
DT_CANCELLED::timestamp without time zone,
MGR_AREA_CODE,
SRV_CTR_CODE,
CD_CREWHQ,
ID_OPER_ASSIGNED,
CD_CREW_COMPLETED,
CD_JOB,
ACTN_TKN,
FAC_ADDR,
LATITUDE,
LONGITUDE,
DVC_COOR,
FDR_NUM,
ID_COOR::integer,
CUST_ACCT_NUM,
SMRTLGHT_SERL_NUM,
AMI_DVC_NAME,
SMRTLGHT_CMPT_ID,
NIC_MAC_ADDR,
DVC_UTIL_ID,
NO_EXT_SYS_ID,
TCKT_KEY,
TCKT_NUM::integer,
TCKT_TP,
TRBL_RPTD,
TCKT_CMNT,
ETL_BATCH_ID::bigint,
ETL_PRCS_TYPE,
ETL_PRCS_DTTM::timestamp without time zone,
PRCS_FLAG,
CD_CREW_ASSIGN,
TS_UPDATE::timestamp without time zone,
DT_TCKT_CREATED::timestamp without time zone,
CD_ASSOC_DIST,
CD_ASSOC_WR::integer,
coalesce(nullif(ltrim(rtrim(CD_WR)), ''), 't') as isnull_CD_WR,

coalesce(nullif(coalesce(ltrim(rtrim(CD_WR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_CD_WR,
coalesce(nullif(coalesce(ltrim(rtrim(ID_COOR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_ID_COOR,
coalesce(nullif(coalesce(ltrim(rtrim(TCKT_NUM)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_TCKT_NUM,
coalesce(nullif(coalesce(ltrim(rtrim(CD_ASSOC_WR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_CD_ASSOC_WR,

coalesce(nullif(coalesce(ltrim(rtrim(DT_CREATED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_CREATED_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(dt_wr_complete)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as dt_wr_complete_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_699_COMPLETE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_699_COMPLETE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_COMPLETE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_COMPLETE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_CANCELLED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_CANCELLED_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(ETL_PRCS_DTTM)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as ETL_PRCS_DTTM_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(TS_UPDATE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as TS_UPDATE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_TCKT_CREATED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_TCKT_CREATED_isdate

 from ami_stage.ext_slwr_wms_details_ora) ext;
 
 
insert into AMI.AMI_PBLM_ERR_TBL
(PBLM_ERR_ID
,PBLM_TIMESTMP
,SRC_SYS_NAME
,INTRFC
,ETL_PRCS_NAME
,INPT_IMG
,PRCS_ERR_MSG
,REF_KEY
,REF_KEY_DS
,ETL_BATCH_ID
)
select  
'1111' as pblm_err_id
,CURRENT_TIMESTAMP	as	pblm_timestmp
,'slwr_wms_details_ora'	as	src_sys_name
,'slwr_wms_details_ora'	as 	intrfc
,'ami_elt_slwr_wms_details_ora_load.sql'	as	etl_prcs_name
,coalesce(ext.CD_WR, '')||'|'||coalesce(ext.ID_COOR, '')||'|'||coalesce(ext.TCKT_NUM, '')||'|'||
coalesce(ext.CD_ASSOC_WR, '')||'|'||coalesce(ext.DT_CREATED, '') 
||'|'||coalesce(ext.dt_wr_complete, '')||'|'||coalesce(ext.DT_699_COMPLETE, '')||'|'||
coalesce(ext.DT_COMPLETE, '')||'|'||coalesce(ext.DT_CANCELLED, '')||'|'||coalesce(ext.ETL_PRCS_DTTM, '')||'|'||
coalesce(ext.TS_UPDATE, '')||'|'||coalesce(ext.DT_TCKT_CREATED, '')as inpt_img
,coalesce(case when ext.isInt_ID_COOR = 'f' then ' Warning:ID_COOR is invalid,' end, '')||
coalesce(case when ext.isInt_CD_WR = 'f' or ext.isnull_CD_WR='t' then ' ERROR:CD_WR is invalid,' end, '')||
coalesce(case when ext.isInt_TCKT_NUM = 'f' then ' Warning:TCKT_NUM is invalid,' end, '')||
coalesce(case when ext.isInt_CD_ASSOC_WR = 'f' then 'Warning:CD_ASSOC_WR is invalid,' end, '')||
coalesce(case when ext.DT_CREATED_isdate = 'f' then 'Warning:DT_CREATED is invalid,' end, '')||
coalesce(case when ext.dt_wr_complete_isdate = 'f' then ' Warning:dt_wr_complete is invalid,' end, '')||
coalesce(case when ext.DT_699_COMPLETE_isdate = 'f' then ' Warning:DT_699_COMPLETE is invalid,' end, '')||
coalesce(case when ext.DT_COMPLETE_isdate = 'f' then 'Warning:DT_COMPLETE is invalid,' end, '')||
coalesce(case when ext.DT_CANCELLED_isdate = 'f' then 'Warning:DT_CANCELLED is invalid,' end, '')||
coalesce(case when ext.ETL_PRCS_DTTM_isdate = 'f' then ' Warning:ETL_PRCS_DTTM is invalid,' end, '')||
coalesce(case when ext.TS_UPDATE_isdate = 'f' then 'Warning:TS_UPDATE is invalid,' end, '')||
coalesce(case when ext.DT_TCKT_CREATED_isdate = 'f' then 'Warning:DT_TCKT_CREATED is invalid,' end, '')
as PRCS_ERR_MSG
,'' as REF_KEY
,'' as REF_KEY_DS
,:etl_batch_id as etl_batch_id
from (
select
CD_WR,
ID_COOR,
TCKT_NUM,
CD_ASSOC_WR,
DT_CREATED,
dt_wr_complete,
DT_699_COMPLETE,
DT_COMPLETE,
DT_CANCELLED,
ETL_PRCS_DTTM,
TS_UPDATE,
DT_TCKT_CREATED,

coalesce(nullif(ltrim(rtrim(CD_WR)), ''), 't') as isnull_CD_WR,

coalesce(nullif(coalesce(ltrim(rtrim(CD_WR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_CD_WR,
coalesce(nullif(coalesce(ltrim(rtrim(ID_COOR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_ID_COOR,
coalesce(nullif(coalesce(ltrim(rtrim(TCKT_NUM)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_TCKT_NUM,
coalesce(nullif(coalesce(ltrim(rtrim(CD_ASSOC_WR)),'1'),'' ),'1') ~ E'^[0-9]+$'  as isInt_CD_ASSOC_WR,

coalesce(nullif(coalesce(ltrim(rtrim(DT_CREATED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_CREATED_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(dt_wr_complete)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as dt_wr_complete_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_699_COMPLETE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_699_COMPLETE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_COMPLETE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_COMPLETE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_CANCELLED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_CANCELLED_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(ETL_PRCS_DTTM)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as ETL_PRCS_DTTM_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(TS_UPDATE)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as TS_UPDATE_isdate,
coalesce(nullif(coalesce(ltrim(rtrim(DT_TCKT_CREATED)),'01/01/9999 01:01:01'),'' ),'01/01/9999 01:01:01') 
~ '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' as DT_TCKT_CREATED_isdate

 from ami_stage.ext_slwr_wms_details_ora) ext
 where isnull_CD_WR='t' or isInt_CD_WR='f' or isInt_ID_COOR='f' or isInt_TCKT_NUM='f' or isInt_CD_ASSOC_WR='f' or DT_CREATED_isdate='f' or dt_wr_complete_isdate='f' or DT_699_COMPLETE_isdate='f' or DT_COMPLETE_isdate='f' or DT_CANCELLED_isdate='f' or ETL_PRCS_DTTM_isdate='f' or 
 TS_UPDATE_isdate='f' or DT_TCKT_CREATED_isdate='f';
 
 \copy (select 'SUCCESS') to '/home/ami0app/ami_scripts/bin/temp/slwr_wms_details_ora.txt';
 
 COMMIT;