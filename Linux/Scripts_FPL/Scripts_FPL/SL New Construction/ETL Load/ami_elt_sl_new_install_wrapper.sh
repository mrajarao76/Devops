##############################################################################################
##############################################################################################
##################      SCRIPT_NAME   : ami_elt_wms_sl_new_install_dtl_wrapper.sh		######
##################      SCRIPT_OWNER  : Cognizant Technology Solutions           #############
##################      Description   : Wrapper script for ETL process.	    	 #############
##############################################################################################
##############################################################################################

export GPHOME=/greenplum/client/client
export BinDir=/home/ami0app/ami_scripts/bin
export EtcDir=${BinDir}/../etc
export SQLDir=${BinDir}/../sql
export TmpDir=${BinDir}/temp
export LogDir=/home/ami0app/ami_scripts/log

export EnvFile=${EtcDir}/ami_etl.env
RunTs=`echo "$(date +%Y-%m-%d\_%T)"`

export ScriptNm=`basename $0`

LogMe()
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" | tee -a ${LogFile}
}

Psql()
{
    psql -d ${PGDATABASE} -U ${PGUSER} -h ${PGHOST} -p ${PGPORT} --set ON_ERROR_STOP=on -w -t -A -c "$1"
}

Email()
{
	MyStatus=$1
    MyBatches=$2
    LogMe "Emailing ETL Job Status to the Support Team"
	cat ${LogFile} | mailx -s "AMI_DIM_ELT_PROCESS(`hostname`:${MyName}:${RunTs}) | ${tgt_table_name} | ${MyStatus} | ${MyBatches}" ${EmailList}
}

########Execution starts from here##############
. ${GPHOME}/greenplum_clients_path.sh
. ${EnvFile}
#EmailList="suneesh.as@fpl.com"
tgt_table_name="wms_sl_new_install_dtl"
StgTblName="wms_sl_new_install_stg"
TGT_TBL_NAME=`echo $tgt_table_name | tr 'a-z' 'A-Z'`

export ETLBatchID=`psql -w -A -t -c"select nextval('ami.etl_load_batch_id_seq')::bigint"`
export LogFile=${LogDir}/`basename $0`.${RunTs}_${ETLBatchID}_${tgt_table_name}.log
export RunDate=`date +%Y%m%d`

LogMe "Temporary list of stage tables to be processed : $StgTblList"

LogMe "Inserting entry to ami.etl_load table."
Psql "INSERT INTO ami.etl_load(PROCESS_SEQ, ETL_BATCH_ID,RUNDATE, PROCESS_TYPE, SCHEMA_NAME, TABLE_NAME, SRC_FILE_NAME, LOAD_START_TIME, LOAD_STATUS, ETL_USER, FILE_READ_END_TIME,FILE_TYPE) VALUES (1,$ETLBatchID,'"$RunDate"'::date,'FACT','ami','"$tgt_table_name"','"$ScriptNm"',current_timestamp,'INPROGRESS',current_user,current_timestamp,'SLNI')"

rc=`echo $?`

if [ ${rc} -ne 0 ];then
	LogMe "Insert into ami.etl_load failed."
	Email "Failed" "${ETLBatchID}"
	exit 1
else
	LogMe "Insert to ami.etl_load completed. Batch_ID : [${ETLBatchID}]"
fi

	# initiating stage load script with stage table name and batch_id as parameter.
	sh stage_load_process.sh ${StgTblName} ${ETLBatchID}>>${LogFile} 2>&1
	RC=`echo $?`
	LogMe "Data Load to stage table ended with return code : [$RC] "

	LogMe "#########################################################################################################################"


if [ ${RC} -ne 0 ];then
LogMe "Stage load failed for the table : ami_stage.${StgTblName}."
LogMe "Please execute the following to rerun the stage load for table : ${StgTblName}."
LogMe "sh stage_load_process.sh ${StgTblName} ${ETLBatchID}"
LogMe "#########################################################################################################################"
LogMe "Updating stage load failed status in audit table [ ami.etl_load ]"

LogMe "UPDATE ami.etl_load SET load_status = 'FAILED' , description = 'Stage Load Failed',load_end_time = current_timestamp WHERE etl_batch_id = $ETLBatchID and table_name = '"$tgt_table_name"'" 
#Psql "UPDATE ami.etl_load SET load_status = 'FAILED' , description = 'Stage Load Failed' WHERE e.etl_batch_id = $ETLBatchID and e.table_name = '"$TGT_TBL_NAME"'"
Psql "UPDATE ami.etl_load SET load_status = 'FAILED' , description = 'Stage Load Failed',load_end_time = current_timestamp WHERE etl_batch_id = "$ETLBatchID" and table_name = '"$tgt_table_name"'" 

rc=`echo $?`
if [ ${rc} -ne 0 ];then
	LogMe "Updating ami.etl_load failed."
fi

Email "Stage Failed" "${ETLBatchID}"
exit 1
else
	LogMe "All stage tables loaded successfully."
fi
	LogMe "#########################################################################################################################"

count=`psql -w -A -t -c" select count(0) from ami_stage.wms_sl_new_install_stg "`	

if [ ${count} -ne 0 ];then

# initiating target load script with target table name and batch_id as parameter.
LogMe "Initiating target load for table [$tgt_table_name]."
LogMe " sh sl_new_inst_fact_load_process.sh ${ETLBatchID}"
sh sl_new_inst_fact_load_process.sh ${ETLBatchID}>>${LogFile} 2>&1
RC=`echo $?`

if [ $RC -ne 0 ];then
	LogMe "Data Load failed for table : [$tgt_table_name]."
	Email "Failed" "${ETLBatchID}"
	exit 1
else
	LogMe "Load completed for table : [$tgt_table_name]."
	Email "Success" "${ETLBatchID}"
	exit 0
fi

else

	LogMe "Record count in the Stage table : 0"
				psql -w -A -t -c "UPDATE ami.etl_load SET load_status = 'SUCCESS' , description = 'Stage count zero',rows_processed='0',rows_inserted='0',load_end_time = current_timestamp WHERE etl_batch_id = "$ETLBatchID" and table_name = '"$tgt_table_name"'"
		
		rc=`echo $?`
		if [ ${rc} -ne 0 ];then
			LogMe "Updating ami.etl_load failed."
		fi
		
	LogMe "Load completed for table : [$tgt_table_name]."
	Email "Warning stage count zero" "${ETLBatchID}"
	exit 0
fi
