##############################################################################################################
#Description:
#This Script Gets the Streetlight new Installation data from stage table [ami_stage.wms_sl_new_install_stg] 
#and stores the same into the target table #Target Table : AMI.wms_sl_new_install_dtl 
# 
#sends notification mail 
#Usage : sh <scriptname>
#AUTHOR: Cognizant Technologies
#Initial release: 
##############################################################################################################


export RootDir=/home/ami0app/ami_scripts/bin
export SQLDir=${RootDir}/../sql
export EtcDir=${RootDir}/../etc
export TmpDir=${RootDir}/../tmp
export LogDir=${RootDir}/../log

GPHOME=${EtcDir}

. ${GPHOME}/greenplum_clients_path.sh
. ${GPHOME}/ami_etl.env
SCRIPT_NM=`basename $0`
LOGFILE=$LogDir/$SCRIPT_NM.$(date +"%Y%m%d_%H%M%S").log
export RunDate=`echo "$(date +%Y-%m-%d)"` export SQLFile1=${SQLDir}/sl_new_inst_data_load_tgt.sql
export Tmp_file=/home/ami0app/ami_scripts/bin/temp/query_output_${ETLBatchID}.txt
pid=`echo $$`
LogFile_RetentionDays=60

LogMe()
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" | tee -a ${LOGFILE}
}


#if [ $PGDATABASE == "fpl_dev" ] || [ $PGDATABASE == "fpl_qa" ];then
#        MailTo='benjerald.aruja@fpl.com'
#		MailCc='Sreenath.Sathyanarayanan@fpl.com'
#elif [ $PGDATABASE == "fpl" ];then
#        MailTo='DL-AMI-PROD-SUPPORT-EMAIL@fpl.com'
#        MailCc='benjerald.aruja@fpl.com'
#fi

#function SendMail
#{
#    Sub=$@
#	print "Sending the status email to the recipients :  $LOGFILE"
#    cat ${LOGFILE} | mailx -s "${Sub}" -c "${MailCc}" "${MailTo}"
#}

SCRIPT_NM=`basename $0`
process_type='SLNI'
source_schema='ami_stage'
source_table='wms_sl_new_install_stg'
target_schema='ami'
target_table='wms_sl_new_install_dtl'

LogMe " ############script sl_new_inst_fact_load_process starts here ##############"
if [ $# -eq 1 ];then
export etl_batch_id=$1
else
	LogFile=${LogDir}/`basename $0`.${RunTs}.log
    LogMe "while executing the Script- {$0}, as wrong number of parameters were passed : $LOGFILE"
	exit 1
fi


LogMe "$(date '+%m%d%Y:%H:%M'): ${target_table} Load started..." 
		LogMe "$(date '+%m%d%Y:%H:%M'): ETL batch id : ${etl_batch_id}"
	if [ -z "${etl_batch_id}" ]; then
		LogMe "No data present in stage table"
		StgTblCnt=0
	else
		StgTblCnt=`psql -w -A -t -c "select count(*) from ami_stage.wms_sl_new_install_stg where etl_batch_id = ${etl_batch_id}"`
	fi
export prcs_date_time=`psql -w -A -t -c"select NOW()::TIMESTAMP WITHOUT TIME ZONE"` 
#print "$(date '+%m%d%Y:%H:%M'): ETL_BATCH_ID : ${etl_batch_id}" 
if [ $StgTblCnt != 0 ]
	then
	LogMe "START PROCESSING TABLE LOAD for [ ${target_table} ]"
	psql -w -A -f $SQLFile1 | tee -a $LOGFILE 
	rc=`echo $?` 
	LoadStatus=`psql -w -A -t -c "select load_status from ami.etl_load where etl_batch_id=$etl_batch_id"` 
	if [ $LoadStatus != 'SUCCESS' ];
	then 
		LogMe "${target_table} load Process Failed.."
		exit_code=1
	else
		exit_code=0
		
	fi
	PrcsdCnt=`psql -w -A -t -c"select rows_processed from ami.etl_load where etl_batch_id=$etl_batch_id"` 
	InsrtdCnt=`psql -w -A -t -c"select rows_inserted from ami.etl_load where etl_batch_id=$etl_batch_id"` 
	UpdtedCnt=`psql -w -A -t -c"select rows_updated from ami.etl_load where etl_batch_id=$etl_batch_id"` 
	#DeltedCnt=`Psql "select rows_deleted from ami.etl_load where etl_batch_id=$etl_batch_id"` 
	LogMe "Number of records processed in table [${target_table}] for the batch_id [${etl_batch_id}] is : ${PrcsdCnt}"
	LogMe "Number of records inserted to table [${target_table}] for the batch_id [${etl_batch_id}] is : ${InsrtdCnt}"
	LogMe "Number of records updated to table [${target_table}] for the batch_id [${etl_batch_id}] is : ${UpdtedCnt}"
	#print "Number of records deleted from table [${target_table}] for the batch_id [${etl_batch_id}] is : ${DeltedCnt}"
	LogMe "$(date '+%m%d%Y:%H:%M'): ${target_table}  Load Completed..."
	LogMe "$(date '+%m%d%Y:%H:%M'): exit $exit_code"
else
		LogMe "Record count in the Stage table : 0"
		psql -w -A -t -c "UPDATE ami.etl_load SET load_status = 'SUCCESS' , description = 'Stage count zero',rows_processed='0',rows_inserted='0',load_end_time = current_timestamp WHERE etl_batch_id = "$etl_batch_id" and table_name = '"$target_table"'"
		
		rc=`echo $?`
		if [ ${rc} -ne 0 ];then
			LogMe "Updating ami.etl_load failed."
		fi

		exit_code=0
fi
	#Remove the log files older than retention days
	LogMe "Removing log files older than  ${LogFile_RetentionDays} days"
	find ${LogDir} -type f -mtime +${LogFile_RetentionDays} -name "${SCRIPT_NM}*.log" -exec rm -f {} \;
exit $exit_code


