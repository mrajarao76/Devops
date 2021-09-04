##############################################################################################
##############################################################################################
##################      SCRIPT_NAME   : ami_elt_slwr_wms_details_ora_wrapper.sh	 	 #########
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
	cat ${LogFile} | mailx -s "AMI_DIM_ELT_PROCESS(`hostname`:${MyName}:${RunTs}) | slwr_wms_details_ora | ${MyStatus} | ${MyBatches}" ${EmailList}
}

########Execution starts from here##############
. ${GPHOME}/greenplum_clients_path.sh
. ${EnvFile}
EmailList="suneesh.as@fpl.com"
StgTblName2="slwr_wms_details_ora"

export ETLBatchID=`psql -w -A -t -c"select nextval('ami.etl_load_batch_id_seq')::bigint"`
export LogFile=${LogDir}/`basename $0`.${RunTs}_${ETLBatchID}_wms_details.log
export RunDate=`date +%Y%m%d`


	# initiating stage load script with stage table name and batch_id as parameter.
	sh /home/ami0app/ami_scripts/bin/stage_load_process.sh ${StgTblName2} ${ETLBatchID}>>${LogFile} 2>&1
	RC2=`echo $?`
	LogMe "Data Load to stage table ended with return codes : [$RC2] "

	LogMe "#########################################################################################################################"


if [ ${RC2} -ne 0 ];then
	LogMe "Stage load failed for the table : ami_stage.${StgTblName2}."
	Email "Stage Failed" "${ETLBatchID}"
	LogMe "#########################################################################################################################"
exit 1
else
	LogMe "All stage tables loaded successfully."
	Email "Stage Load Success" "${ETLBatchID}"
	exit 0
fi
	LogMe "#########################################################################################################################"
	

