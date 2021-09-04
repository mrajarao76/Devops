##############################################################################################
##############################################################################################
##################      SCRIPT_NAME   : stage_load_process.sh					 #############
##################      SCRIPT_OWNER  : Cognizant Technology Solutions           #############
##################      Description   : ETL stage table load script.			 #############
##############################################################################################
##############################################################################################

#! /bin/sh +x

export GPHOME=/greenplum/client/client
export BinDir=/home/ami0app/ami_scripts/bin
export EtcDir=${BinDir}/../etc
export SQLDir=${BinDir}/../sql
#export SrcFileDir=/informatica/InfaFiles/stage/ami
export EnvFile=${EtcDir}/ami_etl.env
export RunTs=`echo "$(date +%Y-%m-%d\_%T)"`
MyName=`basename $0`


LogMe()
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" | tee -a ${LogFile}
}


#Function acting as a wrapper on psql client
Psql()
{
    psql -d ${PGDATABASE} -U ${PGUSER} -h ${PGHOST} -p ${PGPORT} --set ON_ERROR_STOP=on -w -t -A -c "$1"
}

Usage()
{
    echo "Usage : "
    echo "        $0 <<StgTblNm>> <<ETLBatchID>>"
    echo "        $0 bill_acct_curr_stg 12345"
    exit 1
}


########Execution starts from here##############
. ${GPHOME}/greenplum_clients_path.sh
. ${EnvFile}
export LogDir=/home/ami0app/ami_scripts/log
export RunDate=`date +%Y%m%d`
export TmpDir=${BinDir}/temp

if [ $# -eq 2 ]; then
	export LogFile=${LogDir}/`basename $0`.${RunTs}_$1.log
    LogMe "Input parameters passed matches the counts of required arguments. "
    NumArgs=$#
    export StgTblNm=$1
	export ETLBatchID=$2
	
	export SQLFile1=${SQLDir}/ami_elt_${StgTblNm}_load.sql
	
else
	LogFile=${LogDir}/`basename $0`.${RunTs}.log
    JobMessage="while executing the Script- {$0}, as wrong number of parameters were passed"
    LogMe ${JobMessage}
    Usage
fi


#exec 1>> ${LogFile} 2>&1

#Remove the log file older than retention days
LogFile_RetentionDays=30
find ${LogDir} -type f -mtime +${LogFile_RetentionDays} -name "*${StgTblNm}*.log" -exec rm -f {} \;

#Removing old temporary file
if [ -f ${TmpDir}/${StgTblNm}.txt ];then
	
	LogMe "Removing the temporary load status file [${StgTblNm}.txt] from directory [$TmpDir]"
	rm ${TmpDir}/${StgTblNm}.txt
	
	rc=`echo $?`

	if [ $rc != 0 ];then
		LogMe "Removing the temporary load status file failed"
		exit 1
	fi
fi

#updating the etl_load of previously failed instance [incase of rerun for stage load]

Psql "UPDATE AMI.ETL_LOAD E SET LOAD_STATUS = 'INPROGRESS',load_end_time = null ,description = 'Stage Load Complete' WHERE LOAD_STATUS = 'FAILED' AND e.etl_batch_id = "$ETLBatchID""
rc=`echo $?`
if [ ${rc} -ne 0 ];then
	LogMe "Updating ami.etl_load failed."
	exit 1
fi

	
#Refresh stage tables
LogMe "Refreshing stage table [ ami_stage.$StgTblNm ]..."
LogMe "Executing ${SQLFile1} ..."
psql -w -A -f $SQLFile1 | tee -a $LogFile
rc=`echo $?`

if [ ${rc} != 0 ];then
    LogMe "Unable to execute sql: ${SQLFile1}"
    exit 1

else
	if [ -f ${TmpDir}/${StgTblNm}.txt ];then
		LoadStatus=`cat ${TmpDir}/${StgTblNm}.txt`
		if [ "$LoadStatus" =  "Success" -o "$LoadStatus" =  "SUCCESS" ];then
			LogMe "SQL Load for stage table [${StgTblNm}] completed successfully"
			exit 0
		else
			LogMe "SQL Load for stage table [${StgTblNm}] was Unsuccessful."
			exit 1
		fi
	else
		LogMe "SQL Load for stage table [${StgTblNm}] was Unsuccessful."
		exit 1 
	fi
fi
