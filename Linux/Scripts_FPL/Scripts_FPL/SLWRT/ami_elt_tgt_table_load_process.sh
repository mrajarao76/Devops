##############################################################################################
##############################################################################################
##################      SCRIPT_NAME   : ami_elt_tgt_table_load_process.sh    		 #############
##################      SCRIPT_OWNER  : Cognizant Technology Solutions           #############
##################      Description   : Target table write process ELT script.     ############
##############################################################################################
##############################################################################################

#! /bin/sh +x

export GPHOME=/greenplum/client/client
export BinDir=/home/ami0app/ami_scripts/bin
export EtcDir=${BinDir}/../etc
export SQLDir=${BinDir}/../sql
export SrcFileDir=/informatica/InfaFiles/stage/ami/amigp
export wf_WMS_SLWRS_DETAILS_Load_stat=${SrcFileDir}/../uiq/wf_WMS_SLWRS_DETAILS_Load_stat.txt
export EnvFile=${EtcDir}/ami_etl.env
export RunTs=`echo "$(date +%Y-%m-%d\_%T)"`
MyName=`basename $0`
exit_code_fail=1
exit_code_success=0



LogMe()
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" >> ${LogFile} 2>&1
}

Exit()
{
    ErrorMessage=$@
    exit 2
}

#Function acting as a wrapper on psql client
Psql()
{
    psql -d ${PGDATABASE} -U ${PGUSER} -h ${PGHOST} -p ${PGPORT} --set ON_ERROR_STOP=on -w -t -A -c "$1"
}

Email()
{
	MyStatus=$1
    LogMe "Emailing ETL Job Status to the Support Team"
    cat ${LogFile} | mailx -s "AMI_TARGET_LOAD_PROCESS(`hostname`:${MyName}:${RunTs}) | ${PrcsName} | ${MyStatus}" ${EmailList}
}

Usage()
{
    echo "Usage : "
    echo "        $0 <<PrcsName>>"
    exit 1
}


########Execution starts from here##############
. ${GPHOME}/greenplum_clients_path.sh
. ${EnvFile}
export etl_batch_list=${TmpDir}/batch.lst

if [ $# -eq 3 ]; then
    LogMe "Input parameters passed matches the counts of required arguments. "
    NumArgs=$#
    export PrcsName=$1
	export Location=$2
	Loadtype=$3
	export SQLTOUCHFILE=${SrcFileDir}/${Location}/tmp/${PrcsName}_touch.txt
       if [ ${Loadtype} != "UPD" ]; then
	export SQLFile1=${SQLDir}/ami_elt_${PrcsName}_load.sql
       else
       export SQLFile1=${SQLDir}/ami_elt_${PrcsName}_updt.sql
       fi
else
    JobMessage="Failed while executing the Script- {$0}, as wrong number of parameters were passed"
     LogMe ${JobMessage}
    Usage
fi

export LogDir=/home/ami0app/ami_scripts/log
export RunDate=`date +%Y%m%d`
export HostName=`hostname`
export JobDescription="${PrcsName} ETL job (Host=${HostName}, RunDate=${RunDate})"
export LogFile=${LogDir}/`basename $0`.${RunTs}_${PrcsName}.log

#Remove the log file older than retention days
LogFile_RetentionDays=30
#find ${LogDir} -type f -mtime +${LogFile_RetentionDays} -name "*${PrcsName}*.log"
find ${LogDir} -type f -mtime +${LogFile_RetentionDays} -name "*${PrcsName}*.log" -exec rm -f {} \;

##Removing the  source file
#LogMe "Removing the source file..."
#rm -f ${SrcFileDir}/${Location}/${PrcsName}.txt
#rc=`echo $?`
#if [ ${rc} != 0 ];then
#	LogMe "Unable to remove previous run source file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
#    Email "Unable to remove file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
#    exit 1
#fi

#Checking for Source File
if [ ${Loadtype} = "STG" ];then
	#Removing the  source file
	LogMe "Checking for source file..."
	if [ -f ${SrcFileDir}/${Location}/${PrcsName}.txt ];then
		Filecount=`wc -l < ${SrcFileDir}/${Location}/${PrcsName}.txt`
		if [ ${Filecount} -ne 0 ]; then
			LogMe "Source file found and not empty"
		else
			LogMe "Source file found empty"
			ErrMessage="${JobDescription} Source file found empty at [${SrcFileDir}/${Location}] `date +%Y-%m-%d\ %T`"
			LogMe ${ErrMessage}
			Email "FAILED"
			Exit ${exit_code_fail}
		fi
	else
		LogMe "The source file [${PrcsName}.txt] not found in directory [${SrcFileDir}/${Location}]"
		ErrMessage="${JobDescription} Source file does not found at `date +%Y-%m-%d\ %T`"
        LogMe ${ErrMessage}
		Email "FAILED"
        Exit ${exit_code_fail}
	fi
fi
#Writing data to table 
LogMe "Writing to target table starts..."
LogMe "Executing ${SQLFile1} ..."
psql -w -A -f $SQLFile1 >> $LogFile 2>&1
rc=`echo $?`
if [ ${rc} != 0 ];then
    LogMe "Unable to execute sql: ${SQLFile1}"
    Email "Unable to execute sql: ${SQLFile1}"
    Exit ${exit_code_fail}

else

	if [ -f ${SQLTOUCHFILE} ];then
		rm -f ${SQLTOUCHFILE}
		if [ `echo $?` -ne 0 ];then
				#gen_extract_status=1
                LogMe "[`date '+%m%d%Y:%H:%M:%S'`]:Load process failed.Error while removing sql touch file"
                ErrMessage="${JobDescription} ${PrcsName} Load : FAILED at`date +%Y-%m-%d\ %T`"
                LogMe="Load process failed.Error while removing touch file"
                Email "FAILED"
				Exit ${exit_code_fail}
		fi
	else
        LogMe "[`date '+%m%d%Y:%H:%M:%S'`]:Load process failed.Error loading to target table data"
        ErrMessage="${JobDescription} ${PrcsName} Load : FAILED at`date +%Y-%m-%d\ %T`"
        LogMe="Load process failed.Error while loading data to target table"
        Email "FAILED"
		Exit ${exit_code_fail}
	fi

fi
if [ ${Loadtype} = "STG" ];then
	#Removing the  source file
	LogMe "Removing the source file..."
	rm -f ${SrcFileDir}/${Location}/${PrcsName}.txt
	rc=`echo $?`
	if [ ${rc} != 0 ];then
		LogMe "Unable to remove previous run source file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
		Email "Unable to remove file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
    Exit ${exit_code_fail}
	fi
fi
exit $exit_code_success