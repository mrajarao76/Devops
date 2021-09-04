##############################################################################################
##############################################################################################
##################      SCRIPT_NAME   : ami_elt_src_file_wrt_process.sh    		 #############
##################      SCRIPT_OWNER  : Cognizant Technology Solutions           #############
##################      Description   : Source file write process ELT script.     ############
##############################################################################################
##############################################################################################

#! /bin/sh +x

export GPHOME=/greenplum/client/client
export BinDir=/home/ami0app/ami_scripts/bin
export EtcDir=${BinDir}/../etc
export SQLDir=${BinDir}/../sql
export SrcFileDir=/informatica/InfaFiles/stage/ami
#export EnvFile=${EtcDir}/ami_etl_innowt.env
export EnvFile=${EtcDir}/ami_etl.env
export RunTs=`echo "$(date +%Y-%m-%d\_%T)"`
MyName=`basename $0`



LogMe()
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" | tee -a ${LogFile}
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
    cat ${LogFile} | mailx -s "AMI_FILE_WRITE_PROCESS(`hostname`:${MyName}:${RunTs}) | ${PrcsName} | ${MyStatus}" ${EmailList}
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

if [ $# -eq 2 ]; then
    LogMe "Input parameters passed matches the counts of required arguments. "
    NumArgs=$#
    export PrcsName=$1
	export Location=$2
	export SQLFile1=${SQLDir}/ami_elt_${PrcsName}_src_file.sql
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

#Removing the previous run source file
LogMe "Removing the previous run source file..."
rm -f ${SrcFileDir}/${Location}/${PrcsName}.txt
rc=`echo $?`
if [ ${rc} != 0 ];then
	LogMe "Unable to remove previous run source file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
    Email "Unable to remove file: [${SrcFileDir}/${Location}/${PrcsName}.txt]"
    exit 1
fi

#Writing source query to file 
LogMe "Writing source query to file starts..."
LogMe "Executing ${SQLFile1} ..."
psql -w -A -f $SQLFile1 >> $LogFile 2>&1
rc=`echo $?`
if [ ${rc} != 0 ];then
    LogMe "Unable to execute sql: ${SQLFile1}"
    Email "Unable to execute sql: ${SQLFile1}"
    exit 1

else
	if [ -f ${SrcFileDir}/${Location}/${PrcsName}.txt ];then
		Filecount=`wc -l < ${SrcFileDir}/${Location}/${PrcsName}.txt`
		if [ ${Filecount} -ne 0 ]; then
			LogMe "Source file write process completed successfully"
			JobMessage="${JobDescription} Source file write process finished successfully at `date +%Y-%m-%d\ %T`"
			LogMe ${JobMessage}
			exit 0
		else
			LogMe "Source file write process was Unsuccessful"
			ErrMessage="${JobDescription} Source file write process failed at `date +%Y-%m-%d\ %T`"
			LogMe ${ErrMessage}
			Email "FAILED"
			Exit ${ErrMessage}
		fi
	else
		LogMe "The source file [${SrcFileDir}/${Location}/${PrcsName}.txt] not found in directory [${SrcFileDir}/${Location}]"
		ErrMessage="${JobDescription} Source file write process failed at `date +%Y-%m-%d\ %T`"
        LogMe ${ErrMessage}
		Email "FAILED"
        Exit ${ErrMessage}
	fi
fi