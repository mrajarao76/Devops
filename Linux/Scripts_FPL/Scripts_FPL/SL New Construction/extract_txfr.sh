#!/usr/bin/ksh


#####################################################################################
#Description:
#This script will transfer the daily incremental file to specified target system
#Usage : sh <scriptname> <control file name> <output file name>
#AUTHOR: Cognizant Technologies
#
#####################################################################################

GPHOME=/home/ami0app/ami_scripts/etc
SCRIPT_NM=`basename $0`
ENVFILEPATH="/home/ami0app/ami_scripts/extracts/scripts"
SCRIPTDIR=${ENVFILEPATH}
ENVFILE=sftp_env.env
BASE_DIR="${ENVFILEPATH}/../.."
LOG_DIR="/informatica/InfaFiles/stage/ami/logs"
DT1=`date '+%d%m%Y'`
DT2=`date '+%Y%m%d'`
FTP_DATE=`date '+%Y%m%d_%H%M%S'`
failure_exitcode=2
success_exitcode=0
exit_code_fail=1
PROCESS_TYPE=${2}
OUTPUT_FILE=${3}
ETLSCRIPT=${SCRIPTDIR}/'etl_load_table.sh'
ARCHIVEDIR=/informatica/InfaFiles/stage/ami/archive/`echo ${PROCESS_TYPE}| tr '[:upper:]' '[:lower:]'`
#FILE_RET=15 #file retention period in days


function ETLInsert
{
	#To insert an entry into ami.etl_load table
    #Parameters passed :
    #a) Type of record - 'INSERT'
    LogFunc "Data base : ${PGDATABASE}"
	LogFunc "User : ${PGUSER}"
	LogFunc "Host : ${PGHOST}"
	LogFunc "Port : ${PGPORT}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${ETLSCRIPT}"
    sh ${ETLSCRIPT} "INSERT"
}

function ETLUpdate
{
	#To update an entry into ami.etl_load table to SUCCESS
	#Parameters passed :
    #a) Type of record - UPDATE, SUCCESS
    LogFunc "Data base : ${PGDATABASE}"
	LogFunc "User : ${PGUSER}"
	LogFunc "Host : ${PGHOST}"
	LogFunc "Port : ${PGPORT}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${ETLSCRIPT} "UPDATE" "SUCCESS""
    sh ${ETLSCRIPT} "UPDATE" "SUCCESS" "${DESC}"
}

function ETLFail
{
	#To update an entry into ami.etl_load table to SUCCESS
	#Parameters passed :
	#a) Type of record - UPDATE, FAILED
    LogFunc "Data base : ${PGDATABASE}"
	LogFunc "User : ${PGUSER}"	   
	LogFunc "Host : ${PGHOST}"
	LogFunc "Port : ${PGPORT}"		
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${ETLSCRIPT} "UPDATE" "FAILED" "
    sh ${ETLSCRIPT} "UPDATE" "FAILED" "${DESC}"
}


function LogFunc
{
    #Logging Comments in the Log File
    LogText=$@
    print "${LogText}" 
}		

#----------- file_list_creation
function file_list_all
{
	if [ -f ${DATADIR}/${FILENM}* ];then
		find ${DATADIR}/${FILENM}* -prune -type f -name "${FILENM}_[0-9]*" > ${DATADIR}/$file_list_name
		print "[`date '+%m%d%Y:%H:%M:%S'`]: Changing permission on ${DATADIR}/$file_list_name"
		chmod 777 ${DATADIR}/$file_list_name
	fi	
}

function file_list_latest
{
	cd ${DATADIR}
	if [ -f ${DATADIR}/${FILENM}* ];then
		find ${DATADIR}/${FILENM}* -prune -type f -name "${FILENM}_${SUF}*" > ${DATADIR}/$file_list_name
		print "[`date '+%m%d%Y:%H:%M:%S'`]: Changing permission on ${DATADIR}/$file_list_name"
		chmod 777 ${DATADIR}/$file_list_name
	fi	
}

function file_list_nodate
{
	if [ -f ${DATADIR}/${FILENM}* ];then
		find ${DATADIR}/${FILENM}* -prune -type f -name "${FILENM}.${SUF}" > ${DATADIR}/$file_list_name
		print "[`date '+%m%d%Y:%H:%M:%S'`]: Changing permission on ${DATADIR}/$file_list_name"
		chmod 777 ${DATADIR}/$file_list_name
	fi	
}
#----------- interactive sftp session
function start_sftp_pwd
{
	FILE=$1
	expect<<-EOF
	set timeout -1	
	spawn /usr/bin/sftp -o Port=$PORT $REM_USER@$REM_HOST
	expect "password:"
	send "$PASSWORD\r"
	expect "sftp>"
	send "lcd ${DATADIR}\r"
	expect "sftp>"
	send "put $FILE $REM_DIR\r"
	expect "sftp>"
	send "bye\r"
	EOF

}

#----------- non interactive sftp session
function start_sftp_pwdless
{
	sftp ${REM_USER}@${REM_HOST} <<-EOF
	lcd ${DATADIR}
	cd ${REM_DIR}
	put ${FTP_FILE}
	bye
	EOF
}

function success_mailnotif
{
    SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:File Transfer from `hostname` to $SYS - SUCCESS"
    mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE  >>$LOGFILE 2>&1
}

function mailnotif
{
	cat ${LOGFILE} | mailx -s "$SUBJ" ${MAIL_LIST}
}

function SetDate
{
	fmt=$1 #date format
	if [ "${fmt}" == 'YYYYMMDD' ];then
		FILEDATE=`date '+%Y%m%d'`
	elif [ "${fmt}" == 'DDMMYYYY' ];then
		FILEDATE=`date '+%d%m%Y'`
	elif [ 	"${fmt}" == 'MMDDYYYY' ];then
		FILEDATE=`date '+%m%d%Y'`
        elif [  "${fmt}" == 'YYYYMMDDHMS' ];then
                FILEDATE=`date '+%Y%m%d%H%M%S'`
	else 
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Specified date format ${fmt} is invalid.Please pass a valid date format."
		SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
		mailnotif 
		exit $exit_code_fail
	fi		
}


function GetFileSize
{
file=$1
cd ${DATADIR}
fileSizeLocal=`ls -lrt $file |awk -F " " '{print $5}'`
#fileSizeLocal=0
}

function cksum_sftp_pwd
{
file=$1
	expect<<-EOF > $SFTP_LOG
	set timeout -1	
	spawn /usr/bin/sftp -o Port=$PORT $REM_USER@$REM_HOST
	expect "password:"
	send "$PASSWORD\r"
	expect "sftp>"
	send "cd $REM_DIR\r"
	expect "sftp>"
	send "ls -lrt $file\r"
	expect "sftp>"
	send "bye\r"
	EOF
	echo ""
	
fileSizeRemote=`cat $SFTP_LOG | grep -v "sftp" |grep -iv "Couldn't" | grep -iv "Can't" |grep -iv "ERROR"| grep "$file" | awk -F " " '{print $5}'`
#If the sftp log contains error then this variable is not set	
}

function cksum_sftp_pwdless
{
file=$1
	sftp ${REM_USER}@${REM_HOST} <<-EOF > $SFTP_LOG
	ls -lrt ${REM_DIR}/$file
	bye
	EOF

fileSizeRemote=`cat $SFTP_LOG | grep -v "sftp" | grep -iv "Couldn't" | grep -iv "Can't" |grep -iv "ERROR" |grep $file | awk -F " " '{print $5}'`
#If the sftp log contains error then this variable is not set	#rm $SFTP_LOG
}

#--------------------------------------
# main
#--------------------------------------

if [ $# -eq 3 ]
then
    control_file="${SCRIPTDIR}/${1}"
    if [ -f ${control_file} ];then
        LogFunc "Control file name : ${1}"
    else
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: Control file not found"
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: exit $failure_exitcode"
        exit $failure_exitcode
    fi
else
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: Not enough arguments passed with main script."
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: Expected no. of arguments : 3"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: Expected arguments : control file name , vendor name , output file name"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:	Sample usage"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:	sh extract_txfr.sh extract_control_file UM um_kwh_usage"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: exit $failure_exitcode"
    exit $failure_exitcode
fi

. ${GPHOME}/greenplum_clients_path.sh
. ${GPHOME}/greenplum_loaders_path.sh
. ${GPHOME}/ami_etl.env

cat ${control_file} |sed '1d'|grep ${PROCESS_TYPE}|grep -w ${OUTPUT_FILE} | while read ln
do	

	SYS=`echo $ln | awk -F '|' '{print $1}'|tr '[:upper:]' '[:lower:]'`
	EXTRACT_TYPE=`echo $ln | awk -F '|' '{print $2}'`_TXFR
	if [ $SYS = 'rpms' ];then
		FILENM=`echo $ln | awk -F '|' '{print $3}'`
		
	else
		FILENM=`echo $ln | awk -F '|' '{print $3}'|tr '[:upper:]' '[:lower:]'`
	fi
	FILE_TYPE=`echo $ln | awk -F '|' '{print $4}'`	
	DATADIR=`echo $ln | awk -F '|' '{print $5}'`	
	ZIPF=`echo $ln | awk -F '|' '{print $6}'`	
    REM_HOST=`echo $ln | awk -F '|' '{print $7}'|tr '[:upper:]' '[:lower:]'`
    REM_USER=`echo $ln | awk -F '|' '{print $8}'|tr '[:upper:]' '[:lower:]'`
	REM_DIR=`echo $ln | awk -F '|' '{print $9}'`
	INTERACT=`echo $ln | awk -F '|' '{print $10}'`
	MAIL_LIST=`echo $ln | awk -F '|' '{print $11}'`
	TABLE_NM=`echo $EXTRACT_TYPE |tr '[:upper:]' '[:lower:]'`
	ARCH_FLAG=`echo $ln | awk -F '|' '{print $14}'`
	LATEST_FILE_FLAG=`echo $ln | awk -F '|' '{print $15}'`
	DATE_FMT=`echo $ln | awk -F '|' '{print $18}'|tr '[:lower:]' '[:upper:]'`
	CKSUM_FLAG=`echo $ln | awk -F '|' '{print $19}'|tr '[:lower:]' '[:upper:]'`
	DATE_APND_FLAG=`echo $ln | cut -d'|' -f 20`
	
	export PROCESS_TYPE EXTRACT_TYPE
	
	LOGFILE=${LOG_DIR}/`echo "${PROCESS_TYPE}_${EXTRACT_TYPE}"| tr '[:upper:]' '[:lower:]'`_${FTP_DATE}.log
	SFTP_LOG=${LOG_DIR}/sftp_`echo "${PROCESS_TYPE}_${EXTRACT_TYPE}"| tr '[:upper:]' '[:lower:]'`_${FTP_DATE}.log
	exec 1>> ${LOGFILE} 2>&1
    chmod 775 ${LOGFILE}
		
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Total parameters passed : $#"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Parameters passed : $@"
	
	SetDate ${DATE_FMT}
	if [ ${DATE_APND_FLAG} == 'N' ];then
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Date append flag is set to N."
		SUF="${FILE_TYPE}"
	else
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Date append flag is set to Y."
		SUF="${FILEDATE}.${FILE_TYPE}"
	fi
	
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Target System : ${SYS}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Table name : ${TABLE_NM}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Remote host : ${REM_HOST}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Remote user : ${REM_USER}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Data file location : ${DATADIR}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Remote Directory : ${REM_DIR}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Interactive SFTP flag : ${INTERACT}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Archival flag : ${ARCH_FLAG}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Latest file select flag : ${LATEST_FILE_FLAG}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Check sum flag : ${CKSUM_FLAG}"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Date Append flag : ${DATE_APND_FLAG}"
	
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Starting Run on host `hostname`: $SCRIPT_NM"
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Inserting the entry into ami.etl_load table for Process:${PROCESS_TYPE} and Table:${EXTRACT_TYPE} "
	
	ETLInsert 
	ret_val=$?
	if [ $ret_val -ne 0 ];then
		if [ $ret_val -eq 1 ];then
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Initial insert step for transfer process failed. Error occurred while updating 'IN PROGRESS' records as 'FAILED' in ami.etl_load table."
			SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
			mailnotif 
			exit $exit_code_fail
		elif [ $ret_val -eq 2 ];then	
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Initial insert step for transfer process failed.Error while inserting into ami.etl_load table."
			SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
			mailnotif 
			exit $exit_code_fail
		fi
    fi
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Finished inserting entry in to ami.etl_load table for current file transfer process."	
		
	if [ ${INTERACT} = 'Y' ];then #if interactive sftp session is set then password and port will be exported as environment variables
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:SFTP process is set to be interactive,exporting password to env for remote server authentication"
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: . ${ENVFILEPATH}/${ENVFILE} ${REM_HOST}"
        . ${ENVFILEPATH}/${ENVFILE} ${REM_HOST}
        rc=`echo $?`
		if [ $rc -ne 0 ];then	
            LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Error while setting env variables"
			SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname`- FAILED"
			DESC="Transfer process failed.Error while setting environment variables"
			ETLFail
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
			mailx -s "$SUBJ" ${MAIL_LIST} < ${LOGFILE}
            exit $failure_exitcode
		fi
		
		PASSWORD=${HOST_PASS}
		PORT=${HOST_PORT}
	
		if [ -z "$PASSWORD" ] || [ -z "$PORT" ];then
            LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:PASSWORD or PORT not set"
			SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Exract file transfer from `hostname`- FAILED"
			DESC="Transfer process failed.Password or Port not set"
			ETLFail
			LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
			mailx -s "$SUBJ" ${MAIL_LIST} < ${LOGFILE}			
			exit $failure_exitcode
		fi
		
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:PASSWORD and PORT values are set"
	fi

    cd $DATADIR
    file_list_name="${SYS}_file_list_${TABLE_NM}_${FTP_DATE}.lst"
    LogFunc "Creating file list for ${SYS}.${TABLE_NM} - ${file_list_name} "
	if [ ${DATE_APND_FLAG} == 'N' ];then
		LogFunc "find ${DATADIR}/${FILENM}* -prune -type f -name ${FILENM}.${SUF}  > ${DATADIR}/$file_list_name"
		file_list_nodate
	elif [ $LATEST_FILE_FLAG == 'Y' ];then
		LogFunc "find ${DATADIR}/${FILENM}* -prune -type f -name ${FILENM}_${SUF}*  > ${DATADIR}/$file_list_name"
		file_list_latest
	elif [ $LATEST_FILE_FLAG == 'N' ];then
		LogFunc "find ${DATADIR}/${FILENM}* -prune -type f -name ${FILENM}_[0-9]* > ${DATADIR}/$file_list_name"
		file_list_all
	else 
		LogFunc "LATEST_FILE_FLAG variable set to unknown value."
		SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
		DESC="Transfer process failed.Latest file select falg not set."
		ETLFail
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
		mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
		exit $failure_exitcode		
	fi

    file_cnt=0

    if [ -f ${DATADIR}/${file_list_name} ]
    then
        file_cnt=`wc -l < ${DATADIR}/${file_list_name}`
        LogFunc "Total number of files to be transferred for ${TABLE_NM} :${file_cnt}"
    else
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]: File list ${file_list_name} not found "
		SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
		DESC="Transfer process failed.File list not found"
		ETLFail
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
		mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
		exit $failure_exitcode
    fi
	
    if [ $file_cnt -gt 0 ];then
		for FTP_FILE in `cat ${DATADIR}/$file_list_name`
		do
			trap '{ echo " you pressed Ctrl-C. Time to quit." ; exit 1; }' INT
			#LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:start_sftp $FTP_FILE"
			if [ ${INTERACT} == 'Y' ];then
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Starting interactive sftp session..."
				start_sftp_pwd $FTP_FILE
			elif [  ${INTERACT} == 'N' ];then
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Starting non interactive sftp session..."
				start_sftp_pwdless $FTP_FILE
			else 
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Interact flag not set.Please set the flag correctly(Y/N)"
				SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
				DESC="Transfer process failed.Interact flag not set"
				ETLFail
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
				mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
				exit $failure_exitcode	
			fi
			rc=`echo $?`
			if [ $rc -ne 0 ];then
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:SFTP session failed from `hostname` to ${SYS} for ${TABLE_NM}"
				SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
				DESC="Transfer process failed.SFTP session failed"
				ETLFail
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
				mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
				exit $failure_exitcode
			fi	
			#check sum addition starts here
			if [ -z "$CKSUM_FLAG" ] 
			then
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Checksum flag is not set.Please set the flag to Y/N"
			elif [ $CKSUM_FLAG == 'Y' ]
			then
				file=`echo $FTP_FILE | awk -F "/" '{print $NF}'` #get the file name only
				GetFileSize $file
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Size of the file in local server `hostname` : $fileSizeLocal bytes"
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Checking the size of the uploaded file in remote server ${REM_HOST}"
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Connecting to remote server ${REM_HOST}..."
				#print "[`date '+%m%d%Y:%H:%M:%S'`]:File name - $file"
				if [ ${INTERACT} == 'Y' ]
				then
					cksum_sftp_pwd $file
				elif [ ${INTERACT} == 'N' ]
				then
					cksum_sftp_pwdless $file
				fi
				
				rc=`echo $?`
				
				if [ $rc -ne 0 ] ;then
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:SFTP session failed from `hostname` to ${SYS} for ${TABLE_NM}"
					SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
					DESC="Transfer process failed.Checksum session failed"
					ETLFail
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
					mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
					exit $failure_exitcode
				fi
				if [ -z "$fileSizeRemote" ]
				then
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:The sftp log file seems to contain errors.Could not resolve the size of the file from log.Please check sftp log file"
					SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
					DESC="Transfer process failed.Not able to resolve the file size in remote server."
					ETLFail
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
					mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
					exit $failure_exitcode
				fi				
				
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:File size in remote server : $fileSizeRemote bytes"
				
				if [ $fileSizeLocal -ne $fileSizeRemote ]
				then
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Check sum session failed.File size doesnot match"
					SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
					DESC="Transfer process failed.File size doesnot match"
					ETLFail
					LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
					mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
					exit $failure_exitcode	
				fi

				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Checksum session passed.File size matched"
			elif [ $CKSUM_FLAG == 'N' ]
			then			
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Checksum session disabled."
			else
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Checksum flag is set to unknown value $CKSUM_FLAG.Please set the flag to Y/N"	
			fi
				
		done
	    LogFunc "The below file is transferred to $SYS"
		cat ${DATADIR}/$file_list_name >> $LOGFILE	
			if [ $ARCH_FLAG == 'Y' ];then	
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Below files will be archived."
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Archive path : ${ARCHIVEDIR}"
				cat ${DATADIR}/$file_list_name
				cd ${DATADIR}
				cat  ${DATADIR}/$file_list_name |while read line
				do 
					mv $line ${ARCHIVEDIR}
					if [ ${DATE_APND_FLAG} == 'N' ];then
						LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Date append flag set to N. Appending date before archival"
						mv ${ARCHIVEDIR}/${FILENM}.${SUF} ${ARCHIVEDIR}/${FILENM}_${FTP_DATE}.${SUF}
						if [ $rc -ne 0 ] ;then
							LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Script failed while renaming the file in ${ARCHIVEDIR}"
							SUBJ="$FTP_DATE:${SYS}:${TABLE_NM}:Extract file transfer from `hostname` to ${SYS} - FAILED"
							DESC="Transfer process failed while archiving"
							ETLFail
							LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
							mailx -s "$SUBJ" $MAIL_LIST <$LOGFILE
							exit $failure_exitcode
						fi	
					fi
				done
			elif [ $ARCH_FLAG == 'N' ];then
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Archival flag is set to N.No file will be archived from ${DATADIR}."
			else
				LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Archival flag is set to unknown value."
			fi
			
	else
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:No ${TABLE_NM} file to transfer to ${SYS}"	
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:No files archived"
	fi
		export file_cnt
	## Archive the files after transfer		
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Removing file list $file_list_name"
	rm -f ${DATADIR}/$file_list_name 
	DESC="Transfer process completed"
		
	## Remove files older than retention days
	#if [ `find $DATADIR -name "${FILENM}*" -mtime +${FILE_RET}|wc -l|awk '{print $1}'` -gt 0 ];then 
	#	print "[`date '+%m%d%Y:%H:%M:%S'`]:Remove files older than ${FILE_RET} day[s] from ${DATADIR}"	
	#	print "[`date '+%m%d%Y:%H:%M:%S'`]:Below files will be removed"	
	#	find $DATADIR -name "${FILENM}*" -mtime +${FILE_RET} |while read line;do print $line;rm -f $line;done
	#else 
	#	print "[`date '+%m%d%Y:%H:%M:%S'`]:No files older than ${FILE_RET} day[s]"
	#	print "[`date '+%m%d%Y:%H:%M:%S'`]:No files are purged"
	#fi
	
	LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Updating ami.etl_load table status."	
	 ETLUpdate 
	if [ `echo $?` -ne 0 ];then
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Transfer process failed.Error while updating ami.etl_load table."
		SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Transfer : FAILED"
		LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Failure notification..."
		mailnotif 
		exit $exit_code_fail
	fi
LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${PROCESS_TYPE}-${EXTRACT_TYPE} Transfer process completed"
LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Sending Email notification..."	
success_mailnotif
done
exit `echo $?`

