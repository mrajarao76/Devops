#! /usr/bin/ksh

#########################################################################################
# This script is a wrapper which triggers the Data Extract process.
# Input Parameters:
#      1) Control File Name
#      2) Vendor Name
#      1) SQL Generating Script Name
#
#                               version :       1.2
#                               * Output file date format parameterized.
#########################################################################################


#Function to explain the usage of the script
function Usage
{
        print "Pass the control file name,vendor name and the SQL generating script name as argument"
        print "Usage :-"
        print "${SCRIPT_NM} Control_file_name Vendor_name SQL_generating_script_name"
        print "Sample usage :- "
        print "${SCRIPT_NM} extract_control_file UM um_meter_read_extr_sql.sh"
        exit ${exit_code_fail}
}

function GenExtractSql
{
        #Step to create the extract sql
        #Two parameters are passed :
        #a)SQL file's name that has to be created along with the path
        #b)Control file entry for the extract

        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${GENSQLSCRIPT} ${GENSQLFILE} ${line}"
        sh ${GENSQLSCRIPT} ${GENSQLFILE} ${line}
}

function GenExtract
{
        #Step to create the data extract
        #Two parameters are passed :
        #a)SQL file's name along with path that has to be excuted to create the extract
        #b)Name of the Extract file along with the location where it has to be created

        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${EXTRACTSCRIPT} ${GENSQLFILE} ${EXTRACTFILE}"
        sh ${EXTRACTSCRIPT} ${GENSQLFILE} ${EXTRACTFILE}
}

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
        elif [  "${fmt}" == 'MMDDYYYY' ];then
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

############################  Main ###################################
#Variable Declaration section

SCRIPT_NM=`basename $0`
SCRIPTDIR=/home/ami0app/ami_scripts/extracts/scripts
LOGDIR=/informatica/InfaFiles/stage/ami/logs
ETCDIR=/home/ami0app/ami_scripts/etc
BASEDIR=${SCRIPTDIR}/../..
CONTROLFILE=${SCRIPTDIR}/$1
PROCESS_TYPE=$2
GENSQLSCRIPT=${SCRIPTDIR}/$3
GPHOME=${ETCDIR}
TS=`date '+%Y%m%d_%H%M%S'`
RUNDATE=`date '+%m%d%Y'`
#FILEDATE=`date '+%Y%m%d'`
ETLSCRIPT=${SCRIPTDIR}/'etl_load_table.sh'
EXTRACTSCRIPT=${SCRIPTDIR}/'gen_extract.sh'
exit_code_success=0
etl_ins_status=0
gen_sql_status=0
gen_extract_status=0
exit_code_fail=1
file_cnt=0


if [ $# -ne 3 ];then
   Usage
fi

. ${GPHOME}/greenplum_clients_path.sh
. ${GPHOME}/greenplum_loaders_path.sh
. ${GPHOME}/ami_etl.env



#LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Reading Extract Control File : ${CONTROLFILE}"

cat ${CONTROLFILE} | sed '1d' |grep ${2}|grep -i ${3} |while read line
  do
    EXTRACT_TYPE=`echo $line | cut -d'|' -f 2`
        FILENM=`echo $line | cut -d'|' -f 3`
    FILETYPE=`echo $line | cut -d'|' -f 4`
        ZIPFLAG=`echo $line | cut -d'|' -f 6`
    DATADIR=`echo $line | cut -d'|' -f 5`
        MAIL_LIST=`echo $line | cut -d'|' -f 11`
        DATE_FMT=`echo $line | cut -d'|' -f 18|tr '[:lower:]' '[:upper:]'`
		DATE_APND_FLAG=`echo $line | cut -d'|' -f 20`

LOGFILE=${LOGDIR}/`echo "${PROCESS_TYPE}_${EXTRACT_TYPE}"| tr '[:upper:]' '[:lower:]'`_wrapper_${TS}.log

exec 1>> ${LOGFILE} 2>&1
chmod 775 ${LOGFILE}

        export PROCESS_TYPE EXTRACT_TYPE ZIPFLAG


    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${PROCESS_TYPE} ${EXTRACT_TYPE} Extract : Started"
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Setting date format for the output file."
        SetDate ${DATE_FMT}
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Output file date format:${DATE_FMT} -> $FILEDATE"

        chk=`echo "${FILENM}" | awk '{print substr($0,length,1)}' | tr -d "[:alnum:]"`
	
	if [ $DATE_APND_FLAG == 'N' ];then
		EXTRACTFILE_PFX=${FILENM}
		LogFunc "Value: "${EXTRACTFILE_PFX}
		#mv ${DATADIR}/${FILENM}* ${DATADIR}/${FILENM}.${FILETYPE}
    elif [ -z "${chk}" ];then
        EXTRACTFILE_PFX=${FILENM}_${FILEDATE}
        LogFunc "Value: "${EXTRACTFILE_PFX}
    else
        EXTRACTFILE_PFX=${FILENM}${FILEDATE}
        LogFunc "Value: "${EXTRACTFILE_PFX}
    fi

    if [ -z "${FILETYPE}" ];then
        export EXTRACTFILE=${DATADIR}/${EXTRACTFILE_PFX}
    else
        export EXTRACTFILE=${DATADIR}/${EXTRACTFILE_PFX}.${FILETYPE}
    fi


    GENSQLFILE=${SCRIPTDIR}/../sql/`echo $line | cut -d'|' -f 1,2 | sed "s/|/_/g" | tr '[:upper:]' '[:lower:]'`.sql
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Extract file name : ${EXTRACTFILE}"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Inserting the entry into ami.etl_load table for Process:${PROCESS_TYPE} and Table:${EXTRACT_TYPE} "

    ETLInsert
        ret_val=$?
        if [ $ret_val -ne 0 ];then
                if [ $ret_val -eq 1 ];then
                        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Initial insert step failed. Error occurred while updating .IN PROGRESS. records as .FAILED. in ami.etl_load table."
                        SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
                        mailnotif
                        exit $exit_code_fail
                elif [ $ret_val -eq 2 ];then
                        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Initial insert step failed.Error while inserting into ami.etl_load table."
                        SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
                        mailnotif
                        exit $exit_code_fail
                fi

    fi

    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Generating extract sql for ${PROCESS_TYPE} ${EXTRACT_TYPE} : Started"

    GenExtractSql

        if [ `echo $?` -ne 0 ];then
                gen_sql_status=1
                LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Extract process failed.Error while creating the SQL file."
                SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
                DESC="Extract process failed.Error while creating the SQL file"
                mailnotif
                ETLFail
                exit $exit_code_fail
        fi
        chmod 775 ${GENSQLFILE}

    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Generating extract sql for ${PROCESS_TYPE} ${EXTRACT_TYPE} : Completed"
    LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Generating extract file for ${PROCESS_TYPE} ${EXTRACT_TYPE} : Started"

    GenExtract

        if [ `echo $?` -ne 0 ];then
                gen_extract_status=1
                LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Extract process failed.Error while generating extract file."
                SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
                DESC="Extract process failed.Error while generating extract file"
                mailnotif
                ETLFail
                exit $exit_code_fail
        fi

        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Generating extract file for ${PROCESS_TYPE} ${EXTRACT_TYPE} : Completed"
        #file_cnt=`cat ${EXTRACTFILE} |sed '1d' |wc -l|awk '{print $1}'`

        if [ ${ZIPFLAG} == 'Y' ]; then
                f_cnt=`gunzip -cd ${EXTRACTFILE}.gz | wc -l`
                file_cnt=`expr $f_cnt - 1`
        else
                file_cnt=`cat ${EXTRACTFILE} |sed '1d' |wc -l|awk '{print $1}'`
        fi

        cpy_cnt=`grep 'COPY' ${GENSQLFILE} | wc -l`
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${cpy_cnt}"
        if [ ${cpy_cnt} -eq 1 ];then
                file_cnt=`cat ${EXTRACTFILE} |wc -l|awk '{print $1}'`
        fi

        chmod 774 ${EXTRACTFILE}*
	
		
        if [ ${file_cnt} -eq 0 ]; then
                LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${EXTRACTFILE} does not have data"
        else
                LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${EXTRACTFILE} - [ $file_cnt ] records"
        fi

        LogFunc "The below file is generated with [ $file_cnt ] records :-"
        LogFunc "${EXTRACTFILE}"

        SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract file generated :[ $file_cnt ] records"
        export file_cnt
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Updating the entry into ami.etl_load table for Process:${PROCESS_TYPE} and  Table:${EXTRACT_TYPE}"
        DESC="Extract process completed"
    ETLUpdate
        if [ `echo $?` -ne 0 ];then
                etl_update_status=1
                LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:Extract process failed.Error while updating ami.etl_load table."
                SUBJ="`date '+%m%d%Y'`: `hostname` : ${PROCESS_TYPE}-${EXTRACT_TYPE} Extract : FAILED"
                mailnotif
                exit $exit_code_fail
        fi
        LogFunc "[`date '+%m%d%Y:%H:%M:%S'`]:${PROCESS_TYPE} ${EXTRACT_TYPE} Extract : Completed"
        mailnotif
  done
exit $exit_code_success


