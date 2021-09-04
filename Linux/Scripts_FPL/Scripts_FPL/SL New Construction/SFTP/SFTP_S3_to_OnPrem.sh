#!/usr/bin/ksh

#################################################################################################
# This script fetch file from AWS S3 SFTP endpoint to on-prem server.
# Components : This execution script requires a control_file.
# Control File : /home/ami0app/ami_scripts/bin/sftp_s3to_onprem_control_file_<env>
# 
# Invoking Command: 
# sh <scriptname> <file name prefix> <Control_File_Name> 
# Eg : sh SFTP_S3_to_OnPrem.sh wms_sl_new_inst_stg sftp_s3to_onprem_control_file_<env>
#
#################################################################################################

GPHOME=/home/ami0app/ami_scripts/etc
SCRIPT_NM=`basename $0`
HostName=`hostname`
RunTime=`date '+%Y%m%d_%H%M%S'`
SCRIPTDIR=/home/ami0app/ami_scripts/bin
TMP_PATH=/home/ami0app/ami_scripts/bin/temp
LOG_DIR=/informatica/InfaFiles/stage/ami/logs
EmailRecipients="Rahuls.Menon@fpl.com,karthika.chandran@fpl.com"

###################
log()
###################
{
    Message=$@
    echo "[$(date +%Y-%m-%d\ %T)]: ${Message}" | tee -a ${LogFile}
}

#Mail the job status to recepients
###################
Email()
###################
{
	Email_message="$1"
	
	log "Emailing ETL Job Status to [ ${EmailRecipients} ]"
	
	cat ${LogFile} | mailx -s "${SCRIPT_NM} | ${RunTime} | ${HostName} | ${file_prefix} | ${Email_message}" "${EmailRecipients}"
}

#Check value for parameters from control_file
###################
Check_Value()
###################
{
	param_name="$1"
	param_value=$2
	if [ -z "${param_value}" ];then
		log "${param_name} entry not found in [ ${control_file} ]. Exiting the script"
		Email "FAILED : ${param_name} entry missing in Control_File"
		exit 1
	else
		log "${param_name}  :  ${param_value}"
	fi

}

#######################
list_files_in_S3_loc()
#######################
{
fname=$1

sftp_connect_flag=1
retry_connnctn=0

while [ ${sftp_connect_flag} -ne 0 ];do

	sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path} <<-EOF > $SFTP_LOG 2>&1
	ls -lrt ${fname}
	bye
	EOF
	
	rc_sftp=`echo $?`
	
	if [ ${rc_sftp} -ne 0 ];then
		if [ ${max_connect_retry} -gt ${retry_connnctn} ];then
			log "Unable to connect with AWS S3 endpoint. Initiating a connection retry."
			retry_connnctn=`expr ${retry_connnctn} + 1`
			log "Retry Attempt : ${retry_connnctn}"
			sleep ${Sleep_Time_Cnctn}
		else
			log "Connectivity issue with AWS S3 endpoint. Maximum retry exhausted."
			log "Use the following command to check the connectivity."
			log "sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path}"
			return 1
		fi
	else
		sftp_connect_flag=0
		log "Connected successfully to S3 endpoint. Details available in SFTP log : ${SFTP_LOG}"
		return 0
	fi
	
done
}

#######################
sftp_to_onprem_path()
#######################
{
on_prem_path=$1 
fname=$2

sftp_connect_flag=1
retry_connnctn=0

while [ ${sftp_connect_flag} -ne 0 ];do

	sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path} <<-EOF > ${SFTP_LOG} 2>&1
	lcd ${on_prem_path}
	get -p ${fname}
	bye
	EOF
	
	rc_sftp=`echo $?`
	
	if [ ${rc_sftp} -ne 0 ];then
		if [ ${max_connect_retry} -gt ${retry_connnctn} ];then
			log "Unable to connect with AWS S3 endpoint. Initiating a connection retry."
			retry_connnctn=`expr ${retry_connnctn} + 1`
			log "Retry Attempt : ${retry_connnctn}"
			sleep ${Sleep_Time_Cnctn}
		else
			log "Connectivity issue with AWS S3 endpoint. Maximum retry exhausted."
			log "Use the following command to check the connectivity."
			log "sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path}"
			return 1
		fi
	else
		sftp_connect_flag=0
		log "Connected successfully to S3 endpoint to fetch files. Details available in SFTP log : ${SFTP_LOG}"
		return 0
	fi
	
done
}

#######################
archival_in_s3()
#######################
{
s3_archv_path=$1
fname=$2

sftp_connect_flag=1
retry_connnctn=0

while [ ${sftp_connect_flag} -ne 0 ];do

	sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path} <<-EOF > $SFTP_LOG 2>&1
	rename ${fname} ${s3_archv_path}/${fname}
	bye
	EOF
	
	rc_sftp=`echo $?`
	
	if [ ${rc_sftp} -ne 0 ];then
		if [ ${max_connect_retry} -gt ${retry_connnctn} ];then
			log "Unable to connect with AWS S3 endpoint. Initiating a connection retry."
			retry_connnctn=`expr ${retry_connnctn} + 1`
			log "Retry Attempt : ${retry_connnctn}"
			sleep ${Sleep_Time_Cnctn}
		else
			log "Connectivity issue with AWS S3 endpoint. Maximum retry exhausted."
			log "Use the following command to check the connectivity."
			log "sftp -i ${Trnsfr_Key} ${Rem_Usr}@${Rem_Host}:${S3_land_path}"
			return 1
		fi
	else
		sftp_connect_flag=0
		log "Connected successfully to S3 endpoint to archive files. Details available in SFTP log : ${SFTP_LOG}"
		return 0
	fi
	
done

}

#Function to check whether the file is already transferred.
###################
trnsfer_verify()
###################
{
	vrfy_trnsfr_dir=$1
	file_nm=$2
	operation=$3

	if [ "${operation}" == "check" ];then
		if [ -f ${vrfy_trnsfr_dir}/${file_nm} ];then
			log "File [ $file_nm ] available in Directory [$vrfy_trnsfr_dir]"
			return 1
		else
			return 0
		fi

	elif [ "${operation}" == "touch" ];then
		touch ${vrfy_trnsfr_dir}/${file_nm}
		if [ $? -eq 0 ];then
			log "Zero byte file is created to indicate the transfer of file [${file_nm}]"
			return 0
		else
			log "Unable to touch zero byte file for [${file_nm}] in directory: [$vrfy_trnsfr_dir]"
			return 1
		fi
	else 
		log "unknown operation. Please pass either check/touch to trnsfr_verify function."
		Email "FAILED: Unknown value passed to function"
		exit 1
	fi

}

#########################
file_timestamp_check()
#########################
{
	new_file=$1
	one_file_flag=0	
	
	log "Timestamp comparison of last SFTPed file and new file."
	
	last_file=`ls -tr ${On_Prem_Land_path} | tail -1`
	
	if [ -z "${last_file}" ];then
		log "No older files available in path [${On_Prem_Land_path}]."
		log "File : [${new_file}] is the first file to process."
		one_file_flag=1
	fi
	
	if [ ${one_file_flag} -eq 0 ];then
		log "Last copied file in on_prem landing path is [${last_file}] "
		timestmp_old=`echo ${last_file} | awk -F'.' '{print $1}' | awk -F'_' '{print $NF}'`
		
		log "Timestamp value in last SFTPed file : [${timestmp_old}]"
	else
		timestmp_old=0
	fi
	
	log "New file to SFTP : [ ${new_file} ]"
	
	timestmp_new=`echo ${new_file} | awk -F'.' '{print $1}' | awk -F'_' '{print $NF}'`
	log "Timestamp value in new file for SFTP : [${timestmp_new}]"
	
	log "Comparing the timestamp in filenames of both files."
	
	if [ ${timestmp_new} -lt ${timestmp_old} ];then
		log "Timestamp in [ ${new_file} ] is older than [${last_file}]"
		return 1
	else
		log "File [ ${new_file} ] is same or newer than last SFTPed file."
		return 0
	fi
	
}


#Format for script invoking
###################
Usage()
###################
{
    echo "Please execute the script with the following format"
    echo "        sh <scriptname> <file name prefix> <Control_File_Name>"
    echo "        sh SFTP_S3_to_OnPrem.sh wms_sl_new_inst_stg sftp_s3to_onprem_control_file"
    exit 1
}

#################################################
###main start here##
#################################################

if [ $# -eq 2 ];then
	export file_prefix=${1}
	export control_file_name=${2}
	export RunTime=`date '+%Y%m%d_%H%M%S'`
	LogFile=${LOG_DIR}/${SCRIPT_NM}_${file_prefix}_${RunTime}.log
	SFTP_LOG=${LOG_DIR}/sftp_${file_prefix}_${RunTime}.log
else
	RunTime=`date '+%Y%m%d_%H%M%S'`
	LogFile=${LOG_DIR}/${SCRIPT_NM}_${RunTime}.log
	log "While executing the Script: {$0},Invalid number of parameters were passed"
	Email "FAILED : Invalid parameter"
	Usage
fi

. ${GPHOME}/greenplum_clients_path.sh
. ${GPHOME}/greenplum_loaders_path.sh
. ${GPHOME}/ami_etl.env

control_file=${SCRIPTDIR}/${control_file_name}

if [ -f ${control_file} ];then
	log "Control File for this execution is : [${control_file}]"
else
	log "Control file [${control_file}] for this SFTP process is not available."
	Email "FAILED : Control file missing"
	exit 1
fi

log "Getting the parameters for the file_prefix [${file_prefix}] from control_file [${control_file}]"
line=`cat ${control_file} | grep -w ${file_prefix} `


if [ ! -z ${line} ];then
	file_extn=`echo $line | cut -d'|' -f 2`
		Check_Value "FILE_EXTN" ${file_extn}
	Rem_Usr=`echo $line | cut -d'|' -f 3`
		Check_Value "Rem_Usr" ${Rem_Usr}
	Rem_Host=`echo $line | cut -d'|' -f 4`
		Check_Value "Remote_HostName" ${Rem_Host}
	Interact_Flag=`echo $line | cut -d'|' -f 5`
		Check_Value "Interactive_SFTP" ${Interact_Flag}
	Trnsfr_Key=`echo $line | cut -d'|' -f 6`
		if [ ${Interact_Flag} == "Y" ];then
			Check_Value "Transfer_Key" ${Trnsfr_Key}
		fi
	S3_land_path=`echo $line | cut -d'|' -f 7`
		Check_Value "Landing Path in S3" ${S3_land_path}
	Arch_Flag=`echo $line | cut -d'|' -f 8`
		Check_Value "Archival_Flag" ${Arch_Flag}
	S3_Arch_Path=`echo $line | cut -d'|' -f 9`
		if [ ${Arch_Flag} == "Y" ];then
			Check_Value "S3_Archival_Path" ${S3_Arch_Path}
		fi
	On_Prem_Land_path=`echo $line | cut -d'|' -f 10`
		Check_Value "Landing_path in On_Prem" ${On_Prem_Land_path}
	Already_Trnsfrd_Chk=`echo $line | cut -d'|' -f 11`
		Check_Value "Transferred_File_Check" ${Already_Trnsfrd_Chk}
	Prcsd_files_Dir=`echo $line | cut -d'|' -f 12`
		if [ ${Already_Trnsfrd_Chk} == "Y" ];then
			Check_Value "Processed Files_list path" ${Prcsd_files_Dir}
		fi
	max_connect_retry=`echo $line | cut -d'|' -f 13`
		Check_Value "Maximum connection retry" ${max_connect_retry}
	max_file_srch_retry=`echo $line | cut -d'|' -f 14`
		Check_Value "Maximum file search retry" ${max_file_srch_retry}
	File_RetentionDays=`echo $line | cut -d'|' -f 15`
		Check_Value "File_RetentionDays" ${File_RetentionDays}
	Sleep_Time_File_Srch=`echo $line | cut -d'|' -f 16`
		Check_Value "Sleep_Time_File_Srch" ${Sleep_Time_File_Srch}
	Sleep_Time_Cnctn=`echo $line | cut -d'|' -f 17`
		Check_Value "Sleep_Time_Connection" ${Sleep_Time_Cnctn}
else
	log "No entry found in control_file [ ${control_file} ] for the file_prefix [${file_prefix}]."
	log "Please check control_file or check the file_prefix passed to the script."
	Email "FAILED : No entry in control_file for file_prefix : [ ${file_prefix} ]"
	exit 1
fi

if [ $Interact_Flag == "Y" ];then
	if [ -f ${Trnsfr_Key} ];then
		log "Transfer Key file [${Trnsfr_Key}] is available. Proceeding the process."
	else
		log "Transfer Key file : [${Trnsfr_Key}] is missing. Exiting the process."
		Email "FAILED : Transfer Key file missing."
		exit 1
	fi
fi

############### Initaializing variables that are used through_out execution #############
num_retry_tmp=0
num_retry_file=0

file_list=${TMP_PATH}/list_${file_prefix}_${RunTime}.ls  ##List to get the available files in S3.

file_cnt=0 ##Initializing to 0, to assume that there are no files initially.

##########################################################################################

log "Removing files older than ${File_RetentionDays} days from landing path : [$On_Prem_Land_path]"
find ${On_Prem_Land_path} -type f -mtime +${File_RetentionDays} -name "${file_prefix}*" -exec rm -f {} \;

log "Removing files older than ${File_RetentionDays} days from processed files path : [${Prcsd_files_Dir}]"
find ${Prcsd_files_Dir} -type f -mtime +${File_RetentionDays} -name "${file_prefix}*" -exec rm -f {} \;

while [ ${file_cnt} -eq 0 ];do

	log "Listing all files available in S3 location for file_prefix : [${file_prefix}]"
	list_files_in_S3_loc "${file_prefix}*${file_extn}"
	list_rc=`echo $?`
	
	log "SFTP log details for file listing is as follows."
	cat ${SFTP_LOG} >> ${LogFile}
	
	if [ ${list_rc} -ne 0 ];then
		log "S3 Connectivity issue. Exiting the Script."
		Email "FAILED : AWS SFTP Connectivity Issue"
		exit 1
	fi
	
	cat $SFTP_LOG | grep -v "sftp" | grep -iv "Couldn't" | grep -iv "Can't" |grep -iv "ERROR" | grep ${file_prefix} | awk -F' ' '{print $9}' > ${file_list}
	file_cnt=`cat ${file_list} | wc -l`
	
	if [ ${file_cnt} -eq 0 ];then ## No Files available for the particular file_prefix.
		
		num_retry_file=`expr $num_retry_file + 1`
			
		if [ ${max_file_srch_retry} -ge ${num_retry_file} ];then
			log "No files available in S3 for the prefix [${file_prefix}]. Retry after sometime."
			log "Retry Attempt : ${num_retry_file}"
			sleep ${Sleep_Time_File_Srch}
		else
			log "Retry for file availability check is exhausted. Exiting the script"
			Email "FAILED : No files available for SFTP"
			exit 1
		fi
	
	fi
	
done

log "List of Files ready for SFTP : [${file_list}] "

##Fetch files to on-prem server.
if [ ${file_cnt} -ne 0 ];then
	
	for file_name in `cat ${file_list}`;do
		
		log "Fetching file from S3 to on-prem landing path : [${file_name}]"
		
		file_timestamp_check ${file_name}
		timstmp_rc=`echo $?`
		
		if [ ${timstmp_rc} -ne 0 ];then
			log "Timestamp in file [${file_name}] is older than the previously processed file."
			log "Check with source team, whether older file is misplaced OR need to process to downstream."
			Email "FAILED : File timestamp is older than last processed file"
			exit 1
		fi

		trnsfrd_rc=0
		
		if [ ${Already_Trnsfrd_Chk} == "Y" ];then
		
			log "Checking whether the file [${file_name}] is already SFTP-ed"
			
			trnsfer_verify ${Prcsd_files_Dir} ${file_name} "check"
			trnsfrd_rc=`echo $?`
			
		fi
			
		if [ ${trnsfrd_rc} -eq 0 ];then
			
			log "File : [${file_name}] : Not SFTP-ed yet, processing continues."
			
			sftp_to_onprem_path "${On_Prem_Land_path}" "${file_name}"
			sftp_rc=`echo $?`
			
			log "SFTP log details as follows."
			cat ${SFTP_LOG} >> ${LogFile}
			
			if [ ${sftp_rc} -ne 0 ];then
				log "SFTP process failed after several retry. Please check the connectivity."
				log "File fetch from S3 to on-prem server failed."
				Email "FAILED : SFTP Failed."
				exit 1
			else
				log "File fetch from S3 to infaserver completed for : [${file_name}]"
			fi
		
			#Check the file size in S3 and on-prem.
			log "Checking the file size in S3 and on-prem for file : [${file_name}]."
			
			list_files_in_S3_loc "${file_name}"
			list_rc=`echo $?`
			
			if [ ${list_rc} -ne 0 ];then
				log "S3 Connectivity issue. Exiting the Script."
				Email "FAILED : AWS SFTP Connectivity Issue"
				exit 1
			else
				fileSizeRemote=`cat $SFTP_LOG | grep -v "sftp" | grep -iv "Couldn't" | grep -iv "Can't" |grep -iv "ERROR" |grep ${file_name} | awk -F " " '{print $5}'`
				#If the sftp log contains error then this variable is not set   #rm $SFTP_LOG
				log "File Size in S3 location : ${fileSizeRemote}"  
				
				fileSizeLocal=`ls -lrt ${On_Prem_Land_path}/${file_name} | awk -F " " '{print $5}'`
				log "File Size in on-premise location : ${fileSizeLocal}" 			
				
				if [ ${fileSizeRemote} -ne ${fileSizeLocal} ];then
					log "File size in S3 doesn't match with on-prem file size"
					Email "FAILED : File size mismatch"
					exit 1
				else
					log "File size in S3 and On-Premise locations are matching."
					log "Successfully downloaded the file [ ${file_name} ] from S3 to on-prem"
					
					if [ ${Already_Trnsfrd_Chk} == "Y" ];then
					
						trnsfer_verify ${Prcsd_files_Dir} ${file_name} "touch"
						touched_rc=`echo $?`
						if [ ${touched_rc} -ne 0 ];then
							log "Unable to touch file_name in [${Trnsfr_Vrfy_Dir}]. Exiting the process."
							Email "FAILED : Unable to create zero byte file to indicate transferred filename."
							exit 1
						fi
					fi
				fi
			fi
			
		else
			log "File : [${file_name}] : is already transferred."
			log "Skipping the SFTP Process. Moving to Archival."
		fi

		
		if [ ${Arch_Flag} = 'Y' ];then
			#Archiving the file to S3 archival path.
			log "Archiving the file [ ${file_name} ] to S3 archival path [${S3_Arch_Path}]."
			
			archival_in_s3 "${S3_Arch_Path}" "${file_name}"
			archv_rc=`echo $?`
			
			log "SFTP log details for archival is as follows."
			cat ${SFTP_LOG} >> ${LogFile}
			
			error_inline=`cat ${SFTP_LOG} | grep -e "Can't" -e "Couldn't" -e "No such file or directory" -e "Error"`
			
			if [ ${archv_rc} -eq 0 -a -z "${error_inline}" ];then
				log "Archival completed for file [${file_name}]"
			else
				log "Archiving in S3 failed for File : [ ${file_name} ] ."
				log "File name [${file_name}] is already available in archival folder."
				log "Please check, why an archived file is available in landing path."
				Email "FAILED : Archival in S3 Failed."
				exit 1
			fi
		fi
		
	done

fi

log "Transfer from S3 to on-prem completed for [${file_prefix}] files."

log "Removing Temporary file_list"
rm ${file_list}

log "SUCCESS : SFTP from S3 to On-prem server completed."
exit 0
