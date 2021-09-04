#!/usr/bin/ksh

#################################################################################################
# This script copy file from source_path to destination_path in on-prem server.
# Components : This execution script requires a control_file. 
# Control File : /home/ami0app/ami_scripts/bin/file_copy_prcs_control_file
# 
# Invoking Command: 
# sh <scriptname> <file name prefix> <Control_File_Name>
# Eg : sh file_copy_process.sh wms_sl_new_inst_stg file_copy_prcs_control_file
#
#################################################################################################

GPHOME=/home/ami0app/ami_scripts/etc
SCRIPT_NM=`basename $0`
HostName=`hostname`
RunTime=`date '+%Y%m%d_%H%M%S'`
SCRIPTDIR=/home/ami0app/ami_scripts/bin
TMP_PATH=/home/ami0app/ami_scripts/bin/temp
LOG_DIR=/informatica/InfaFiles/stage/ami/logs
EmailRecipients="Rahuls.Menon@fpl.com,ramaswami.seshan@fpl.com,karthika.chandran@fpl.com,sherin.leo@fpl.com,limna.mohandas@fpl.com"

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

#Format for script invoking
###################
Usage()
###################
{
    echo "Please execute the script with the following format"
    echo "        sh <scriptname> <file name prefix> <Control_File_Name>"
    echo "        sh file_copy_process.sh wms_sl_new_inst_stg file_copy_prcs_control_file"
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
		Check_Value "File_Extn" ${file_extn}
	source_path=`echo $line | cut -d'|' -f 3`
		Check_Value "Source_Path" ${source_path}
	dest_path=`echo $line | cut -d'|' -f 4`
		Check_Value "Destination_Path" ${dest_path}
	touch_file=`echo $line | cut -d'|' -f 5`
		Check_Value "Touch_File" ${touch_file}
	unzip_flag=`echo $line | cut -d'|' -f 6`
		Check_Value "Unzip_Flag" ${unzip_flag}
	file_list_flag=`echo $line | cut -d'|' -f 7`
		Check_Value "File_list_flag" ${file_list_flag}
	dest_file_list=`echo $line | cut -d'|' -f 8`
		if [ ${file_list_flag} == "Y" ];then
			Check_Value "File_List in Destination" ${dest_file_list}
		fi
	multi_file_prcs_flag=`echo $line | cut -d'|' -f 9`
		Check_Value "Multiple_file_process_flag" ${multi_file_prcs_flag}
	empty_file_fail_flag=`echo $line | cut -d'|' -f 10`
		Check_Value "Empty_File_Fail_Flag" ${empty_file_fail_flag}
else
	log "No entry found in control_file [ ${control_file} ] for the file_prefix [${file_prefix}]."
	log "Please check control_file or check the file_prefix passed to the script."
	Email "FAILED : No entry in control_file for file_prefix : [ ${file_prefix} ]"
	exit 1
fi

if [ -f ${touch_file} ];then
	log "Touchfile [${touch_file}] is available. Proceeding the process."
else
	log "Touchfile : [${touch_file}] is missing. Exiting the process."
	Email "FAILED : Touchfile missing."
	exit 1
fi

############### Initaializing variables that are used through_out execution #############
file_name_list_tmstmp=0
file_name_list=${TMP_PATH}/list_${file_prefix}_${RunTime}.ls
new_file_cnt=0

##########################################################################################
log "Checking the destination directory [${dest_path}] for older files"
log "Older files in Destination_Path : "
ls -ltr ${dest_path}/* >> ${LogFile}

log "Clearing the destination directory : [${dest_path}] "
rm -f ${dest_path}/*
rmv_rc=`echo $?`
if [ ${rmv_rc} -ne 0 ];then
	log "Older file removal from directory : [${dest_path}] is failed"
	log "Please check the file permissions or whether the file is in use"
	Email "FAILED : Older file removal failed"
	exit 1
else
	log "Older files removed from destination directory : [${dest_path}]"
	log "Listing the destination path (after removal of older files)"
	ls -ltr ${dest_path}/* >> ${LogFile}
fi

touchfile_time_stmp_b4=`istat ${touch_file} | grep "Last modified:" | awk -F' ' '{print $4,$5,$6}'`
log "Touch File timestamp before execution : [${touchfile_time_stmp_b4}] "

log "Checking new files availablity in Source Directory : [${source_path}] "
new_file_cnt=`find ${source_path}/* -prune -type f -name "${file_prefix}*" -newer ${touch_file} | wc -l | tr -d ' '`

log "Count of new files available in source path : [ ${new_file_cnt} ]"

find ${source_path}/* -prune -type f -name "${file_prefix}*" -newer ${touch_file} > ${file_name_list}
file_name_list_tmstmp=`date '+%Y%m%d%H%M.%S'`

if [ ${new_file_cnt} -gt 0 ];then

	log "Files available for copy."
	
	if [ ${new_file_cnt} -gt 1 -a ${multi_file_prcs_flag} = "N" ];then
	
		log "More than 1 new file available for [${file_prefix}] in the source_path : [${source_path}]"
		log "Files newer than timestamp [ ${touchfile_time_stmp_b4} ]"
		log "Filenames available in list : [${file_name_list}]"
		log "Please copy the correct file/files manually to path : [${dest_path}]"
		log "Then, update the timestamp of touch_file  : [ ${touch_file} ]"
		log "For detailed steps, please follow the failure recovery steps given in the link : https://confluence.nexteraenergy.com/display/WMSDI/Failure+Scenarios"
		Email "FAILED : More than 1 file to copy"
		exit 1
	fi
	
	for file_name in `cat ${file_name_list}`; do
		
		log "Copy process starting for file : [ ${file_name} ]"
		file_basenm=`basename ${file_name}`
		log "Source path : ${source_path}"
		log "Destination Path : ${dest_path}"
		
		cp -p ${source_path}/${file_basenm} ${dest_path}/${file_basenm}
		cp_rc=`echo $?`
		
		if [ ${cp_rc} -ne 0 ];then
			log "Copy process failed for file ${file_name}"
			log "To restart copy process, please invoke the script as follows."
			log "sh $0 ${file_prefix} ${control_file_name}"
			Email "FAILED : Copy process failed"
			exit 1
		else
			log "Successfully copied file : ${file_basenm}"
		fi
		
		if [ ${unzip_flag} = "Y" ];then
			
			log "Unzipping the file : [${file_basenm}]"
			
			gunzip ${dest_path}/${file_basenm}
			zip_rc=`echo $?`
			if [ ${zip_rc} -ne 0 ];then
				log "Unzipping the file [{${file_basenm}}] failed."
				Email "FAILED : Unzipping file failed"
				exit 1
			fi
			
		fi
		
	done
	
	log "Updating timestamp for the touch_file : [ ${touch_file} ] "
	touch -t ${file_name_list_tmstmp} ${touch_file}
	rc=`echo $?`
	
	if [ $rc -ne 0 ];then
		log "Unable to update the timestamp for touchfile [${touch_file}]. Please check."
		Email "FAILED : Unable to update touchfile timestamp "
		exit 1
	fi
	
	touchfile_time_stmp_upd=`istat ${touch_file} | grep "Last modified:" | awk -F' ' '{print $4,$5,$6}'`
	log "Touch_file [${touch_file}] timestamp after update : [ ${touchfile_time_stmp_upd} ]"	

	ls -ltr ${dest_path}/${file_prefix}_* | awk -F' ' '{print $9}' > ${file_name_list}
	
	#checking whether any file is empty
	for file_nm in `cat ${file_name_list}`; do
		if [ -s "${file_nm}" ];then
			continue
		else
			if [ ${empty_file_fail_flag} == "Y" ];then
				log "Empty File found : [ ${file_nm} ]"
				Email "FAILED : Empty file available"
				exit 1
			else
				log "File [ ${file_nm} ] is empty."
			fi
		fi
	done
	
	if [ ${file_list_flag} = "Y" ];then
		log "Creating the list of files in destination folder."
		ls -ltr ${dest_path}/${file_prefix}_* | awk -F' ' '{print $9}' > ${dest_file_list}
		chmod 775 ${dest_file_list}
		log "List File in destination path : [${dest_file_list}]"
		log "List File Content : "
		cat ${dest_file_list} >> ${LogFile}
	fi
	
	log "Copy process completed for file_prefix : [${file_prefix}]"	

else
	log "No files available as newer than [ ${touchfile_time_stmp_b4} ] in Source Directory : [${source_path}] "
	log "No ${file_prefix} file to copy"
	Email "FAILED : No new files available to copy"
	exit 1
fi

log "Removing temporary files"
rm ${file_name_list}

log "Process completed successfully"
log "SUCCESS : Copy Process completed"
exit 0
