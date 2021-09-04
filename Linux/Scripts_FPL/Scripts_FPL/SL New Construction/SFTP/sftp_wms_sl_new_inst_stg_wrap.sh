#!/usr/bin/ksh

#################################################################################################
# This script triggers the following scripts to fetch files from AWS S3 to on-prem server.
# SFTP Process : /home/ami0app/ami_scripts/bin/SFTP_S3_to_OnPrem.sh
# COPY Process : /home/ami0app/ami_scripts/bin/file_copy_process.sh
#################################################################################################
SCRIPT_NM_WRAP=`basename $0`
RUNDATE=`date '+%m%d%Y'`
export PrcsName="wms_sl_new_inst_stg"
DREAM_ON_PATH=/home/ami0app/ami_scripts/bin/dreamon_files
LOGFILE=/informatica/InfaFiles/stage/ami/logs/${SCRIPT_NM_WRAP}_${RUNDATE}.log

if [ -f $DREAM_ON_PATH/${PrcsName}_dream_on.txt ];then
	print "[`date +\"%Y-%m-%d %H:%M:%S\"`] Dream on activated. Exiting the script" >> ${LOGFILE}
	exit 0
else
	print "[`date +\"%Y-%m-%d %H:%M:%S\"`] Initiating SFTP Process for ${PrcsName} files." >> ${LOGFILE}
	
	sh /home/ami0app/ami_scripts/bin/SFTP_S3_to_OnPrem.sh ${PrcsName} sftp_s3to_onprem_control_file_DEV >> ${LOGFILE} 2>&1
	#sh /home/ami0app/ami_scripts/bin/SFTP_S3_to_OnPrem.sh ${PrcsName} sftp_s3to_onprem_control_file_QA >> ${LOGFILE} 2>&1
	#sh /home/ami0app/ami_scripts/bin/SFTP_S3_to_OnPrem.sh ${PrcsName} sftp_s3to_onprem_control_file_PROD >> ${LOGFILE} 2>&1
	
	rc_sftp=`echo $?`
	
	if [ ${rc_sftp} -eq 0 ];then
		print "[`date +\"%Y-%m-%d %H:%M:%S\"`] Initiating Copy Process for ${PrcsName} files." >> ${LOGFILE}
		sh /home/ami0app/ami_scripts/bin/file_copy_process.sh ${PrcsName} file_copy_prcs_control_file >> ${LOGFILE} 2>&1
		rc_copy=`echo $?`
		
		if [ ${rc_copy} -ne 0 ];then
			print "[`date +\"%Y-%m-%d %H:%M:%S\"`] Copy Process for ${PrcsName} files FAILED." >> ${LOGFILE}
			exit ${rc_copy}
		else
			exit ${rc_copy}
		fi
	else
		print "[`date +\"%Y-%m-%d %H:%M:%S\"`] SFTP Process for ${PrcsName} files FAILED." >> ${LOGFILE}
		exit ${rc_sftp}
	fi
fi
