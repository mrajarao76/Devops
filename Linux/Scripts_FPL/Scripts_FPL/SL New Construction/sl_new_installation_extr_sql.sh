#! /bin/ksh
#########################################################################################
# This Script generates the SQL file based on the parameters passed to the script       #
#     Parameter 1 - SQL File Name                                                       #
#     Parameter 2 - Entry from Control file for the extract                             #
#########################################################################################

SQL_FILE=$1                     #This will be the name of the output SQL file
CONTROLFILE=$2                  #The entry in the control file
SCRIPT_NM=`basename $0`
TS=`date '+%Y%m%d_%H%M%S'`
LOGDIR=/informatica/InfaFiles/stage/ami/logs
LOGFILE=${LOGDIR}/${SCRIPT_NM}.${SQL_FILE}.${TS}.log

exec 1> ${SQL_FILE}

#echo "set enable_nestloop=true;"
echo "COPY (SELECT
cd_wr as work_request,
loc_num as location_number,
loc_seq_num as sequence_number,
fac_num as key_facility_number,
mntc_code as maintenance_code,
map_num as map_number,
grid_number as ddb_coordinate,
fix_styl as fixture_style,
lamp_type as lamp_type,
fix_wattage as wattage,
brkt_len as bracket_length,
case when orientation_code = 'PT' THEN 'T' else orientation_code end as orientation,
addr_ln_1 as address_1,
REPLACE(loc_dtl,'KFNPL:' ,'') as location_details,
voltage_level as voltage,
dt_699_complete as wms_approval,
cd_wr||'-'||loc_num||'-'||loc_seq_num as cmpt_num
FROM ami.wms_sl_new_install_dtl
WHERE
PRCS_FLAG ='N'
) TO STDOUT WITH DELIMITER '^' HEADER NULL '';"

echo "update ami.wms_sl_new_install_dtl
set PRCS_FLAG ='Y' WHERE
PRCS_FLAG ='N';"
