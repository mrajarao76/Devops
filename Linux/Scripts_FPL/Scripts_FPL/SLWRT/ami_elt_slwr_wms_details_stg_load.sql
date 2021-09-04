\set on_error_stop on
BEGIN TRANSACTION;

\echo truncating table ami_stage.slwr_wms_details_stg ...

select ami_stage.truncate('ami_stage.slwr_wms_details_stg');

\echo inserting into table ami_stage.slwr_wms_details_stg ...


insert into ami_stage.slwr_wms_details_stg
(
	cd_wr
	,cd_status
	,dt_created
	,dt_wr_complete
	,dt_699_complete
	,dt_complete
	,dt_cancelled
	,mgr_area_code
	,srv_ctr_code
	,cd_crewhq
	,id_oper_assigned
	,cd_crew_completed
	,cd_job
	,actn_tkn
	,fac_addr
	,latitude
	,longitude
	,dvc_coor
	,fdr_num
	,id_coor
	,cust_acct_num
	,smrtlght_serl_num
	,ami_dvc_name
	,smrtlght_cmpt_id
	,nic_mac_addr
	,dvc_util_id
	,no_ext_sys_id
	,tckt_key
	,tckt_num
	,tckt_tp
	,trbl_rptd
	,tckt_cmnt
	,cd_crew
	,ts_update
	,dt_tckt_created
	,cd_assoc_dist
	,cd_assoc_wr
)
SELECT cd_wr
	,cd_status
	,dt_created
	,dt_wr_complete
	,dt_699_complete
	,dt_complete
	,dt_cancelled
	,mgr_area_code
	,srv_ctr_code
	,cd_crewhq
	,id_oper_assigned
	,cd_crew_completed
	,cd_job
	,actn_tkn
	,fac_addr
	,latitude
	,longitude
	,dvc_coor
	,fdr_num
	,id_coor
	,cust_acct_num
	,smrtlght_serl_num
	,ami_dvc_name
	,smrtlght_cmpt_id
	,nic_mac_addr
	,dvc_util_id
	,no_ext_sys_id
	,tckt_key
	,tckt_num
	,tckt_tp
	,trbl_rptd
	,tckt_cmnt
	,cd_crew
	,ts_update
	,dt_tckt_created
	,cd_assoc_dist
	,cd_assoc_wr
FROM (
	SELECT cd_wr
		,cd_status
		,dt_created
		,dt_wr_complete
		,dt_699_complete
		,dt_complete
		,dt_cancelled
		,mgr_area_code
		,srv_ctr_code
		,cd_crewhq
		,id_oper_assigned
		,cd_crew_completed
		,cd_job
		,actn_tkn
		,fac_addr
		,latitude
		,longitude
		,dvc_coor
		,fdr_num
		,id_coor
		,cust_acct_num
		,smrtlght_serl_num
		,ami_dvc_name
		,smrtlght_cmpt_id
		,nic_mac_addr
		,dvc_util_id
		,no_ext_sys_id
		,tckt_key
		,tckt_num
		,tckt_tp
		,trbl_rptd
		,tckt_cmnt
		,cd_crew
		,ts_update
		,dt_tckt_created
		,cd_assoc_dist
		,cd_assoc_wr
		,ROW_NUMBER() OVER (PARTITION BY cd_wr ORDER BY rnk) rn 
	FROM (
		SELECT wr.cd_wr
			,wr.cd_status
			,wr.ts_oper_recorded AS dt_created
			,wr.dt_complete AS dt_wr_complete
			,rpt.dt_699_complete
			,COALESCE(rpt.dt_699_complete,wr.dt_complete) dt_complete
			,wr.dt_cancelled
			,wr.cd_dist AS mgr_area_code
			,wr.cd_area AS srv_ctr_code
			,wr.cd_crewhq
			,wr.id_oper_assigned
			,wr.cd_crew_completed
			,rpt.cd_job
			,REPLACE(wr.ds_wr,chr(13)||chr(10),' ') AS actn_tkn
			--,replace(tkt.ad_facility, chr(239)||chr(191)||chr(189), ' ') as fac_addr
	             ,(tkt.ad_facility) as fac_addr
			,wr.ad_gr_2 AS latitude
			,wr.ad_gr_1 AS longitude
			,COALESCE(tkt.id_fac_coord,wr.id_location) AS dvc_coor
			,wr.txt_reference_3 AS fdr_num
			,wr.id_coordinate AS id_coor
			,tkt.id_fac_acct AS cust_acct_num
			,rmk.serial_num AS smrtlght_serl_num
			--place preference on photocell record found based on serial number
			--if not found, then consider record found based on dvc_coor/account
			,CASE WHEN sl1.ami_dvc_name IS NOT NULL THEN sl1.ami_dvc_name WHEN sl2.ami_dvc_name IS NOT NULL THEN sl2.ami_dvc_name WHEN sl3.ami_dvc_name IS NOT NULL THEN sl3.ami_dvc_name ELSE sl4.ami_dvc_name END ami_dvc_name
			,CASE WHEN sl1.ami_dvc_name IS NOT NULL THEN sl1.smrtlght_cmpt_id::INT WHEN sl2.ami_dvc_name IS NOT NULL THEN sl2.smrtlght_cmpt_id::INT WHEN sl3.ami_dvc_name IS NOT NULL THEN sl3.smrtlght_cmpt_id::INT ELSE sl4.smrtlght_cmpt_id::INT END smrtlght_cmpt_id
			,CASE WHEN sl1.ami_dvc_name IS NOT NULL THEN sl1.nic_mac_addr WHEN sl2.ami_dvc_name IS NOT NULL THEN sl2.nic_mac_addr WHEN sl3.ami_dvc_name IS NOT NULL THEN sl3.nic_mac_addr ELSE sl4.nic_mac_addr END nic_mac_addr
			,CASE WHEN sl1.ami_dvc_name IS NOT NULL THEN sl1.dvc_util_id WHEN sl2.ami_dvc_name IS NOT NULL THEN sl2.dvc_util_id WHEN sl3.ami_dvc_name IS NOT NULL THEN sl3.dvc_util_id ELSE sl4.dvc_util_id END dvc_util_id			
			,wr.no_ext_sys_id
			,wr.no_ext_job tckt_key
			,tkt.no_ticket AS tckt_num
			,tkt.tp_ticket AS tckt_tp
			,tkt.txt_complaint AS trbl_rptd
			,tkt.cm_comment AS tckt_cmnt
			,wr.cd_crew
			,wr.ts_update
			,to_timestamp(tkt.dt_ticket,'yymmddhh24mi') as dt_tckt_created
			,wr.cd_assoc_dist
			,wr.cd_assoc_wr
            --if multiple records found - give preference to records with photocell matched on serial number
            --if multiple records found, None Match on serial number, and multiple match on dvc_coor/account - give preference to record where address matches, if address does not match just pick one.
			,CASE 
				WHEN COUNT(1) OVER (PARTITION BY wr.cd_wr) = 1 THEN -1
				WHEN sl1.ami_dvc_name IS NOT NULL THEN -1
				WHEN sl2.ami_dvc_name IS NOT NULL THEN -1
				WHEN sl3.ami_dvc_name IS NOT NULL THEN -1
				WHEN sl4.ami_dvc_name IS NOT NULL AND tkt.ad_facility = sl4.prem_addr THEN -1
				ELSE ROW_NUMBER() OVER (PARTITION BY wr.cd_wr ORDER BY sl4.ami_dvc_name) 
			END rnk
		FROM ami.wms_sl_work_request wr 
--		FROM $$Schema_ami.$$TableName_src_wr wr
		LEFT OUTER JOIN ami.wms_sl_report_data rpt 
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_rpt rpt
			ON wr.cd_wr = rpt.cd_wr
		LEFT OUTER JOIN ami.wms_sl_ticket_detail tkt 
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_tkt tkt
			ON wr.cd_wr = tkt.cd_wr
		--parse serial number from remarks
		LEFT OUTER JOIN (
			SELECT cd_wr
				,CASE WHEN SUBSTRING(parsed_serial,1,2) = 'A0' AND LENGTH(parsed_serial) = 8 THEN parsed_serial
					WHEN SUBSTRING(parsed_serial,1,2) = '0X' AND LENGTH(parsed_serial) = 18 THEN parsed_serial
					ELSE NULL 
				END serial_num
			FROM (
				SELECT cd_wr
					,txt_remark 
					,UPPER(TRIM(SPLIT_PART(SUBSTRING(txt_remark, strpos(txt_remark, 'SN:'),22),' ',2))) parsed_serial
					,ROW_NUMBER() OVER (PARTITION BY cd_wr ORDER BY ts_remark, cd_seq) rn 
				FROM ami.wms_sl_work_request_remarks
--				FROM $$Schema_ami.$$TableName_src_rem
				WHERE tp_remark = 'SLTS' 
					AND (txt_remark LIKE '%SN: 0X%' or txt_remark LIKE '%SN: A0%')
			) a
			WHERE rn = 1
		) rmk
			ON wr.cd_wr = rmk.cd_wr
		--get photocell information based on serial number in text reference field
		LEFT OUTER JOIN ami.smart_light_photocell sl1
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_sp sl1
			ON wr.txt_reference_1 = sl1.smrtlght_serl_num
				AND sl1.efct_end_date = '9999-12-31'
				AND sl1.crnt_row_flag = 'Y'		
                --get photocell information based on serial number in the report table
		LEFT OUTER JOIN ami.smart_light_photocell sl2
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_sp sl2
			ON rpt.cd_serial_num = sl2.smrtlght_serl_num
				AND sl2.efct_end_date = '9999-12-31'
				AND sl2.crnt_row_flag = 'Y'
		--get photocell information based on serial number parsed from remarks
		LEFT OUTER JOIN ami.smart_light_photocell sl3
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_sp sl3
			ON rmk.serial_num = sl3.smrtlght_serl_num
				AND sl3.efct_end_date = '9999-12-31'
				AND sl3.crnt_row_flag = 'Y'
                --get photocell information based on dvc_coor/account
		LEFT OUTER JOIN ami.smart_light_photocell sl4
--		LEFT OUTER JOIN $$Schema_ami.$$TableName_src_sp sl4
			ON sl4.cust_acct_num = tkt.id_fac_acct::BIGINT
				AND sl4.smrtlght_dvc_coor= SUBSTR(COALESCE(tkt.id_fac_coord,wr.id_location),1,LENGTH(sl4.smrtlght_dvc_coor))
				AND sl4.efct_end_date = '9999-12-31'
				AND sl4.crnt_row_flag = 'Y'
		WHERE ((wr.fg_canceled = 'N' 
			AND (COALESCE(rpt.dt_699_complete, wr.dt_complete) IS NULL 
--			OR COALESCE(rpt.dt_699_complete, wr.dt_complete) > CURRENT_DATE - INTERVAL $$Interval_days)
			OR COALESCE(rpt.dt_699_complete, wr.dt_complete) > CURRENT_DATE - INTERVAL '90 days')
		)
--		OR (wr.fg_canceled = 'Y' and wr.dt_cancelled > CURRENT_DATE - INTERVAL $$Interval_days))
		OR (wr.fg_canceled = 'Y' and wr.dt_cancelled > CURRENT_DATE - INTERVAL '90 days'))
	) z
) x
WHERE rn = 1;

\copy (select 'SUCCESS') to '/home/ami0app/ami_scripts/bin/temp/slwr_wms_details_stg.txt';

COMMIT;