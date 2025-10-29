-- ===================================================================================
-- Configuration
-- ===================================================================================
DECLARE run_timestamp_suffix STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());
DECLARE target_dataset STRING DEFAULT 'prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_loh_txnmaster_fdp';
DECLARE temp_table_expiration_ts TIMESTAMP DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR);

-- ===================================================================================
-- Step 1: Materialize Dimension and Transaction Tables for this Run as Expiring Tables
-- ===================================================================================

-- Part Master Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_part_master_%s`
CLUSTER BY part_prefix_num, part_base_num, part_suffix_num, part_control_num
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT DISTINCT
    TRIM(W65.gsdb_site_code) AS gsdb_site_code,
    TRIM(W65.part_num) AS part_num,
    CASE WHEN REPLACE(TRIM(W65.part_base_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(W65.part_base_num), CHR(0), '') END AS part_base_num,
    CASE WHEN REPLACE(TRIM(W65.part_prefix_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(W65.part_prefix_num), CHR(0), '') END AS part_prefix_num,
    CASE WHEN REPLACE(TRIM(W65.part_suffix_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(W65.part_suffix_num), CHR(0), '') END AS part_suffix_num,
    TRIM(W65.part_desc) AS part_desc,
    CASE WHEN REPLACE(TRIM(W65.part_control_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(W65.part_control_num), CHR(0), '') END AS part_control_num,
    W65.effective_in_date,
    W65.effective_out_date
FROM `prj-dfdl-411-fstrt-p-411.bq_411_fstrt_trg_lc_vw.gfstw65_p_loc_part_rptg_vw_vw` AS W65
WHERE df_row_created_date = (
  SELECT MAX(df_row_created_date)
  FROM `prj-dfdl-411-fstrt-p-411.bq_411_fstrt_trg_lc_vw.gfstw65_p_loc_part_rptg_vw_vw`
)
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- Exchange Rates
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_exchange_rates_%s`
CLUSTER BY cd_currency
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT
    TRIM(cd_currency) AS cd_currency,
    dt_effective,
    COALESCE(LEAD(dt_effective) OVER (PARTITION BY cd_currency ORDER BY dt_effective), '9999-12-31') AS dt_effective_out,
    CAST(TRIM(pc_us_ratio) AS NUMERIC) AS pc_us_ratio
FROM `prj-dfdl-159-monex-p-0159.bq_159_monex_lnd_lc_vw.gmxp006_exchrate_vw`
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- GSDB Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_gsdb_data_%s`
CLUSTER BY cd_supplr_site
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT TRIM(cd_supplr_site) AS cd_supplr_site, TRIM(na_site_name) AS na_site_name, TRIM(cd_sp_country_code) AS cd_sp_country_code
FROM `prj-dfdl-50-gsdb-p-0050.bq_50_gsdb_db2_lnd_lc_vw.gbt271a_site_vw`
UNION DISTINCT
SELECT TRIM(cd_supplr_site) AS cd_supplr_site, TRIM(na_site_name) AS na_site_name, TRIM(cd_sp_country_code) AS cd_sp_country_code
FROM `prj-dfdl-50-gsdb-p-0050.bq_50_gsdb_db2_lnd_lc_vw.gbt271b_site_vw`
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- Cycle Check Analyst Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_cc_analyst_data_%s`
CLUSTER BY cd_plant, no_part_prefix, no_part_base, no_part_control
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT DISTINCT
    TRIM(cd_plant) AS cd_plant,
    CASE WHEN REPLACE(TRIM(no_part_base), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(no_part_base), CHR(0), '') END AS no_part_base,
    CASE WHEN REPLACE(TRIM(no_part_prefix), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(no_part_prefix), CHR(0), '') END AS no_part_prefix,
    CASE WHEN REPLACE(TRIM(no_part_suffix), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(no_part_suffix), CHR(0), '') END AS no_part_suffix,
    CASE WHEN REPLACE(TRIM(no_part_control), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(no_part_control), CHR(0), '') END AS no_part_control,
    TRIM(cd_cycle_chk_anal) AS cd_cycle_chk_anal,
    qt_unt_bws_cur
FROM `prj-dfdl-77-cmms3-p-0077.bq_77_cmms3_lnd_lc_vw.cpnt023_part_net_vw`
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- Hierarchy Master Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_hierarchy_master_%s`
CLUSTER BY gsdb
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT
    TRIM(gsdb) AS gsdb, TRIM(oploc) AS oploc, TRIM(plant_name) AS plant_name, TRIM(plant_code) AS plant_code, TRIM(plant_subcode) AS plant_subcode, TRIM(plant_type) AS plant_type, TRIM(division_display_name) AS division_display_name, TRIM(unit_type) AS unit_type, TRIM(region) AS region, TRIM(currency) AS currency, TRIM(dom) AS dom, TRIM(skilled_team) AS skilled_team, TRIM(profit_center) AS profit_center
FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.master_ref_lc_vw`
WHERE audit_create_date = (
  SELECT MAX(audit_create_date)
  FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.master_ref_lc_vw`
)
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- Hierarchy Plant Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_hierarchy_plant_%s`
CLUSTER BY gsdb_1, scrap_dept_cmms
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT
    LPAD(CAST(SAFE_CAST(TRIM(universal_id) AS INT64) AS STRING), 10, '0') AS universal_id,
    TRIM(direct_indirect) AS direct_indirect,
    TRIM(team_team_leader_level) AS team_team_leader_level,
    TRIM(zone_process_coach) AS zone_process_coach,
    TRIM(area) AS area,
    TRIM(department) AS department,
    TRIM(global_common_area) AS global_common_area,
    TRIM(paypoint_department) AS paypoint_department,
    TRIM(plant_subcode) AS plant_subcode_1,
    TRIM(plant_code) AS plant_code_1,
    TRIM(oploc) AS oploc_1,
    TRIM(gsdb) AS gsdb_1,
    TRIM(timekeeping_system) AS timekeeping_system,
    CASE WHEN SAFE_CAST(TRIM(timekeeping_department) AS INT64) IS NOT NULL AND LENGTH(TRIM(timekeeping_department)) < 4 THEN LPAD(TRIM(timekeeping_department), 4, '0') ELSE TRIM(timekeeping_department) END AS timekeeping_department,
    CASE WHEN SAFE_CAST(TRIM(scrap_dept_cmms) AS INT64) IS NOT NULL AND LENGTH(TRIM(scrap_dept_cmms)) < 4 THEN LPAD(TRIM(scrap_dept_cmms), 4, '0') ELSE TRIM(scrap_dept_cmms) END AS scrap_dept_cmms,
    CASE WHEN SAFE_CAST(TRIM(crib_dept_cpars) AS INT64) IS NOT NULL AND LENGTH(TRIM(crib_dept_cpars)) < 4 THEN LPAD(TRIM(crib_dept_cpars), 4, '0') ELSE TRIM(crib_dept_cpars) END AS crib_dept_cpars,
    TRIM(ierp_department_sap) AS ierp_department_sap, TRIM(pos_aurora) AS pos_aurora, TRIM(fab_card_citi) AS fab_card_citi, TRIM(maximo) AS maximo, TRIM(fis_assembly_paypoint) AS fis_assembly_paypoint, TRIM(pto_volumes_workcenter_cmms) AS pto_volumes_workcenter_cmms, TRIM(ngavs_paypoint) AS ngavs_paypoint
FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.plant_ref_lc_vw`
WHERE audit_create_date = (
  SELECT MAX(audit_create_date)
  FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.plant_ref_lc_vw`
)
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;


-- Hierarchy Workcenter Data
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_hierarchy_workcenter_%s`
CLUSTER BY gsdb, work_center_cmms, department_cmms
OPTIONS(expiration_timestamp = @temp_ts) AS
SELECT
    TRIM(gsdb) AS gsdb,
    CASE WHEN SAFE_CAST(TRIM(department_cmms) AS INT64) IS NOT NULL AND LENGTH(TRIM(department_cmms)) < 4 THEN LPAD(TRIM(department_cmms), 4, '0') ELSE TRIM(department_cmms) END AS department_cmms,
    TRIM(work_center_cmms) AS work_center_cmms,
    LPAD(CAST(SAFE_CAST(TRIM(universal_id) AS INT64) AS STRING), 10, '0') AS universal_id,
    TRIM(zone_process_coach) AS zone_process_coach,
    TRIM(workcenter_name) AS workcenter_name,
    TRIM(part_base_num) AS part_base_num,
    TRIM(part_prefix_num) AS part_prefix_num,
    TRIM(part_suffix_num) AS part_suffix_num
FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.workcenter_ref_vw`
WHERE audit_create_date = (
  SELECT MAX(audit_create_date)
  FROM `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_org_hierarchy_fdp_dwc_vw.workcenter_ref_vw`
)
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;

-- Transaction Data with Pre-Calculated Keys
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.temp_cmms_with_keys_%s`
CLUSTER BY effective_plant_key, part_prefix_num_clean, part_base_num_clean, part_control_num_clean
OPTIONS(expiration_timestamp = @temp_ts) AS
WITH
  cmms_cleaned_source AS (
    SELECT
        *,
        CASE WHEN REPLACE(TRIM(part_base_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(part_base_num), CHR(0), '') END AS part_base_num_clean,
        CASE WHEN REPLACE(TRIM(part_prefix_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(part_prefix_num), CHR(0), '') END AS part_prefix_num_clean,
        CASE WHEN REPLACE(TRIM(part_suffix_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(part_suffix_num), CHR(0), '') END AS part_suffix_num_clean,
        CASE WHEN REPLACE(TRIM(part_control_num), CHR(0), '') = '' THEN NULL ELSE REPLACE(TRIM(part_control_num), CHR(0), '') END AS part_control_num_clean
    FROM `prj-dfdl-144-ierp-p-0144.bq_144_ierp_pcf_lc_vw.cmms_txn_msgs_staging_vw`
    WHERE
        TRIM(status) = 'PROCESSED'
        AND source_business_date IS NOT NULL
        AND TRIM(txn_rule_code) IN ('51','51E','34','64','64C','50','80','80E','80U','80V','80W','58','74','CE','PC')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            TRIM(object_storage_id),
            txn_last_update_timestamp,
            part_base_num,
            part_prefix_num,
            part_suffix_num,
            part_control_num,
            TRIM(txn_rule_code)
        ORDER BY
            df_bq_update_ts DESC
    ) = 1
  )
SELECT
    *,
    -- This is the "effective plant key" calculated in one single place.
    (CASE WHEN TRIM(txn_rule_code) = '64' THEN CASE WHEN REPLACE(gsdb_shipped_from_code, CHR(0), '') = '' THEN NULL ELSE TRIM(gsdb_shipped_from_code) END ELSE TRIM(plant) END) AS effective_plant_key,
    -- This is the "effective department key"
    (CASE WHEN SAFE_CAST(TRIM(cmms_dept_code) AS INT64) IS NOT NULL AND LENGTH(TRIM(cmms_dept_code)) < 4 THEN LPAD(TRIM(cmms_dept_code), 4, '0') ELSE CASE WHEN REPLACE(cmms_dept_code, CHR(0), '') = '' THEN NULL ELSE TRIM(cmms_dept_code) END END) AS effective_dept_key,
    CASE WHEN REPLACE(work_center_num, CHR(0), '') = '' THEN NULL ELSE TRIM(work_center_num) END AS work_center_num_clean
FROM cmms_cleaned_source
""", target_dataset, run_timestamp_suffix)
USING temp_table_expiration_ts AS temp_ts;


-- ===================================================================================
-- Step 1.5: Conditional Full Reload Trigger
-- ===================================================================================
IF CURRENT_DATE() = '2025-10-03' THEN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE `prj-dfdw-1119-f360-p-1119.bq_1119_fin_mfg_loh_txnmaster_fdp.fin026_mfg_cmms_txn_master_ct`';
END IF;

-- ===================================================================================
-- Step 2: Execute the Incremental Load using a MERGE Statement
-- ===================================================================================
EXECUTE IMMEDIATE FORMAT(
"""
MERGE INTO `%s.fin026_mfg_cmms_txn_master_ct` AS T
USING (
    WITH
      -- CTE 1: Join all dimension tables to the materialized source data using pre-calculated keys.
      SourceData_Joined AS (
        SELECT
            cmms.*,
            pmd.part_desc,
            pmd.effective_in_date AS pmd_effective_in_date,
            cca.cd_cycle_chk_anal,
            cca.qt_unt_bws_cur,
            cusinv.pc_us_ratio AS cusinv_pc_us_ratio,
            ep.pc_us_ratio AS ep_pc_us_ratio,
            le.pc_us_ratio AS le_pc_us_ratio,
            po.pc_us_ratio AS po_pc_us_ratio,
            tp.pc_us_ratio AS tp_pc_us_ratio,
            gsdb.na_site_name,
            gsdb.cd_sp_country_code,
            iso.iso_cntry_alpha_three_cd,
            hmd.oploc, hmd.plant_name, hmd.plant_code, hmd.plant_subcode, hmd.plant_type, hmd.division_display_name, hmd.unit_type, hmd.region, hmd.currency, hmd.dom, hmd.skilled_team, hmd.profit_center,
            hpd.universal_id AS hpd_universal_id, hpd.direct_indirect, hpd.team_team_leader_level, hpd.zone_process_coach AS hpd_zone_process_coach, hpd.area, hpd.department, hpd.global_common_area, hpd.paypoint_department, hpd.plant_subcode_1, hpd.plant_code_1, hpd.oploc_1, hpd.timekeeping_system, hpd.timekeeping_department, hpd.scrap_dept_cmms, hpd.crib_dept_cpars, hpd.ierp_department_sap, hpd.pos_aurora, hpd.fab_card_citi, hpd.maximo, hpd.fis_assembly_paypoint, hpd.pto_volumes_workcenter_cmms, hpd.ngavs_paypoint,
            hwd.universal_id AS hwd_universal_id, hwd.zone_process_coach AS hwd_zone_process_coach
        FROM `%s.temp_cmms_with_keys_%s` AS cmms
        LEFT JOIN `%s.temp_part_master_%s` AS pmd
            ON COALESCE(cmms.part_base_num_clean, 'NULL') = COALESCE(pmd.part_base_num, 'NULL')
            AND COALESCE(cmms.part_prefix_num_clean, 'NULL') = COALESCE(pmd.part_prefix_num, 'NULL')
            AND COALESCE(cmms.part_suffix_num_clean, 'NULL') = COALESCE(pmd.part_suffix_num, 'NULL')
            AND COALESCE(cmms.part_control_num_clean, 'NULL') = COALESCE(pmd.part_control_num, 'NULL')
            AND COALESCE(cmms.business_event_effective_date, SAFE_CAST(cmms.txn_timestamp_cdate AS DATE), EXTRACT(DATE FROM cmms.txn_last_update_timestamp), '2100-01-01') >= pmd.effective_in_date
            AND COALESCE(cmms.business_event_effective_date, SAFE_CAST(cmms.txn_timestamp_cdate AS DATE), EXTRACT(DATE FROM cmms.txn_last_update_timestamp), '2100-01-01') < pmd.effective_out_date
        LEFT JOIN `%s.temp_cc_analyst_data_%s` AS cca
            ON cmms.effective_plant_key = cca.cd_plant
            AND COALESCE(cmms.part_base_num_clean, 'NULL') = COALESCE(cca.no_part_base, 'NULL')
            AND COALESCE(cmms.part_prefix_num_clean, 'NULL') = COALESCE(cca.no_part_prefix, 'NULL')
            AND COALESCE(cmms.part_suffix_num_clean, 'NULL') = COALESCE(cca.no_part_suffix, 'NULL')
            AND COALESCE(cmms.part_control_num_clean, 'NULL') = COALESCE(cca.no_part_control, 'NULL')
        LEFT JOIN `%s.temp_exchange_rates_%s` AS cusinv ON TRIM(cmms.cusinv_currency_code) = cusinv.cd_currency AND cmms.business_event_effective_date >= cusinv.dt_effective AND cmms.business_event_effective_date < cusinv.dt_effective_out
        LEFT JOIN `%s.temp_exchange_rates_%s` AS ep ON TRIM(cmms.ep_currency_code) = ep.cd_currency AND cmms.business_event_effective_date >= ep.dt_effective AND cmms.business_event_effective_date < ep.dt_effective_out
        LEFT JOIN `%s.temp_exchange_rates_%s` AS le ON TRIM(cmms.le_currency_code) = le.cd_currency AND cmms.business_event_effective_date >= le.dt_effective AND cmms.business_event_effective_date < le.dt_effective_out
        LEFT JOIN `%s.temp_exchange_rates_%s` AS po ON TRIM(cmms.po_currency_code) = po.cd_currency AND cmms.business_event_effective_date >= po.dt_effective AND cmms.business_event_effective_date < po.dt_effective_out
        LEFT JOIN `%s.temp_exchange_rates_%s` AS tp ON TRIM(cmms.tp_currency_code) = tp.cd_currency AND cmms.business_event_effective_date >= tp.dt_effective AND cmms.business_event_effective_date < tp.dt_effective_out
        LEFT JOIN `%s.temp_gsdb_data_%s` AS gsdb ON cmms.effective_plant_key = gsdb.cd_supplr_site
        LEFT JOIN `prj-dfdl-584-iso-p-584.country_code_iso.iso_country` AS iso ON gsdb.cd_sp_country_code = iso.iso_cntry_alpha_two_cd
        LEFT JOIN `%s.temp_hierarchy_master_%s` AS hmd ON cmms.effective_plant_key = hmd.gsdb
        LEFT JOIN `%s.temp_hierarchy_plant_%s` AS hpd ON cmms.effective_plant_key = hpd.gsdb_1 AND cmms.effective_dept_key = hpd.scrap_dept_cmms
        LEFT JOIN `%s.temp_hierarchy_workcenter_%s` AS hwd
            ON cmms.effective_plant_key = hwd.gsdb
            AND cmms.work_center_num_clean = hwd.work_center_cmms
            AND cmms.effective_dept_key = hwd.department_cmms
    ),
    -- CTE 2: Centralize all cleaning, casting, calculations, and transformations.
    SourceData_Prepared AS (
      SELECT
        *,
        -- Cleaned and parsed fields
        safe.parse_date('%%Y-%%m-%%d', TRIM(disbursed_date)) AS disbursed_date_parsed,
        safe.parse_date('%%Y-%%m-%%d', TRIM(rejected_date)) AS rejected_date_parsed,
        CASE WHEN TRIM(txn_rule_code) = '64' THEN TRIM(plant) ELSE CASE WHEN REPLACE(gsdb_shipped_from_code, CHR(0), '') = '' THEN NULL ELSE TRIM(gsdb_shipped_from_code) END END AS gsdb_shipped_from_code_clean,
        CONCAT(IFNULL(part_prefix_num_clean, ''), '-', IFNULL(part_base_num_clean, ''), '-', IFNULL(part_suffix_num_clean, '')) AS part_num_full,
        PARSE_DATE("%%Y-%%m-%%d",TRIM(price_last_changed_date_s4)) AS price_last_changed_date_s4_parsed,
        PARSE_TIMESTAMP("%%Y-%%m-%%d-%%H.%%M.%%E*S",TRIM(txn_timestamp_cdate)) AS txn_timestamp_cdate_parsed,
        CASE WHEN REPLACE(ref_part_base_num, CHR(0), '') = '' THEN NULL ELSE TRIM(ref_part_base_num) END AS ref_part_base_num_clean,
        CASE WHEN REPLACE(ref_part_control_num, CHR(0), '') = '' THEN NULL ELSE TRIM(ref_part_control_num) END AS ref_part_control_num_clean,
        CASE WHEN REPLACE(ref_part_prefix_num, CHR(0), '') = '' THEN NULL ELSE TRIM(ref_part_prefix_num) END AS ref_part_prefix_num_clean,
        CASE WHEN REPLACE(ref_part_suffix_num, CHR(0), '') = '' THEN NULL ELSE TRIM(ref_part_suffix_num) END AS ref_part_suffix_num_clean,
        CASE WHEN TRIM(txn_rule_code) = '50' THEN TRIM(cd_cycle_chk_anal) ELSE NULL END AS cycle_check_analyst_final,
        COALESCE(hwd_universal_id, hpd_universal_id) AS hierplt_universal_id_final,
        COALESCE(hwd_zone_process_coach, hpd_zone_process_coach) AS hierplt_zone_process_coach_final,
        -- Numeric and calculated fields
        SAFE_CAST(TRIM(part_four_wall_variance_qty) AS INT64) AS part_four_wall_variance_qty_int,
        SAFE_CAST(TRIM(part_labor_hour_adj_qty) AS INT64) AS part_labor_hour_adj_qty_int,
        SAFE_CAST(TRIM(s4_material_doc_year) AS INT64) AS s4_material_doc_year_int,
        SAFE_CAST(TRIM(verification_cnt) AS INT64) AS verification_cnt_int,
        SAFE_CAST(TRIM(verification_crct_cnt) AS INT64) AS verification_crct_cnt_int,
        SAFE_CAST(TRIM(txn_qty) AS INT64) * CASE WHEN TRIM(txn_rule_code) = '34' THEN -1 ELSE 1 END AS txn_qty_signed,
        SAFE_CAST(TRIM(cusinv_cost_amt) AS NUMERIC) AS cusinv_cost_amt_num,
        SAFE_CAST(TRIM(le_cost_amt) AS NUMERIC) AS le_cost_amt_num,
        SAFE_CAST(TRIM(po_unit_cost_amt) AS NUMERIC) AS po_unit_cost_amt_num,
        SAFE_CAST(TRIM(tp_unit_cost_amt) AS NUMERIC) AS tp_unit_cost_amt_num,
        SAFE_CAST(TRIM(ep_cost_amt) AS NUMERIC) AS ep_cost_amt_num,
        CAST(unit_current_work_std_num AS NUMERIC) / 10 AS unit_current_work_std_num_calc,
        (CASE
            WHEN TRIM(txn_rule_code) = '80' AND business_event_effective_date = '2025-08-23' THEN
                CASE
                    WHEN receipt_date = '2025-08-23' THEN CAST(unit_budgted_work_std_num AS NUMERIC) / 10
                    WHEN receipt_date > '2025-08-23' THEN CAST(unit_budgted_work_std_num AS NUMERIC)
                END
            WHEN TRIM(txn_rule_code) = '80' AND business_event_effective_date > '2025-08-23' THEN CAST(unit_budgted_work_std_num AS NUMERIC)
            ELSE CAST(unit_budgted_work_std_num AS NUMERIC) / 10
        END) AS unit_budgted_work_std_num_calc
      FROM SourceData_Joined
    ),
    -- Final SELECT layer where all deduplication and final calculations happen.
    SourceData_Final AS (
      SELECT
        *,
        -- Final calculation for budgeted work std num
        CASE
            WHEN unit_budgted_work_std_num_calc <> 0 THEN unit_budgted_work_std_num_calc
            WHEN (unit_budgted_work_std_num_calc = 0 OR unit_budgted_work_std_num_calc IS NULL) AND qt_unt_bws_cur <> 0 THEN qt_unt_bws_cur
            ELSE 0
        END AS unit_budgted_work_std_num_final,
        -- Final cost calculations using the prepared numeric columns
        cusinv_cost_amt_num * cusinv_pc_us_ratio AS cusinv_cost_amt_usd_final,
        ep_cost_amt_num * ep_pc_us_ratio AS ep_cost_amt_usd_final,
        le_cost_amt_num * le_pc_us_ratio AS le_cost_amt_usd_final,
        po_unit_cost_amt_num * po_pc_us_ratio AS po_unit_cost_amt_usd_final,
        tp_unit_cost_amt_num * tp_pc_us_ratio AS tp_unit_cost_amt_usd_final,
        ep_cost_amt_num * txn_qty_signed AS extended_actual_cost_final,
        ep_cost_amt_num * ep_pc_us_ratio * txn_qty_signed AS extended_actual_cost_usd_final
      FROM SourceData_Prepared
      QUALIFY
          ROW_NUMBER() OVER (
              PARTITION BY
                  TRIM(object_storage_id),
                  txn_last_update_timestamp,
                  part_base_num_clean,
                  part_prefix_num_clean,
                  part_suffix_num_clean,
                  part_control_num_clean,
                  TRIM(txn_rule_code)
              ORDER BY
                  df_bq_update_ts DESC,
                  pmd_effective_in_date DESC
          ) = 1
    )
-- The alias for the final source data that the MERGE statement will use
SELECT * FROM SourceData_Final
) AS S
ON T.fin026_id = S.id
WHEN MATCHED AND S.txn_last_update_timestamp > T.fin026_txn_last_update_timestamp THEN
  UPDATE SET
    fin026_adv_shipping_notice_num = S.adv_shipping_notice_num, fin026_area_code = S.area_code, fin026_authorization_num = S.authorization_num, fin026_bill_of_lading_cnum = S.bill_of_lading_cnum, fin026_burden_dept_num = S.burden_dept_num, fin026_business_event_effective_date = S.business_event_effective_date, fin026_business_unit = S.business_unit, fin026_carrier_code = S.carrier_code, fin026_cmms_dept_code = S.effective_dept_key, fin026_cmms_division_code = S.cmms_division_code, fin026_cmms_userid = S.cmms_userid, fin026_conveyance_num = S.conveyance_num, fin026_correction_ind = S.correction_ind, fin026_correction_reason_code = S.correction_reason_code, fin026_count_point_flag = S.count_point_flag, fin026_created_by = S.created_by, fin026_created_ts = S.created_ts, fin026_cusinv_cost_amt = S.cusinv_cost_amt_num, fin026_cusinv_currency_code = S.cusinv_currency_code, fin026_cust_part_num = S.cust_part_num, fin026_dealer_id = S.dealer_id, fin026_delivery_terms_code = S.delivery_terms_code, fin026_die_tryout_part_ind = S.die_tryout_part_ind, fin026_disbursed_date = S.disbursed_date_parsed, fin026_end_item_group_code = S.end_item_group_code, fin026_engineering_change_cnum = S.engineering_change_cnum, fin026_eod_missed_flag = S.eod_missed_flag, fin026_eod_recon_flag = S.eod_recon_flag, fin026_ep_cost_amt = S.ep_cost_amt_num, fin026_ep_currency_code = S.ep_currency_code, fin026_follow_up_analyst_code = S.follow_up_analyst_code, fin026_free_on_board_term_code = S.free_on_board_term_code, fin026_freight_bill_num = S.freight_bill_num, fin026_freight_payment_term_code = S.freight_payment_term_code, fin026_gsdb_actual_receipt_loc_code = S.gsdb_actual_receipt_loc_code, fin026_gsdb_alter_shipped_to_code = S.gsdb_alter_shipped_to_code, fin026_gsdb_customer_location_code = S.gsdb_customer_location_code, fin026_gsdb_shipped_from_code = S.gsdb_shipped_from_code_clean, fin026_gsdb_sold_to_code = S.gsdb_sold_to_code, fin026_handling_charge_num = S.handling_charge_num, fin026_ics_cust_type_code = S.ics_cust_type_code, fin026_interface_id = S.interface_id, fin026_inbound_freight_auth_num = S.inbound_freight_auth_num, fin026_inbound_transit_mode_code = S.inbound_transit_mode_code, fin026_interface_id = S.interface_id, fin026_le_cost_amt = S.le_cost_amt_num, fin026_le_currency_code = S.le_currency_code, fin026_lift_tag_cnum = S.lift_tag_cnum, fin026_mazda_asn_num = S.mazda_asn_num, fin026_object_storage_id = S.object_storage_id, fin026_order_group_cnum = S.order_group_cnum, fin026_original_lift_tag_cnum = S.original_lift_tag_cnum, fin026_outbound_transit_mode_code = S.outbound_transit_mode_code, fin026_packing_slip_cnum = S.packing_slip_cnum, fin026_pallet_charge_num = S.pallet_charge_num, fin026_part_num = S.part_num_full, fin026_part_base_num = S.part_base_num_clean, fin026_part_control_num = S.part_control_num_clean, fin026_part_four_wall_variance_qty = S.part_four_wall_variance_qty_int, fin026_part_labor_hour_adj_qty = S.part_labor_hour_adj_qty_int, fin026_part_prefix_num = S.part_prefix_num_clean, fin026_part_status_code = S.part_status_code, fin026_part_suffix_num = S.part_suffix_num_clean, fin026_part_type_code = S.part_type_code, fin026_part_weight_num = S.part_weight_num, fin026_payment_ind = S.payment_ind, fin026_plant = S.effective_plant_key, fin026_po_currency_code = S.po_currency_code, fin026_po_unit_cost_amt = S.po_unit_cost_amt_num, fin026_premium_freight_code = S.premium_freight_code, fin026_price_last_changed_date_s4 = S.price_last_changed_date_s4_parsed, fin026_product_group_code = S.product_group_code, fin026_pur_order_intercompany_bu_code = S.pur_order_intercompany_bu_code, fin026_quality_reject_code = S.quality_reject_code, fin026_reason_code = S.reason_code, fin026_receipt_date = S.receipt_date, fin026_received_from_warehouse_code = S.received_from_warehouse_code, fin026_ref_part_base_num = S.ref_part_base_num_clean, fin026_ref_part_control_num = S.ref_part_control_num_clean, fin026_ref_part_prefix_num = S.ref_part_prefix_num_clean, fin026_ref_part_suffix_num = S.ref_part_suffix_num_clean, fin026_rejected_date = S.rejected_date_parsed, fin026_request_for_shipper_rqstor_id = S.request_for_shipper_rqstor_id, fin026_rough_piece_weight_num = S.rough_piece_weight_num, fin026_s4_material_doc_number = S.s4_material_doc_number, fin026_s4_material_doc_year = S.s4_material_doc_year_int, fin026_s4_plant = S.s4_plant, fin026_s4_posting_date = S.s4_posting_date, fin026_s4_production_order_number = S.s4_production_order_number, fin026_scrap_rate_num = S.scrap_rate_num, fin026_scrap_tag_cnum = S.scrap_tag_cnum, fin026_shift_code = S.shift_code, fin026_shipment_date = S.shipment_date, fin026_shipment_type_code = S.shipment_type_code, fin026_shipper_cnum = S.shipper_cnum, fin026_sold_to_vat_cnum = S.sold_to_vat_cnum, fin026_source_business_date = S.source_business_date, fin026_status = S.status, fin026_substitute_part_ind = S.substitute_part_ind, fin026_tolerance_ind = S.tolerance_ind, fin026_tp_currency_code = S.tp_currency_code, fin026_tp_unit_cost_amt = S.tp_unit_cost_amt_num, fin026_txn_last_update_timestamp = S.txn_last_update_timestamp, fin026_txn_qty = S.txn_qty_signed, fin026_txn_rule_code = S.txn_rule_code, fin026_txn_supplier_type_code = S.txn_supplier_type_code, fin026_txn_timestamp_cdate = S.txn_timestamp_cdate_parsed, fin026_unit_budgted_work_std_num = S.unit_budgted_work_std_num_final, fin026_unit_current_work_std_num = S.unit_current_work_std_num_calc, fin026_uom_code = S.uom_code, fin026_updated_by = S.updated_by, fin026_updated_ts = S.updated_ts, fin026_verification_cnt = S.verification_cnt_int, fin026_verification_crct_cnt = S.verification_crct_cnt_int, fin026_verification_crct_desc = S.verification_crct_desc, fin026_work_center_num = S.work_center_num_clean, fin026_work_order_num = S.work_order_num, fin026_part_desc = S.part_desc, fin026_cycle_check_analyst = S.cycle_check_analyst_final, fin026_cusinv_cost_amt_usd = S.cusinv_cost_amt_usd_final, fin026_ep_cost_amt_usd = S.ep_cost_amt_usd_final, fin026_le_cost_amt_usd = S.le_cost_amt_usd_final, fin026_po_unit_cost_amt_usd = S.po_unit_cost_amt_usd_final, fin026_tp_unit_cost_amt_usd = S.tp_unit_cost_amt_usd_final, fin026_extended_actual_cost = S.extended_actual_cost_final, fin026_extended_actual_cost_usd = S.extended_actual_cost_usd_final, fin026_na_site_name = S.na_site_name, fin026_cd_sp_country_code = S.cd_sp_country_code, fin026_iso_cntry_alpha_three_cd = S.iso_cntry_alpha_three_cd, fin026_hiermstr_oploc = S.oploc, fin026_hiermstr_plant_name = S.plant_name, fin026_hiermstr_plant_code = S.plant_code, fin026_hiermstr_plant_subcode = S.plant_subcode, fin026_hiermstr_plant_type = S.plant_type, fin026_hiermstr_division_display_name = S.division_display_name, fin026_hiermstr_unit_type = S.unit_type, fin026_hiermstr_region = S.region, fin026_hiermstr_currency = S.currency, fin026_hiermstr_dom = S.dom, fin026_hiermstr_skilled_team = S.skilled_team, fin026_hiermstr_profit_center = S.profit_center, fin026_hierplt_universal_id = S.hierplt_universal_id_final, fin026_hierplt_direct_indirect = S.direct_indirect, fin026_hierplt_team_team_leader_level = S.team_team_leader_level, fin026_hierplt_zone_process_coach = S.hierplt_zone_process_coach_final, fin026_hierplt_area = S.area, fin026_hierplt_department = S.department, fin026_hierplt_global_common_area = S.global_common_area, fin026_hierplt_paypoint_department = S.paypoint_department, fin026_hierplt_plant_subcode = S.plant_subcode_1, fin026_hierplt_plant_code = S.plant_code_1, fin026_hierplt_oploc = S.oploc_1, fin026_hierplt_timekeeping_system = S.timekeeping_system, fin026_hierplt_timekeeping_department = S.timekeeping_department, fin026_hierplt_scrap_dept_cmms = S.scrap_dept_cmms, fin026_hierplt_crib_dept_cpars = S.crib_dept_cpars, fin026_hierplt_ierp_department_sap = S.ierp_department_sap, fin026_hierplt_pos_aurora = S.pos_aurora, fin026_hierplt_fab_card_citi = S.fab_card_citi, fin026_hierplt_maximo = S.maximo, fin026_hierplt_fis_assembly_paypoint = S.fis_assembly_paypoint, fin026_hierplt_pto_volumes_workcenter_cmms = S.pto_volumes_workcenter_cmms, fin026_hierplt_ngavs_paypoint = S.ngavs_paypoint, _dfgdia_iso3_country_std_cnty = S.iso_cntry_alpha_three_cd, fin026_gdia_updt_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET THEN
  INSERT (fin026_adv_shipping_notice_num, fin026_area_code, fin026_authorization_num, fin026_bill_of_lading_cnum, fin026_burden_dept_num, fin026_business_event_effective_date, fin026_business_unit, fin026_carrier_code, fin026_cmms_dept_code, fin026_cmms_division_code, fin026_cmms_userid, fin026_conveyance_num, fin026_correction_ind, fin026_correction_reason_code, fin026_count_point_flag, fin026_created_by, fin026_created_ts, fin026_cusinv_cost_amt, fin026_cusinv_currency_code, fin026_cust_part_num, fin026_dealer_id, fin026_delivery_terms_code, fin026_die_tryout_part_ind, fin026_disbursed_date, fin026_end_item_group_code, fin026_engineering_change_cnum, fin026_eod_missed_flag, fin026_eod_recon_flag, fin026_ep_cost_amt, fin026_ep_currency_code, fin026_follow_up_analyst_code, fin026_free_on_board_term_code, fin026_freight_bill_num, fin026_freight_payment_term_code, fin026_gsdb_actual_receipt_loc_code, fin026_gsdb_alter_shipped_to_code, fin026_gsdb_customer_location_code, fin026_gsdb_shipped_from_code, fin026_gsdb_sold_to_code, fin026_handling_charge_num, fin026_ics_cust_type_code, fin026_id, fin026_inbound_freight_auth_num, fin026_inbound_transit_mode_code, fin026_interface_id, fin026_le_cost_amt, fin026_le_currency_code, fin026_lift_tag_cnum, fin026_mazda_asn_num, fin026_object_storage_id, fin026_order_group_cnum, fin026_original_lift_tag_cnum, fin026_outbound_transit_mode_code, fin026_packing_slip_cnum, fin026_pallet_charge_num, fin026_part_num, fin026_part_base_num, fin026_part_control_num, fin026_part_four_wall_variance_qty, fin026_part_labor_hour_adj_qty, fin026_part_prefix_num, fin026_part_status_code, fin026_part_suffix_num, fin026_part_type_code, fin026_part_weight_num, fin026_payment_ind, fin026_plant, fin026_po_currency_code, fin026_po_unit_cost_amt, fin026_premium_freight_code, fin026_price_last_changed_date_s4, fin026_product_group_code, fin026_pur_order_intercompany_bu_code, fin026_quality_reject_code, fin026_reason_code, fin026_receipt_date, fin026_received_from_warehouse_code, fin026_ref_part_base_num, fin026_ref_part_control_num, fin026_ref_part_prefix_num, fin026_ref_part_suffix_num, fin026_rejected_date, fin026_request_for_shipper_rqstor_id, fin026_rough_piece_weight_num, fin026_s4_material_doc_number, fin026_s4_material_doc_year, fin026_s4_plant, fin026_s4_posting_date, fin026_s4_production_order_number, fin026_scrap_rate_num, fin026_scrap_tag_cnum, fin026_shift_code, fin026_shipment_date, fin026_shipment_type_code, fin026_shipper_cnum, fin026_sold_to_vat_cnum, fin026_source_business_date, fin026_status, fin026_substitute_part_ind, fin026_tolerance_ind, fin026_tp_currency_code, fin026_tp_unit_cost_amt, fin026_txn_last_update_timestamp, fin026_txn_qty, fin026_txn_rule_code, fin026_txn_supplier_type_code, fin026_txn_timestamp_cdate, fin026_unit_budgted_work_std_num, fin026_unit_current_work_std_num, fin026_uom_code, fin026_updated_by, fin026_updated_ts, fin026_verification_cnt, fin026_verification_crct_cnt, fin026_verification_crct_desc, fin026_work_center_num, fin026_work_order_num, fin026_part_desc, fin026_cycle_check_analyst, fin026_cusinv_cost_amt_usd, fin026_ep_cost_amt_usd, fin026_le_cost_amt_usd, fin026_po_unit_cost_amt_usd, fin026_tp_unit_cost_amt_usd, fin026_extended_actual_cost, fin026_extended_actual_cost_usd, fin026_na_site_name, fin026_cd_sp_country_code, fin026_iso_cntry_alpha_three_cd, fin026_hiermstr_oploc, fin026_hiermstr_plant_name, fin026_hiermstr_plant_code, fin026_hiermstr_plant_subcode, fin026_hiermstr_plant_type, fin026_hiermstr_division_display_name, fin026_hiermstr_unit_type, fin026_hiermstr_region, fin026_hiermstr_currency, fin026_hiermstr_dom, fin026_hiermstr_skilled_team, fin026_hiermstr_profit_center, fin026_hierplt_universal_id, fin026_hierplt_direct_indirect, fin026_hierplt_team_team_leader_level, fin026_hierplt_zone_process_coach, fin026_hierplt_area, fin026_hierplt_department, fin026_hierplt_global_common_area, fin026_hierplt_paypoint_department, fin026_hierplt_plant_subcode, fin026_hierplt_plant_code, fin026_hierplt_oploc, fin026_hierplt_timekeeping_system, fin026_hierplt_timekeeping_department, fin026_hierplt_scrap_dept_cmms, fin026_hierplt_crib_dept_cpars, fin026_hierplt_ierp_department_sap, fin026_hierplt_pos_aurora, fin026_hierplt_fab_card_citi, fin026_hierplt_maximo, fin026_hierplt_fis_assembly_paypoint, fin026_hierplt_pto_volumes_workcenter_cmms, fin026_hierplt_ngavs_paypoint, _dfgdia_iso3_country_std_cnty, fin026_gdia_updt_ts)
  VALUES (S.adv_shipping_notice_num, S.area_code, S.authorization_num, S.bill_of_lading_cnum, S.burden_dept_num, S.business_event_effective_date, S.business_unit, S.carrier_code, S.effective_dept_key, S.cmms_division_code, S.cmms_userid, S.conveyance_num, S.correction_ind, S.correction_reason_code, S.count_point_flag, S.created_by, S.created_ts, S.cusinv_cost_amt_num, S.cusinv_currency_code, S.cust_part_num, S.dealer_id, S.delivery_terms_code, S.die_tryout_part_ind, S.disbursed_date_parsed, S.end_item_group_code, S.engineering_change_cnum, S.eod_missed_flag, S.eod_recon_flag, S.ep_cost_amt_num, S.ep_currency_code, S.follow_up_analyst_code, S.free_on_board_term_code, S.freight_bill_num, S.freight_payment_term_code, S.gsdb_actual_receipt_loc_code, S.gsdb_alter_shipped_to_code, S.gsdb_customer_location_code, S.gsdb_shipped_from_code_clean, S.gsdb_sold_to_code, S.handling_charge_num, S.ics_cust_type_code, S.id, S.inbound_freight_auth_num, S.inbound_transit_mode_code, S.interface_id, S.le_cost_amt_num, S.le_currency_code, S.lift_tag_cnum, S.mazda_asn_num, S.object_storage_id, S.order_group_cnum, S.original_lift_tag_cnum, S.outbound_transit_mode_code, S.packing_slip_cnum, S.pallet_charge_num, S.part_num_full, S.part_base_num_clean, S.part_control_num_clean, S.part_four_wall_variance_qty_int, S.part_labor_hour_adj_qty_int, S.part_prefix_num_clean, S.part_status_code, S.part_suffix_num_clean, S.part_type_code, S.part_weight_num, S.payment_ind, S.effective_plant_key, S.po_currency_code, S.po_unit_cost_amt_num, S.premium_freight_code, S.price_last_changed_date_s4_parsed, S.product_group_code, S.pur_order_intercompany_bu_code, S.quality_reject_code, S.reason_code, S.receipt_date, S.received_from_warehouse_code, S.ref_part_base_num_clean, S.ref_part_control_num_clean, S.ref_part_prefix_num_clean, S.ref_part_suffix_num_clean, S.rejected_date_parsed, S.request_for_shipper_rqstor_id, S.rough_piece_weight_num, S.s4_material_doc_number, S.s4_material_doc_year_int, S.s4_plant, S.s4_posting_date, S.s4_production_order_number, S.scrap_rate_num, S.scrap_tag_cnum, S.shift_code, S.shipment_date, S.shipment_type_code, S.shipper_cnum, S.sold_to_vat_cnum, S.source_business_date, S.status, S.substitute_part_ind, S.tolerance_ind, S.tp_currency_code, S.tp_unit_cost_amt_num, S.txn_last_update_timestamp, S.txn_qty_signed, S.txn_rule_code, S.txn_supplier_type_code, S.txn_timestamp_cdate_parsed, S.unit_budgted_work_std_num_final, S.unit_current_work_std_num_calc, S.uom_code, S.updated_by, S.updated_ts, S.verification_cnt_int, S.verification_crct_cnt_int, S.verification_crct_desc, S.work_center_num_clean, S.work_order_num, S.part_desc, S.cycle_check_analyst_final, S.cusinv_cost_amt_usd_final, S.ep_cost_amt_usd_final, S.le_cost_amt_usd_final, S.po_unit_cost_amt_usd_final, S.tp_unit_cost_amt_usd_final, S.extended_actual_cost_final, S.extended_actual_cost_usd_final, S.na_site_name, S.cd_sp_country_code, S.iso_cntry_alpha_three_cd, S.oploc, S.plant_name, S.plant_code, S.plant_subcode, S.plant_type, S.division_display_name, S.unit_type, S.region, S.currency, S.dom, S.skilled_team, S.profit_center, S.hierplt_universal_id_final, S.direct_indirect, S.team_team_leader_level, S.hierplt_zone_process_coach_final, S.area, S.department, S.global_common_area, S.paypoint_department, S.plant_subcode_1, S.plant_code_1, S.oploc_1, S.timekeeping_system, S.timekeeping_department, S.scrap_dept_cmms, S.crib_dept_cpars, S.ierp_department_sap, S.pos_aurora, S.fab_card_citi, S.maximo, S.fis_assembly_paypoint, S.pto_volumes_workcenter_cmms, S.ngavs_paypoint, S.iso_cntry_alpha_three_cd, CURRENT_TIMESTAMP());
""",
  target_dataset, -- For MERGE target table
  target_dataset, run_timestamp_suffix, -- for cmms_with_keys
  target_dataset, run_timestamp_suffix, -- for pmd
  target_dataset, run_timestamp_suffix, -- for cca
  target_dataset, run_timestamp_suffix, -- for cusinv
  target_dataset, run_timestamp_suffix, -- for ep
  target_dataset, run_timestamp_suffix, -- for le
  target_dataset, run_timestamp_suffix, -- for po
  target_dataset, run_timestamp_suffix, -- for tp
  target_dataset, run_timestamp_suffix, -- for gsdb
  target_dataset, run_timestamp_suffix, -- for hm
  target_dataset, run_timestamp_suffix, -- for hp
  target_dataset, run_timestamp_suffix  -- for hwd
);
