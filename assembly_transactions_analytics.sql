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
CLUSTER BY cd_plant, no_par
