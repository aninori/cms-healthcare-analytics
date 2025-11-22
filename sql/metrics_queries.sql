-- Bed Utilization Rate
SELECT
    "provider name",
    "state",
    "provider type",
    SUM("number of certified beds") AS no_of_certified_beds,
    SUM("average number of residents per day") AS total_avg_residents_per_day,
    ROUND(
        SUM("average number of residents per day") 
        / NULLIF(SUM("number of certified beds"), 0), 2
    ) AS bed_utilization_rate
FROM nh_providerinfo_oct2024_parquet
GROUP BY 1,2,3
ORDER BY bed_utilization_rate DESC;

-- Average Readmission Rate
SELECT
    "provider name",
    "state",
    ROUND(
        AVG("performance period: fy 2022 risk-standardized readmission rate"),
        2
    ) AS avg_readmission_rate
FROM fy_2024_snf_vbp_facility_performance_parquet
WHERE "performance period: fy 2022 risk-standardized readmission rate" IS NOT NULL
GROUP BY 1,2
ORDER BY avg_readmission_rate ASC;

-- Total Nurse Staffing Hours
SELECT 
    "provider name" AS provider_name,
    ROUND(
        AVG("reported total nurse staffing hours per resident per day"), 2
    ) AS avg_staffing_hours_per_resident
FROM nh_providerinfo_oct2024_parquet
GROUP BY "provider name";

-- Correlation between Staffing and Readmissions by State
WITH merged AS (
    SELECT
        vbp."provider name" AS provider_name,
        vbp."state" AS state,
        vbp."performance period: fy 2022 risk-standardized readmission rate" AS readmission_rate,
        prov."reported total nurse staffing hours per resident per day" AS nurse_hours
    FROM fy_2024_snf_vbp_facility_performance_parquet vbp
    JOIN nh_providerinfo_oct2024_parquet prov
        ON CAST(vbp."cms certification number (ccn)" AS VARCHAR) =
           CAST(prov."cms certification number (ccn)" AS VARCHAR)
    WHERE
        vbp."performance period: fy 2022 risk-standardized readmission rate" IS NOT NULL
        AND prov."reported total nurse staffing hours per resident per day" IS NOT NULL
)
SELECT
    state,
    ROUND(CORR(readmission_rate, nurse_hours), 4) AS correlation_staffing_readmission
FROM merged
GROUP BY state
ORDER BY correlation_staffing_readmission ASC;
