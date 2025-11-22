import os
import time
import boto3
import pandas as pd
import streamlit as st

ATHENA_DB = os.getenv("ATHENA_DB_NAME")              # e.g. nh_silver_db
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_LOCATION")  # e.g. s3://.../athena-output/
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")

session = boto3.Session()
athena = session.client("athena")


def run_athena_query(query: str) -> pd.DataFrame:
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )

    qid = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if status != "SUCCEEDED":
        return pd.DataFrame()

    result = athena.get_query_results(QueryExecutionId=qid)
    rows = result["ResultSet"]["Rows"]
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    data = []

    for row in rows[1:]:
        data.append([d.get("VarCharValue", None) for d in row["Data"]])

    return pd.DataFrame(data, columns=headers)


def main():
    st.set_page_config(page_title="Nursing Home Analytics", layout="wide")

    st.title("🏥 Nursing Home Quality & Staffing Analytics")

    st.markdown(
        """
        This dashboard analyzes publicly available nursing home data:
        - Bed utilization
        - Nurse staffing hours
        - Readmission performance
        - States with poor staffing and high penalties
        """
    )

    tab1, tab2, tab3 = st.tabs(
        [
            "Executive Overview",
            "Staffing vs Readmissions",
            "State-Level Risk View",
        ]
    )

    # 1) Executive overview metrics (bed utilization + staffing)
    with tab1:
        st.subheader("Executive Summary")

        bed_util_query = """
        SELECT
            "state",
            ROUND(AVG("average number of residents per day"), 2) AS avg_residents,
            ROUND(AVG("number of certified beds"), 2) AS avg_beds,
            ROUND(AVG("average number of residents per day")
                / NULLIF(AVG("number of certified beds"), 0), 3) AS avg_bed_utilization_ratio
        FROM nh_providerinfo_oct2024_parquet
        GROUP BY "state"
        """

        bed_df = run_athena_query(bed_util_query)

        if not bed_df.empty:
            bed_df["avg_bed_utilization_ratio"] = bed_df["avg_bed_utilization_ratio"].astype(float)
            national_bed_util = bed_df["avg_bed_utilization_ratio"].mean()

            col1, col2, col3 = st.columns(3)
            col1.metric("Avg. Bed Utilization (National)", f"{national_bed_util*100:.1f}%")
            col2.metric("States Covered", str(len(bed_df)))
            col3.metric("Data Source Tables", "Provider Info, VBP Facility Performance")

            st.markdown("### Bed Utilization by State")
            st.dataframe(bed_df)

        else:
            st.warning("No bed utilization data available from Athena.")

    # 2) Staffing vs readmission correlation
    with tab2:
        st.subheader("Staffing vs Readmission Performance")

        corr_query = """
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
        ORDER BY correlation_staffing_readmission ASC
        """

        corr_df = run_athena_query(corr_query)

        if not corr_df.empty:
            corr_df["correlation_staffing_readmission"] = corr_df["correlation_staffing_readmission"].astype(float)

            st.markdown("### Correlation between Staffing & Readmissions by State")
            st.dataframe(corr_df)

            st.bar_chart(
                corr_df.set_index("state")["correlation_staffing_readmission"]
            )

            worst_state = corr_df.sort_values("correlation_staffing_readmission").head(1)
            best_state = corr_df.sort_values("correlation_staffing_readmission", ascending=False).head(1)

            st.markdown("#### Interpretation")
            st.write(
                f"- States with **strong negative correlation** (closer to -1) show that higher nurse staffing is associated with *lower* readmission rates."
            )
            st.write(
                f"- States with correlation near **0** have weak/no linear relationship between staffing and readmissions."
            )

        else:
            st.warning("No correlation data available from Athena.")

    # 3) State-Level Risk View: low staffing + high fines
    with tab3:
        st.subheader("State-Level Staffing & Penalty Risk")

        risk_query = """
        SELECT
            prov."state" AS state,
            ROUND(AVG(prov."reported total nurse staffing hours per resident per day"), 2) AS avg_nurse_hours,
            SUM(prov."total amount of fines in dollars") AS total_fines
        FROM nh_providerinfo_oct2024_parquet prov
        WHERE
            prov."reported total nurse staffing hours per resident per day" IS NOT NULL
        GROUP BY prov."state"
        HAVING
            AVG(prov."reported total nurse staffing hours per resident per day") < 3.5
            AND SUM(prov."total amount of fines in dollars") IS NOT NULL
        ORDER BY total_fines DESC
        """

        risk_df = run_athena_query(risk_query)

        if not risk_df.empty:
            risk_df["avg_nurse_hours"] = risk_df["avg_nurse_hours"].astype(float)
            risk_df["total_fines"] = risk_df["total_fines"].astype(float)

            st.markdown("### States with Low Staffing & High Financial Penalties")
            st.dataframe(risk_df)

            st.bar_chart(
                risk_df.set_index("state")[["avg_nurse_hours", "total_fines"]]
            )

        else:
            st.info("No high-risk staffing + fines combination detected from current data.")


if __name__ == "__main__":
    main()