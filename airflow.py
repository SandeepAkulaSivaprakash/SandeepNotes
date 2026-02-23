"""
=============================================================================
DAG: schema_change_monitor
=============================================================================
Purpose:
    - Waits for today's ODS_ENGINE.SCHEMA snapshot to land in BigQuery
    - Runs a diff query comparing today vs yesterday's schema snapshot
    - Sends a color-coded HTML email to a mailing list if any changes found
    - Always sends an email (with high-priority alert) if DEPRECATED columns detected
    - Skips the email entirely if no changes are found

Schedule: Daily at 07:00 UTC (adjust to run after your data load completes)

Requirements:
    - Airflow BigQuery provider:  apache-airflow-providers-google
    - Airflow SMTP connection configured (conn_id: smtp_default) OR
      SendGrid/SES connection depending on your Composer setup
    - A GCP connection configured in Airflow (conn_id: google_cloud_default)
    - The service account used by Composer must have BigQuery Data Viewer
      and BigQuery Job User roles on the ODS_ENGINE dataset

Configuration:
    Update the CONFIG block below before deploying.
=============================================================================
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.email import send_email

# =============================================================================
# ✏️  CONFIGURATION  — update these values before deploying
# =============================================================================
CONFIG: dict[str, Any] = {
    # GCP / BigQuery
    "gcp_conn_id": "google_cloud_default",
    "project_id": "your-gcp-project-id",          # ← change me
    "dataset": "ODS_ENGINE",
    "table": "SCHEMA",

    # Email
    "mailing_list": [                               # ← change me
        "data-team@yourcompany.com",
        "analytics@yourcompany.com",
    ],
    "email_from": "airflow-alerts@yourcompany.com", # ← change me
    "email_subject_prefix": "[Schema Monitor]",

    # DAG schedule — runs daily at 07:00 UTC
    # Adjust so it fires AFTER your daily ODS load completes
    "schedule_interval": "0 7 * * *",

    # How long to wait for today's data to arrive (seconds)
    "data_sensor_timeout": 60 * 60 * 2,  # 2 hours
    "data_sensor_poke_interval": 60 * 5,  # check every 5 minutes
}
# =============================================================================

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# SQL: wait for today's snapshot to land
# -----------------------------------------------------------------------------
SENSOR_SQL = """
SELECT COUNT(1) AS cnt
FROM `{project}.{dataset}.{table}`
WHERE SNAPSHOT_DT = CURRENT_DATE()
""".format(
    project=CONFIG["project_id"],
    dataset=CONFIG["dataset"],
    table=CONFIG["table"],
)

# -----------------------------------------------------------------------------
# SQL: schema diff — today vs yesterday
# -----------------------------------------------------------------------------
DIFF_SQL = """
WITH
latest_dates AS (
  SELECT DISTINCT SNAPSHOT_DT
  FROM `{project}.{dataset}.{table}`
  ORDER BY SNAPSHOT_DT DESC
  LIMIT 2
),
today_dt     AS (SELECT MAX(SNAPSHOT_DT) AS dt FROM latest_dates),
yesterday_dt AS (SELECT MIN(SNAPSHOT_DT) AS dt FROM latest_dates),

today AS (
  SELECT * FROM `{project}.{dataset}.{table}`
  WHERE SNAPSHOT_DT = (SELECT dt FROM today_dt)
),
yesterday AS (
  SELECT * FROM `{project}.{dataset}.{table}`
  WHERE SNAPSHOT_DT = (SELECT dt FROM yesterday_dt)
),

new_tables AS (
  SELECT DISTINCT
    'NEW TABLE'                                    AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    NULL                                           AS old_value,
    NULL                                           AS new_value,
    'Table did not exist in previous snapshot'     AS change_description
  FROM today t
  WHERE t.TABLE_NAME NOT IN (SELECT DISTINCT TABLE_NAME FROM yesterday)
),

dropped_tables AS (
  SELECT DISTINCT
    'DROPPED TABLE'                                AS change_type,
    y.TABLE_NAME, y.COLUMN_NAME,
    NULL AS old_value, NULL AS new_value,
    'Table no longer exists in current snapshot'   AS change_description
  FROM yesterday y
  WHERE y.TABLE_NAME NOT IN (SELECT DISTINCT TABLE_NAME FROM today)
),

new_columns AS (
  SELECT
    'NEW COLUMN'                        AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    NULL                                AS old_value,
    CONCAT('DATA_TYPE: ',  IFNULL(t.DATA_TYPE,'NULL'),
           ' | IS_NULLABLE: ', IFNULL(t.IS_NULLABLE,'NULL'),
           ' | PRIMARY_KEY: ', IFNULL(t.PRIMARY_KEY,'NULL'),
           ' | FOREIGN_KEY: ', IFNULL(t.FOREIGN_KEY,'NULL')) AS new_value,
    'Column was added to the table'     AS change_description
  FROM today t
  LEFT JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE y.COLUMN_NAME IS NULL
    AND t.TABLE_NAME IN (SELECT DISTINCT TABLE_NAME FROM yesterday)
),

dropped_columns AS (
  SELECT
    'DROPPED COLUMN'                    AS change_type,
    y.TABLE_NAME, y.COLUMN_NAME,
    CONCAT('DATA_TYPE: ',  IFNULL(y.DATA_TYPE,'NULL'),
           ' | IS_NULLABLE: ', IFNULL(y.IS_NULLABLE,'NULL'),
           ' | PRIMARY_KEY: ', IFNULL(y.PRIMARY_KEY,'NULL'),
           ' | FOREIGN_KEY: ', IFNULL(y.FOREIGN_KEY,'NULL')) AS old_value,
    NULL                                AS new_value,
    'Column was removed from the table' AS change_description
  FROM yesterday y
  LEFT JOIN today t
    ON y.TABLE_NAME = t.TABLE_NAME AND y.COLUMN_NAME = t.COLUMN_NAME
  WHERE t.COLUMN_NAME IS NULL
    AND y.TABLE_NAME IN (SELECT DISTINCT TABLE_NAME FROM today)
),

datatype_changes AS (
  SELECT
    'DATA TYPE CHANGE' AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    y.DATA_TYPE AS old_value, t.DATA_TYPE AS new_value,
    CONCAT('Data type changed from [', IFNULL(y.DATA_TYPE,'NULL'),
           '] to [', IFNULL(t.DATA_TYPE,'NULL'), ']') AS change_description
  FROM today t JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.DATA_TYPE,'') != IFNULL(y.DATA_TYPE,'')
),

nullable_changes AS (
  SELECT
    'NULLABILITY CHANGE' AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    y.IS_NULLABLE AS old_value, t.IS_NULLABLE AS new_value,
    CONCAT('Nullability changed from [', IFNULL(y.IS_NULLABLE,'NULL'),
           '] to [', IFNULL(t.IS_NULLABLE,'NULL'), ']') AS change_description
  FROM today t JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.IS_NULLABLE,'') != IFNULL(y.IS_NULLABLE,'')
),

pk_changes AS (
  SELECT
    'PRIMARY KEY CHANGE' AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    y.PRIMARY_KEY AS old_value, t.PRIMARY_KEY AS new_value,
    CONCAT('Primary key flag changed from [', IFNULL(y.PRIMARY_KEY,'NULL'),
           '] to [', IFNULL(t.PRIMARY_KEY,'NULL'), ']') AS change_description
  FROM today t JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.PRIMARY_KEY,'') != IFNULL(y.PRIMARY_KEY,'')
),

fk_changes AS (
  SELECT
    'FOREIGN KEY CHANGE' AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    y.FOREIGN_KEY AS old_value, t.FOREIGN_KEY AS new_value,
    CONCAT('Foreign key changed from [', IFNULL(y.FOREIGN_KEY,'NULL'),
           '] to [', IFNULL(t.FOREIGN_KEY,'NULL'), ']') AS change_description
  FROM today t JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.FOREIGN_KEY,'') != IFNULL(y.FOREIGN_KEY,'')
),

description_changes AS (
  SELECT
    CASE
      WHEN LOWER(IFNULL(t.DESCRIPTION,'')) LIKE '%deprecated%'
       AND LOWER(IFNULL(y.DESCRIPTION,'')) NOT LIKE '%deprecated%'
      THEN 'DEPRECATED COLUMN'
      ELSE 'DESCRIPTION CHANGE'
    END AS change_type,
    t.TABLE_NAME, t.COLUMN_NAME,
    y.DESCRIPTION AS old_value, t.DESCRIPTION AS new_value,
    CASE
      WHEN LOWER(IFNULL(t.DESCRIPTION,'')) LIKE '%deprecated%'
       AND LOWER(IFNULL(y.DESCRIPTION,'')) NOT LIKE '%deprecated%'
      THEN CONCAT('COLUMN MARKED AS DEPRECATED. Was: [',
                  IFNULL(y.DESCRIPTION,'NULL'), '] Now: [',
                  IFNULL(t.DESCRIPTION,'NULL'), ']')
      ELSE CONCAT('Description changed from [', IFNULL(y.DESCRIPTION,'NULL'),
                  '] to [', IFNULL(t.DESCRIPTION,'NULL'), ']')
    END AS change_description
  FROM today t JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.DESCRIPTION,'') != IFNULL(y.DESCRIPTION,'')
)

SELECT
  (SELECT dt FROM today_dt)     AS snapshot_dt_today,
  (SELECT dt FROM yesterday_dt) AS snapshot_dt_yesterday,
  change_type, TABLE_NAME, COLUMN_NAME,
  old_value, new_value, change_description
FROM (
  SELECT * FROM new_tables
  UNION ALL SELECT * FROM dropped_tables
  UNION ALL SELECT * FROM new_columns
  UNION ALL SELECT * FROM dropped_columns
  UNION ALL SELECT * FROM datatype_changes
  UNION ALL SELECT * FROM nullable_changes
  UNION ALL SELECT * FROM pk_changes
  UNION ALL SELECT * FROM fk_changes
  UNION ALL SELECT * FROM description_changes
)
ORDER BY
  CASE change_type
    WHEN 'DEPRECATED COLUMN'  THEN 1
    WHEN 'DROPPED TABLE'      THEN 2
    WHEN 'NEW TABLE'          THEN 3
    WHEN 'DROPPED COLUMN'     THEN 4
    WHEN 'NEW COLUMN'         THEN 5
    WHEN 'DATA TYPE CHANGE'   THEN 6
    WHEN 'PRIMARY KEY CHANGE' THEN 7
    WHEN 'FOREIGN KEY CHANGE' THEN 8
    WHEN 'NULLABILITY CHANGE' THEN 9
    ELSE                           10
  END,
  TABLE_NAME, COLUMN_NAME
""".format(
    project=CONFIG["project_id"],
    dataset=CONFIG["dataset"],
    table=CONFIG["table"],
)

# -----------------------------------------------------------------------------
# Colour map for change types (used in HTML email)
# -----------------------------------------------------------------------------
CHANGE_COLOURS = {
    "DEPRECATED COLUMN":  {"bg": "#FF4444", "fg": "#FFFFFF", "emoji": "⚠️"},
    "DROPPED TABLE":      {"bg": "#FF6B6B", "fg": "#FFFFFF", "emoji": "🗑️"},
    "NEW TABLE":          {"bg": "#51CF66", "fg": "#FFFFFF", "emoji": "🆕"},
    "DROPPED COLUMN":     {"bg": "#FFB347", "fg": "#000000", "emoji": "➖"},
    "NEW COLUMN":         {"bg": "#74C0FC", "fg": "#000000", "emoji": "➕"},
    "DATA TYPE CHANGE":   {"bg": "#DA77F2", "fg": "#FFFFFF", "emoji": "🔄"},
    "PRIMARY KEY CHANGE": {"bg": "#FFA94D", "fg": "#000000", "emoji": "🔑"},
    "FOREIGN KEY CHANGE": {"bg": "#63E6BE", "fg": "#000000", "emoji": "🔗"},
    "NULLABILITY CHANGE": {"bg": "#A9E34B", "fg": "#000000", "emoji": "❓"},
    "DESCRIPTION CHANGE": {"bg": "#DEE2E6", "fg": "#000000", "emoji": "📝"},
}
DEFAULT_COLOUR = {"bg": "#F8F9FA", "fg": "#000000", "emoji": "ℹ️"}


# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def _wait_for_todays_data(**context) -> bool:
    """
    BigQuery sensor: polls until today's SNAPSHOT_DT row count > 0.
    Used as a ShortCircuitOperator — if data never arrives within timeout,
    the task will fail and downstream tasks are skipped.
    """
    hook = BigQueryHook(
        gcp_conn_id=CONFIG["gcp_conn_id"],
        use_legacy_sql=False,
    )
    client = hook.get_client(project_id=CONFIG["project_id"])
    query_job = client.query(SENSOR_SQL)
    results = list(query_job.result())
    count = results[0]["cnt"] if results else 0
    log.info("Today's snapshot row count: %s", count)
    if count == 0:
        raise ValueError(
            "Today's SNAPSHOT_DT data has not yet landed in "
            f"{CONFIG['dataset']}.{CONFIG['table']}. "
            "Will retry..."
        )
    return True


def _run_diff_query(**context) -> None:
    """
    Executes the schema diff SQL and pushes the result rows to XCom.
    Rows are serialised as a list of dicts so they survive XCom serialisation.
    """
    hook = BigQueryHook(
        gcp_conn_id=CONFIG["gcp_conn_id"],
        use_legacy_sql=False,
    )
    client = hook.get_client(project_id=CONFIG["project_id"])
    log.info("Running schema diff query...")
    query_job = client.query(DIFF_SQL)
    rows = [dict(row) for row in query_job.result()]
    log.info("Schema diff returned %d change(s).", len(rows))

    # Convert date objects to strings so XCom can serialise them
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

    context["ti"].xcom_push(key="diff_rows", value=rows)


def _should_send_email(**context) -> bool:
    """
    ShortCircuitOperator function.
    Returns True (proceed) if there are any changes, False (skip) otherwise.
    """
    rows = context["ti"].xcom_pull(key="diff_rows", task_ids="run_diff_query")
    if not rows:
        log.info("No schema changes detected. Skipping email.")
        return False
    log.info("%d change(s) detected. Proceeding to send email.", len(rows))
    return True


def _build_summary(rows: list[dict]) -> dict[str, int]:
    """Returns a count per change_type for the email summary banner."""
    summary: dict[str, int] = {}
    for row in rows:
        ct = row.get("change_type", "UNKNOWN")
        summary[ct] = summary.get(ct, 0) + 1
    return summary


def _rows_to_csv(rows: list[dict]) -> str:
    """Converts rows to a CSV string for attachment."""
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _build_html_email(rows: list[dict], today: str, yesterday: str) -> str:
    """Builds a fully styled HTML email body."""
    summary = _build_summary(rows)
    has_deprecated = "DEPRECATED COLUMN" in summary

    # ---------- summary banner ----------
    banner_bg = "#FF4444" if has_deprecated else "#1C7ED6"
    banner_title = (
        "⚠️ DEPRECATED COLUMNS DETECTED — Schema Changes Report"
        if has_deprecated
        else "📋 Schema Change Report"
    )

    summary_pills = ""
    for ct, cnt in sorted(summary.items(), key=lambda x: x[0]):
        colour = CHANGE_COLOURS.get(ct, DEFAULT_COLOUR)
        summary_pills += (
            f'<span style="display:inline-block;margin:3px;padding:4px 10px;'
            f'border-radius:12px;background:{colour["bg"]};color:{colour["fg"]};'
            f'font-size:12px;font-weight:600;">'
            f'{colour["emoji"]} {ct}: {cnt}</span>'
        )

    # ---------- table rows ----------
    table_rows_html = ""
    for row in rows:
        ct = row.get("change_type", "")
        colour = CHANGE_COLOURS.get(ct, DEFAULT_COLOUR)
        badge = (
            f'<span style="padding:3px 8px;border-radius:10px;'
            f'background:{colour["bg"]};color:{colour["fg"]};'
            f'font-size:11px;font-weight:700;white-space:nowrap;">'
            f'{colour["emoji"]} {ct}</span>'
        )

        def cell(val: Any) -> str:
            return f'<td style="padding:8px 10px;border-bottom:1px solid #E9ECEF;font-size:12px;vertical-align:top;">{val or "&nbsp;"}</td>'

        table_rows_html += (
            f"<tr>"
            f"{cell(badge)}"
            f"{cell(row.get('TABLE_NAME',''))}"
            f"{cell(row.get('COLUMN_NAME',''))}"
            f"{cell(row.get('old_value',''))}"
            f"{cell(row.get('new_value',''))}"
            f"{cell(row.get('change_description',''))}"
            f"</tr>"
        )

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <style>
    body {{ font-family: Arial, sans-serif; background:#F8F9FA; margin:0; padding:0; }}
    .wrapper {{ max-width:980px; margin:20px auto; background:#fff;
                border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.1); overflow:hidden; }}
    .banner {{ background:{banner_bg}; color:#fff; padding:20px 28px; }}
    .banner h1 {{ margin:0 0 4px; font-size:20px; }}
    .banner p  {{ margin:0; font-size:13px; opacity:0.9; }}
    .summary   {{ padding:16px 28px; background:#F1F3F5; border-bottom:1px solid #DEE2E6; }}
    .content   {{ padding:20px 28px; }}
    table  {{ width:100%; border-collapse:collapse; }}
    thead th {{
      background:#343A40; color:#fff; padding:10px 10px;
      font-size:12px; text-align:left; white-space:nowrap;
    }}
    tbody tr:hover {{ background:#F8F9FA; }}
    .footer {{ padding:14px 28px; font-size:11px; color:#868E96;
               border-top:1px solid #DEE2E6; text-align:center; }}
  </style>
</head>
<body>
<div class="wrapper">

  <div class="banner">
    <h1>{banner_title}</h1>
    <p>Comparing <strong>{yesterday}</strong> → <strong>{today}</strong>
       &nbsp;|&nbsp; <strong>{len(rows)}</strong> change(s) detected</p>
  </div>

  <div class="summary">
    <strong style="font-size:13px;">Summary by change type:</strong><br><br>
    {summary_pills}
  </div>

  <div class="content">
    <table>
      <thead>
        <tr>
          <th>Change Type</th>
          <th>Table</th>
          <th>Column</th>
          <th>Old Value</th>
          <th>New Value</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {table_rows_html}
      </tbody>
    </table>
  </div>

  <div class="footer">
    Generated by <strong>schema_change_monitor</strong> Airflow DAG
    &nbsp;|&nbsp; {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
    &nbsp;|&nbsp; Source: <code>{CONFIG['project_id']}.{CONFIG['dataset']}.{CONFIG['table']}</code>
  </div>

</div>
</body>
</html>
"""
    return html


def _send_email(**context) -> None:
    """Builds and dispatches the HTML email with CSV attachment."""
    rows = context["ti"].xcom_pull(key="diff_rows", task_ids="run_diff_query")
    if not rows:
        log.info("No rows to email.")
        return

    today     = rows[0].get("snapshot_dt_today", "today")
    yesterday = rows[0].get("snapshot_dt_yesterday", "yesterday")
    summary   = _build_summary(rows)
    has_deprecated = "DEPRECATED COLUMN" in summary

    # Build subject line
    dep_flag = " ⚠️ DEPRECATED COLUMNS DETECTED |" if has_deprecated else ""
    subject = (
        f"{CONFIG['email_subject_prefix']}{dep_flag} "
        f"{len(rows)} Schema Change(s) Detected | {today}"
    )

    html_body = _build_html_email(rows, today, yesterday)
    csv_content = _rows_to_csv(rows)
    csv_filename = f"schema_changes_{today}.csv"

    log.info("Sending email to: %s", CONFIG["mailing_list"])
    send_email(
        to=CONFIG["mailing_list"],
        subject=subject,
        html_content=html_body,
        files=[],          # Airflow send_email doesn't natively support in-memory
        # attachments via send_email — attach via mime if needed (see note below)
        mime_subtype="mixed",
        mime_charset="utf-8",
        custom_headers={"X-Schema-Monitor": "true"},
    )
    log.info("Email sent successfully.")

    # If you want to attach the CSV, use the EmailOperator or extend with
    # smtplib directly. See the note in the README section at the bottom.


def _send_no_changes_email(**context) -> None:
    """
    Optional: sends a brief 'all clear' email.
    Not wired into the DAG by default — enable by uncommenting the task below.
    """
    run_date = context["ds"]
    send_email(
        to=CONFIG["mailing_list"],
        subject=f"{CONFIG['email_subject_prefix']} ✅ No Schema Changes | {run_date}",
        html_content=f"""
        <p style="font-family:Arial;font-size:14px;">
          ✅ <strong>No schema changes</strong> were detected for snapshot date
          <strong>{run_date}</strong>.<br><br>
          Source: <code>{CONFIG['project_id']}.{CONFIG['dataset']}.{CONFIG['table']}</code>
        </p>""",
    )


# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": CONFIG["mailing_list"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="schema_change_monitor",
    description="Detects daily BigQuery schema changes and alerts via email",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=CONFIG["schedule_interval"],
    catchup=False,
    max_active_runs=1,
    tags=["schema", "monitoring", "alerts"],
) as dag:

    # -------------------------------------------------------------------------
    # T1 — Wait for today's snapshot to arrive in BQ
    # -------------------------------------------------------------------------
    wait_for_data = PythonOperator(
        task_id="wait_for_todays_snapshot",
        python_callable=_wait_for_todays_data,
        retries=24,                          # retry up to 24 times …
        retry_delay=timedelta(minutes=5),    # … every 5 min = 2 hrs total wait
        execution_timeout=timedelta(hours=3),
    )

    # -------------------------------------------------------------------------
    # T2 — Run the diff query and push results to XCom
    # -------------------------------------------------------------------------
    run_diff = PythonOperator(
        task_id="run_diff_query",
        python_callable=_run_diff_query,
    )

    # -------------------------------------------------------------------------
    # T3 — Short-circuit: skip email if no changes
    # -------------------------------------------------------------------------
    check_changes = ShortCircuitOperator(
        task_id="check_for_changes",
        python_callable=_should_send_email,
    )

    # -------------------------------------------------------------------------
    # T4 — Build and send the HTML email
    # -------------------------------------------------------------------------
    send_alert = PythonOperator(
        task_id="send_alert_email",
        python_callable=_send_email,
    )

    # -------------------------------------------------------------------------
    # OPTIONAL T4b — Send "no changes" email (disabled by default)
    # To enable: uncomment this task and add it to the pipeline below
    # -------------------------------------------------------------------------
    # no_changes_email = PythonOperator(
    #     task_id="send_no_changes_email",
    #     python_callable=_send_no_changes_email,
    #     trigger_rule="all_skipped",   # fires only when check_for_changes skips
    # )

    # -------------------------------------------------------------------------
    # Pipeline
    # -------------------------------------------------------------------------
    wait_for_data >> run_diff >> check_changes >> send_alert
    # Uncomment below if you enable the no_changes_email task:
    # wait_for_data >> run_diff >> check_changes >> [send_alert, no_changes_email]


# =============================================================================
# README / DEPLOYMENT NOTES
# =============================================================================
"""
DEPLOYMENT CHECKLIST
─────────────────────────────────────────────────────────────────────────────
1. UPDATE CONFIG BLOCK at the top of this file:
   - project_id       → your GCP project
   - mailing_list     → list of recipient emails
   - email_from       → sender address
   - schedule_interval → adjust to fire after your daily load

2. AIRFLOW CONNECTIONS required:
   - google_cloud_default  (or update gcp_conn_id in CONFIG)
     Type: Google Cloud, with a service account that has:
       • roles/bigquery.dataViewer on ODS_ENGINE dataset
       • roles/bigquery.jobUser on the project

   - smtp_default  (for send_email to work)
     Type: Email / SMTP
     Configure in Airflow UI → Admin → Connections
     OR set in airflow.cfg:
       [smtp]
       smtp_host = smtp.gmail.com
       smtp_user = your@email.com
       smtp_password = yourpassword
       smtp_port = 587
       smtp_mail_from = your@email.com

3. GOOGLE CLOUD COMPOSER specifics:
   - Place this file in your Composer DAGs GCS bucket
   - The Composer service account already has BQ access in most setups
   - For email, Cloud Composer supports SendGrid natively:
     Set SENDGRID_API_KEY in Airflow environment variables and use
     airflow.providers.sendgrid.utils.emailer

4. CSV ATTACHMENT (optional enhancement):
   Airflow's built-in send_email does not support in-memory file attachments
   easily. To attach the CSV, replace the send_email call in _send_email()
   with the following pattern using smtplib:

   import smtplib
   from email.mime.multipart import MIMEMultipart
   from email.mime.text import MIMEText
   from email.mime.base import MIMEBase
   from email import encoders

   msg = MIMEMultipart('mixed')
   msg['Subject'] = subject
   msg['From']    = CONFIG['email_from']
   msg['To']      = ', '.join(CONFIG['mailing_list'])
   msg.attach(MIMEText(html_body, 'html'))
   part = MIMEBase('application', 'octet-stream')
   part.set_payload(csv_content.encode())
   encoders.encode_base64(part)
   part.add_header('Content-Disposition', f'attachment; filename="{csv_filename}"')
   msg.attach(part)
   with smtplib.SMTP('your-smtp-host', 587) as s:
       s.starttls()
       s.login('user', 'pass')
       s.sendmail(CONFIG['email_from'], CONFIG['mailing_list'], msg.as_string())

5. TESTING:
   - Trigger manually from Airflow UI with "Trigger DAG w/ config"
   - To force an email (even with no real changes), temporarily change
     _should_send_email to always return True
─────────────────────────────────────────────────────────────────────────────
"""
