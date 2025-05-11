from datetime import date
from typing import Optional
import pandas as pd


def get_run_month(base_date: Optional[date] = None) -> pd.Timestamp:
    """
    Determine the run month for the ETL job.

    If no base_date is provided:
        - Returns the last day of the previous month from today.
    If base_date is provided:
        - Returns the last day of the month that contains base_date.

    Args:
        base_date (Optional[date]): Optional override date to determine the run month.

    Returns:
        pd.Timestamp: The last calendar day of the run month.
    """
    if base_date is None:
        today = pd.Timestamp(date.today())
        return (today.replace(day=1) - pd.DateOffset(days=1)).normalize()
    base_ts = pd.Timestamp(base_date)
    return (base_ts + pd.offsets.MonthEnd(0)).normalize()


def resolve_etl_dates(
    run_month: pd.Timestamp,
    source_client=None,
    source_query: Optional[str] = None,
) -> dict:
    """
    Resolve all derived date variables for the ETL process.

    Includes:
        - current_date: Today's date
        - run_month: Last day of the run month
        - month_id: 'YYYYMM' string of the run month
        - calendar_start: First day of run month
        - calendar_end: Last day of run month
        - calendar_start_Xm / calendar_end_Xm: 13 months of rolling history
        - source_start / source_end: Optional start/end dates from control table via BigQuery

    Args:
        run_month (pd.Timestamp): The resolved run month (last day of a calendar month).
        source_client (Optional[bigquery.Client]): BigQuery client for querying control table.
        source_query (Optional[str]): SQL query to return source_start and source_end.

    Returns:
        dict[str, str]: Dictionary of derived date variables formatted as 'YYYY-MM-DD' or 'YYYYMM'.
    """
    date_vars = {
        "current_date": str(date.today()),
        "run_month": str(run_month.date()),
        "month_id": run_month.strftime("%Y%m"),
        "calendar_start": str(run_month.replace(day=1).date()),
        "calendar_end": str(run_month.date()),
    }

    for i in range(1, 14):  # Generate 1m to 13m lookbacks
        prior_month = run_month - pd.DateOffset(months=i)
        start_key = f"calendar_start_{i}m"
        end_key = f"calendar_end_{i}m"
        date_vars[start_key] = str(prior_month.replace(day=1).date())
        date_vars[end_key] = str((prior_month + pd.offsets.MonthEnd(0)).date())

    if source_client and source_query:
        row = next(source_client.query(source_query).result())
        date_vars["source_start"] = str(row["source_start"])
        date_vars["source_end"] = str(row["source_end"])

    return date_vars
