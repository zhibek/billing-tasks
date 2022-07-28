import os
import functools

from dotenv import load_dotenv
import mysql.connector
import pandas as pd

load_dotenv()

FETCH_DATA_SQL = (
    "SELECT "
    "  DATE_FORMAT(FROM_UNIXTIME(`timesheet`.`start`), '%Y-%m-%d') AS `Date` "
    "  , `user`.`name` AS `Engineer` "
    "  , CEIL(SUM(`timesheet`.`duration`)/(60*60)) AS `Hours` "
    "FROM `ki_timeSheet` AS `timesheet` "
    "INNER JOIN `ki_projects` AS `project` ON `timesheet`.`projectID` = `project`.`projectID` "
    "INNER JOIN `ki_users` AS `user` on `user`.`userID` = `timesheet`.`userID` "
    "WHERE `timesheet`.`start` BETWEEN UNIX_TIMESTAMP('{month_start}') AND UNIX_TIMESTAMP('{month_end}') "
    "AND `project`.`name` = '{project}' "
    "GROUP BY `Engineer`, `Date` "
    "ORDER BY `Engineer`, `Date` "
)

EXCEL_PATH = "data/{}_{}.xlsx"


def db_connect():
    connection = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )
    return connection


def fetch_data(connection, project, month):
    month_start = _month_start(month)
    month_end = _month_end(month)
    sql = FETCH_DATA_SQL.format(project=project, month_start=month_start, month_end=month_end)

    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def process_data(data, month):
    # Generate initial dataframe with days in month
    df = pd.DataFrame(data={
        "Date": pd.period_range(
            start=pd.Timestamp(month),
            end=pd.Timestamp(month) + pd.offsets.MonthEnd(0),
            freq='D'
        ).strftime('%Y-%m-%d'),
    })
    df = df.set_index("Date")

    # data_list = [
    #     {
    #         "Date": <Date>,
    #         <Engineer>: <Hours>,
    #     },
    # ]
    data_list = []
    for row in data:
        item = {
            "Date": row[0],
            _transform_name(row[1]): row[2],
        }
        data_list.append(item)

    data_df = pd.DataFrame(data_list)
    if data_df.empty:
        return None

    data_df = data_df.set_index("Date")
    df = df.join(data_df)

    return df


def save_excel(df, project, month):
    if df is None:
        print("** No data to save")
        return

    df.to_excel(EXCEL_PATH.format(project, month), sheet_name=month)


def execute_project(month, project):
    print("- Fetching data from DB. month={}, project={}".format(month, project))
    connection = db_connect()
    data = fetch_data(connection, project, month)

    print("- Processing data. month={}, project={}".format(month, project))
    df = process_data(data, month)

    print("- Saving Excel file. month={}, project={}".format(month, project))
    save_excel(df, project, month)


def _last_month():
    now = pd.to_datetime('now')
    last_month = now - pd.DateOffset(months=1)
    last_month = last_month.strftime('%Y-%m')
    return last_month


def _month_start(month):
    month_start = pd.to_datetime(month)
    month_start = month_start.strftime('%Y-%m-%d')
    return month_start


def _month_end(month):
    month_start = pd.to_datetime(month)
    month_end = month_start + pd.DateOffset(months=1)
    month_end = month_end.strftime('%Y-%m-%d')
    return month_end


def _transform_name(input):
    parts = input.split(".")
    output = functools.reduce(
        (lambda a, b: " ".join([a.capitalize(), b.capitalize()])), parts
    )
    return output


def main():
    print("Starting process...")

    month = _last_month()
    print("Month: {}".format(month))

    projects = os.environ["PROJECTS"].split(",")
    print("Projects: {}".format(",".join(projects)))

    for project in projects:
        execute_project(month, project)


main()
