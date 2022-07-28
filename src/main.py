import os
import functools

from dotenv import load_dotenv
import mysql.connector
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

load_dotenv()

DRIVE_ENABLED = False if ("DRIVE_ENABLED" in os.environ and os.environ["DRIVE_ENABLED"] == "false") else True

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

FILE_NAME = "{}_{}.xlsx"


def db_connect():
    db = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )
    return db


def drive_connect():
    gauth = GoogleAuth(settings={
        "service_config": {
            "client_json_dict": {
                "type": "service_account",
                "client_email": os.environ["GOOGLE_AUTH_CLIENT_EMAIL"],
                "client_id": os.environ["GOOGLE_AUTH_CLIENT_ID"],
                "private_key_id": os.environ["GOOGLE_AUTH_PRIVATE_KEY_ID"],
                "private_key": os.environ["GOOGLE_AUTH_PRIVATE_KEY"],
            },
            "client_user_email": os.environ["GOOGLE_AUTH_CLIENT_EMAIL"],
        },
    })
    gauth.ServiceAuth()

    drive = GoogleDrive(gauth)
    return drive


def fetch_data(db, project, month):
    month_start = _month_start(month)
    month_end = _month_end(month)
    sql = FETCH_DATA_SQL.format(project=project, month_start=month_start, month_end=month_end)

    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def process_data(data, month):
    # Generate initial dataframe with days in month
    df = pd.DataFrame(data={
        "Date": pd.period_range(
            start=pd.Timestamp(month),
            end=pd.Timestamp(month) + pd.offsets.MonthEnd(0),
            freq="D"
        ).strftime("%Y-%m-%d"),
    })
    df = df.set_index("Date")

    # data_list = [
    #     {
    #         "Date": <Date>,
    #         <Engineer>: <Hours>,
    #     },
    # ]
    data_dict = {}
    for row in data:
        date = row[0]
        engineer = _transform_name(row[1])
        hours = pd.to_numeric(row[2])

        if date not in data_dict:
            data_dict[date] = {"Date": date}

        data_dict[date][engineer] = hours

    data_df = pd.DataFrame(data_dict.values())

    if data_df.empty:
        return None

    data_df = data_df.set_index("Date")
    df = df.join(data_df)

    return df


def save_excel(df, project, month):
    file_name = _file_name(project, month, data_dir=True)
    sheet_name = month

    (max_row, max_col) = df.shape

    writer = pd.ExcelWriter(file_name)

    df.to_excel(writer, sheet_name=sheet_name)

    worksheet = writer.sheets[sheet_name]

    # Set column width to 20
    worksheet.set_column(0, max_col, 20)

    writer.save()


def file_exists_in_drive(drive, path, project, month):
    file_name = _file_name(project, month)

    files = drive.ListFile({"q": "'{}' in parents".format(path)}).GetList()

    for file in files:
        if file["title"] == file_name:
            return True

    return False


def copy_to_drive(drive, path, project, month):
    target_file_name = _file_name(project, month)
    source_file_name = _file_name(project, month, data_dir=True)

    gfile = drive.CreateFile({"parents": [{"id": path}], "title": target_file_name})

    gfile.SetContentFile(source_file_name)
    gfile.Upload()


def execute_project(db, drive, month, project):
    if DRIVE_ENABLED:
        print("- Checking if file already exists in Google Drive. month={}, project={}".format(month, project))
        file_exists = file_exists_in_drive(drive, os.environ["GOOGLE_DRIVE_PATH"], project, month)
        if file_exists:
            print("** File already exists in Google Drive")
            return False

    print("- Fetching data from DB. month={}, project={}".format(month, project))
    data = fetch_data(db, project, month)

    print("- Processing data. month={}, project={}".format(month, project))
    df = process_data(data, month)
    if df is None:
        print("** No data to save")
        return False

    print("- Saving Excel file. month={}, project={}".format(month, project))
    save_excel(df, project, month)

    if DRIVE_ENABLED:
        print("- Copying to Google Drive. month={}, project={}".format(month, project))
        copy_to_drive(drive, os.environ["GOOGLE_DRIVE_PATH"], project, month)

    return True


def _last_month():
    now = pd.to_datetime("now")
    last_month = now - pd.DateOffset(months=1)
    last_month = last_month.strftime("%Y-%m")
    return last_month


def _month_start(month):
    month_start = pd.to_datetime(month)
    month_start = month_start.strftime("%Y-%m-%d")
    return month_start


def _month_end(month):
    month_start = pd.to_datetime(month)
    month_end = month_start + pd.DateOffset(months=1)
    month_end = month_end.strftime("%Y-%m-%d")
    return month_end


def _file_name(project, month, data_dir=False):
    file_name = FILE_NAME.format(project, month)

    if data_dir:
        file_name = "data/" + file_name

    return file_name


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

    print("Connecting to DB...")
    db = db_connect()

    print("Connecting to Google Drive...")
    drive = drive_connect()

    for project in projects:
        execute_project(db, drive, month, project)


main()
