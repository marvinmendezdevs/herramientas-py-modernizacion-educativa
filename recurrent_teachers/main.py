from pathlib import Path
from rapidfuzz import process

import pandas as pd
data_dir = Path("data")
output_dir = Path("output")

def create_csv_with_emails():

    lxp_teacher_list = pd.read_csv(next(data_dir.glob("teacher*.csv")))
    recurrent_list = pd.read_excel(next(data_dir.glob("recurrent*.xlsx")))
    report_list = pd.read_csv(next(data_dir.glob("Reporte*.csv")))

    recurrent_list["Email"] = recurrent_list.apply(lambda row: search_email_by_name(row["NOMBRE"], lxp_teacher_list), axis=1)
    mask = recurrent_list["Email"].isna()

    recurrent_list.loc[mask, "Email"] = recurrent_list[mask].apply(
        lambda row: search_email_by_name(row["NOMBRE"], report_list), axis=1
    )

    recurrent_list["Email"] = recurrent_list["Email"].fillna("No encontrado")

    recurrent_list.to_csv(output_dir / "cleaned_data.csv", index=False)

def search_email_by_name(excel_name, lxp_df):
    match = process.extractOne(
        excel_name,
        lxp_df["Name"].to_list(),
        score_cutoff=90
    )

    if match:
        return lxp_df.iloc[match[2]]["Email"]

def search_name_match(excel_name, lxp_df):
    match = process.extractOne(
        excel_name,
        lxp_df["Name"].to_list(),
        score_cutoff=80
    )
    if match:
        return f"{match[0]} ({match[1]}%)"
    return "NO ENCONTRADO"

def get_email_list():
     cleaned_data = pd.read_csv(next(output_dir.glob("cleaned*.csv")))
     emails = cleaned_data["Email"].tolist()
     return (",\n".join([f"'{e}'" for e in emails]))
def get_attendance_percentage():
    section_cols = ["Email", "Code", "Type", "Grade", "Track", "Subtrack", "Class", "Subject"]
    historical_data = pd.read_csv(next(data_dir.glob("historical*.csv")))

    total_days = historical_data.groupby(section_cols)["Date"].nunique().reset_index(name="Total_days")
    connected_days = historical_data[historical_data["Access_status"] == 1].groupby(section_cols)["Date"].nunique().reset_index(name="Connected_days")

    section_report = pd.merge(total_days, connected_days, on=section_cols, how="left")
    section_report["Connected_days"] = section_report["Connected_days"].fillna(0).astype(int)
    section_report["Absent_days"] = section_report["Total_days"] - section_report["Connected_days"]
    section_report["Attendance_%"] = (section_report["Connected_days"] / section_report["Total_days"] * 100).round(2)

    section_report.to_csv(output_dir / "Final.csv", index=False)

def get_class_completation_correct_rate():
    final = pd.read_csv(next(output_dir.glob("Final*.csv")))
    consume = pd.read_csv(next(output_dir.glob("consume*.csv")))

    cols_consume = ["Code", "Grade", "Class", "Track", "Subtrack", "Type", "Subject", "ClassCompletionRate", "ClassAverageCorrectRate"]

    final["Code"] = final["Code"].astype(str)
    final["Grade"] = final["Grade"].astype(float).astype(int).astype(str)
    consume["Code"] = consume["Code"].astype(str)
    consume["Grade"] = consume["Grade"].astype(float).astype(int).astype(str)

    consume["ClassCompletionRate"] = consume["ClassCompletionRate"].replace("", None)
    consume["ClassAverageCorrectRate"] = consume["ClassAverageCorrectRate"].replace("", None)

    consume["Track"] = consume["Track"].fillna("none")
    consume["Subtrack"] = consume["Subtrack"].fillna("none")

    consume["ClassCompletionRate"] = pd.to_numeric(consume["ClassCompletionRate"], errors="coerce")
    consume["ClassAverageCorrectRate"] = pd.to_numeric(consume["ClassAverageCorrectRate"], errors="coerce")

    consume_agg = consume[cols_consume].groupby(
    ["Code", "Grade", "Class", "Track", "Subtrack", "Type", "Subject"]
    ).agg(
        ClassCompletionRate    = ("ClassCompletionRate", "mean"),
        ClassAverageCorrectRate = ("ClassAverageCorrectRate", "mean")
    ).round(2).reset_index()

    df = pd.merge(final, consume_agg,
                  on=["Code","Grade","Class","Track","Subtrack","Type","Subject"],
                  how="left"
     
                )
    df.to_csv("report.csv", index=False)
    df.to_json("report.json", index=False)

def main():
    create_csv_with_emails()
    get_attendance_percentage()

    get_class_completation_correct_rate()

main()