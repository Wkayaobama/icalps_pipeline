Snippet

import pandas as pd
import seaborn as sns
import xlwings as xw
from xlwings import func, script
import duckdb
import matplotlib.pyplot as plt

GRAY_1 = "#CCCCCC"
GRAY_2 = "#657072"
GRAY_3 = "#4A606C"
BLUE_1 = "#1E4E5C"


def read_data(sheet):
    data = sheet.range("A1:O1027").value
    df = pd.DataFrame(data, columns=data[0])
    return df




@script
def region_data(book: xw.Book):
    helper_sheet = book.sheets[5]
    data_sheet = book.sheets[1]
    df = read_data(data_sheet)

    helper_sheet["D4"].options(transpose=True).value = df["Län"].unique()


@script
def dashboard(book: xw.Book):
    data_sheet = book.sheets[1]
    dashboard_sheet = book.sheets[0]

    selected_region = dashboard_sheet.range("I6").value

    df = read_data(data_sheet).query("Län == @selected_region")

    cp_title = CellPrinter(dashboard_sheet, 42)
    cp_kpi = CellPrinter(dashboard_sheet, 36)
    cp_text = CellPrinter(dashboard_sheet, 14)

    cp_title.print("E2", "MYH dashboard kurser 2025")

    for cell, (kpi, value) in zip("EFG", kpis(df).items()):
        cp_text.print(f"{cell}5", kpi)
        cp_kpi.print(f"{cell}6", value)


    data = DataProcessing(df)

    educational_area_bar_fig = educational_area_bar(
        data.total_applications,
        f"Antal ansökningar för kurser i omgång 2025 i {selected_region}",
    )
    educational_area_approved_bar_fig = educational_area_bar(
        data.approved_applications,
        f"Antal beviljade ansökningar för kurser i omgång 2025 i {selected_region}",
    )

    points_fig = points_histogram(
        df,
        f"Histogram över YH-poäng för beviljade och avslagna\n kurser i {selected_region}",
    )

    seats_fig = seats_histogram(
        df,
        f"Histogram över Totalt antal beviljade platser i {selected_region}",
    )

    dashboard_sheet.pictures.add(
        educational_area_bar_fig,
        update=True,
        name="Educational area",
        anchor=dashboard_sheet["C8"],
    )

    dashboard_sheet.pictures.add(
        educational_area_approved_bar_fig,
        update=True,
        name="Educational area approved",
        anchor=dashboard_sheet["I8"],
    )

    dashboard_sheet.pictures.add(
        points_fig,
        update=True,
        name="Points histogram",
        anchor=dashboard_sheet["E34"],
    )

    dashboard_sheet.pictures.add(
        seats_fig,
        update=True,
        name="Total seats histogram",
        anchor=dashboard_sheet["K34"],
    )

class DataProcessing:
    def __init__(self, df):
        self.df = df

    @property
    def total_applications(self):
        with duckdb.connect() as conn:
            conn.register("df", self.df)
            df = conn.query(
                """
                SELECT utbildningsområde, COUNT(beslut) "Totala ansökningar"
                FROM df
                GROUP BY utbildningsområde
                ORDER BY "Totala ansökningar"
                DESC
            """
            ).df()
        return df

    @property
    def approved_applications(self):
        with duckdb.connect() as conn:
            conn.register("df", self.df)
            return conn.query(
                """
                SELECT utbildningsområde, COUNT(beslut) "Totala ansökningar"
                FROM df
                WHERE beslut = 'Beviljad'
                GROUP BY utbildningsområde
                ORDER BY "Totala ansökningar"
                DESC
            """
            ).df()




each sheet is loaded into duckdb and subject to data transformation



