from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from pandas.errors import ParserError
from pandastable import Table, TableModel
from dbmanager import DBManager

import matplotlib
import matplotlib.pyplot as plt
import math
import MySQLdb
import numpy as np
import os.path
import pandas as pd
import tkinter as tk
import tkinter.font as tkfont
import tkinter.messagebox as tkMessageBox
import tkinter.filedialog as tkFileDialog
import tkinter.simpledialog as tkSimpleDialog


matplotlib.use("TkAgg")


class Application (tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        DBManager.store_data("products_data", DBManager.get_dbdata())

        self.fonts = {
            "title": tkfont.Font(family="Lucida Grande", size=24)
        }

        self.tabs = [
            DataFrame,
            StatsFrame
        ]
        self.create_widgets()
        self.visible_idx = -1
        self.set_tab(0)

    def create_widgets(self):
        self.rowconfigure(index=2, weight=1)
        self.columnconfigure(index=0, weight=1)

        # Create title Widget
        self.title_label = tk.Label(
            self, text="DataVault Inc.", font=self.fonts["title"], justify="left", bg="#f0f0f0")
        self.title_label.grid(row=0, column=0, ipady=8,
                              ipadx=12, sticky="NSEW")

        # Create view selection widgets, i.e. tab buttons
        if len(self.tabs) > 1:
            self.subheader_frame = tk.Frame(self, bg="#f0f0f0")
            self.subheader_frame.grid(
                row=1, column=0, ipady="8", sticky="NSEW")

            # Set subheader_frame to have 8 columns.
            for col in range(12):
                self.subheader_frame.columnconfigure(index=col, weight=1)

        # Create container for actual tabs.
        self.tab_container = tk.Frame(self, bg="#fff")
        self.tab_container.grid(row=2, column=0, sticky="NSEW")
        # Set tab_container 0 index row & column to weight of 1
        self.tab_container.rowconfigure(index=0, weight=1)
        self.tab_container.columnconfigure(index=0, weight=1)

        self.tab_buttons = []
        for idx, tab in enumerate(self.tabs):
            if len(self.tabs) > 1:
                # Create tab buttons
                t = tk.Button(self.subheader_frame, text=tab.label, relief="ridge",
                              command=lambda index=idx: self.set_tab(index))

                self.tab_buttons.append(t)
                self.tab_buttons[idx].grid(ipadx=10, ipady=5, sticky="NSEW",
                                           row=0, column=(6 - math.floor(len(self.tabs) / 2) + idx),
                                           columnspan=(len(self.tabs) % 2 + 1))

            # Create tab frames
            self.tabs[idx] = tab(master=self.tab_container)
            self.tabs[idx].grid(row=0, column=0, sticky="NSEW")

    def set_tab(self, frame_idx):
        if self.tabs[frame_idx].show():
            self.visible_idx = frame_idx
            for idx, _ in enumerate(self.tab_buttons):
                if idx == frame_idx:
                    self.tab_buttons[idx]["state"] = "disabled"
                else:
                    self.tab_buttons[idx]["state"] = "normal"
                    self.tabs[idx].hide()


class DataFrame(tk.Frame):
    label = "View Data"

    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)
        self.columnconfigure(index=0, weight=1)
        self.rowconfigure(index=1, weight=1)

        self.create_widgets()

    def show(self):
        self.tkraise()
        return True

    def hide(self):
        DBManager.store_data("products_data", self.data_table.model.df)

    def create_widgets(self):
        # Create buttons to manage the DB.
        self.toolbar = tk.Frame(self)
        self.toolbar.grid(row=0, column=0, padx=12, pady=3, sticky="NSEW")
        for col in range(12):
            self.toolbar.columnconfigure(index=col, weight=1)

        self.addrow_button = tk.Button(
            self.toolbar, text="Add Row to End", command=self.add_row_to_table)
        self.save_button = tk.Button(
            self.toolbar, text="Save Data To DB", command=self.save_to_db)
        self.export_button = tk.Button(
            self.toolbar, text="Export Data to File", command=self.export_data)
        self.import_button = tk.Button(
            self.toolbar, text="Import Data from CSV", command=self.import_csv)
        self.refresh_button = tk.Button(
            self.toolbar, text="Refresh Data from DB", command=self.refresh_table_data)

        self.save_button.grid(row=0, column=12)
        self.export_button.grid(row=0, column=11)
        self.import_button.grid(row=0, column=10)
        self.refresh_button.grid(row=0, column=9)
        self.addrow_button.grid(row=0, column=8)

        self.table_container = tk.Frame(self)
        self.table_container.grid(row=1, column=0, sticky="NSEW")
        # Create table to display data
        data_df = DBManager.retrieve_data("products_data")

        self.data_table = Table(self.table_container, TableModel(data_df))
        # self.data_table.autoResizeColumns()
        self.data_table.show()

    def add_row_to_table(self):
        num_rows = self.data_table.rows
        self.data_table.setSelectedRow(num_rows)
        self.data_table.addRow()

    def refresh_table_data(self, suppress_warning=False):
        if not suppress_warning:
            res = tkMessageBox.askyesno(title="Are you sure you want to refresh the DB.",
                                        message="Are you sure that you want to refresh the DB.\n"
                                        "This will undo any changes that you made before saving your data. This includes CSV file that you have imported")

            if res == tkMessageBox.NO:
                return

        data_df = DBManager.get_dbdata()

        DBManager.store_data("products_data", data_df)
        self.data_table.updateModel(TableModel(data_df))
        self.data_table.redraw()

    def export_data(self):
        self.data_table.doExport()

    def save_to_db(self):
        products_df = self.data_table.model.df
        update_categories(products_df)
        if DBManager.isdataset("categories_data"):
            DBManager.add_df_to_db(DBManager.retrieve_data(
                "categories_data"), table="categories", suppress="success")

        categories_df = DBManager.get_dbdata("categories")
        DBManager.store_data("categories_data", categories_df)

        if not "id_category" in products_df:
            products_df["id_category"] = ""
        for idx, row in products_df.iterrows():
            if pd.isna(row.loc["id_category"]) and (row["category"]):
                mask = categories_df["title"].values == row["category"]

                if not categories_df[mask].empty:
                    value = categories_df[mask].iloc[0]["id_category"]
                    products_df.at[idx, "id_category"] = value
        if "category" in products_df:
            products_df.drop(columns=["category"], inplace=True)

        DBManager.add_df_to_db(products_df)
        self.refresh_table_data(suppress_warning=True)

    def import_csv(self, file=""):
        # Get file to import
        if file:
            input_file = file
        else:
            input_file = tkFileDialog.askopenfilename()
            if not input_file.strip():
                tkMessageBox.showerror(title="Import Failed",
                                       message="Import failed as no file was selected.")
                return

        try:
            import_df = pd.read_csv(input_file)
        except ParserError:
            tkMessageBox.showerror(
                message="The supplied file is not a valid CSV file, could not import.")

        if len(import_df) > 0:
            # Data was loaded.
            table_df = DBManager.retrieve_data("products_data")
            table_df = table_df.append(import_df, ignore_index=False)

            DBManager.store_data("products_data", table_df)
            self.data_table.updateModel(TableModel(table_df))
            self.data_table.columnwidths["id_product"] = 5
            self.data_table.redraw()

            tkMessageBox.showinfo(title="Import Successful",
                                  message="Import Completed Successfully!")
        else:
            tkMessageBox.showinfo(title="Import Failed",
                                  message="Input file did not have any CSV data so no data was added.")


class StatsFrame(tk.Frame):
    label = "View Stats"

    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)

        self.rowconfigure(index=0, weight=1)
        self.columnconfigure(index=0, weight=1)

        f = self.get_plot_data()
        if f:
            self.plt_show(f)

    def show(self):
        f = self.get_plot_data()
        if f:
            self.plt_show(f)
            self.tkraise()
            return True

        tkMessageBox.showinfo(title="No Data",
                              message="There are no statistics because there is no data loaded. Load data to view statistics tab.")
        return False

    def hide(self):
        pass

    def plt_show(self, f):
        """Method to add the matplotlib graph onto a tkinter window."""
        canvas = self.plt_redraw(f)

        self.plot_widget = canvas.get_tk_widget()
        self.plot_widget.grid(row=0, column=0, sticky="NSEW")

    def plt_redraw(self, f):
        canvas = FigureCanvasTkAgg(f, self)
        canvas.draw()

        for ax in f.get_axes():
            for tick in ax.get_xticklabels():
                tick.set_rotation(35)
        return canvas

    def get_plot_data(self):
        # Get a data from datastore and import into pandas.DataFrame
        # products_df = DBManager.get_dbdata()
        # DBManager.store_data("products_data", products_df)
        products_df = DBManager.retrieve_data("products_data")

        if len(products_df) == 0:
            return None

        # Create the matplotlib figure and axes that will be used to display the graphs for the statistics.
        fig = Figure(figsize=(15, 5), dpi=100)

        ax1 = fig.add_subplot(1, 3, 1)
        ax2 = fig.add_subplot(1, 3, 2)
        ax3 = fig.add_subplot(1, 3, 3)

        fig.subplots_adjust(bottom=.25)

        # Create different statistics and plot them the figure previously defined.
        products_df.groupby(["id_category"]).size().plot(ax=ax1, y="stock_available", kind="bar", grid=True,
                                                         title="Number of Items per Category")
        products_df.groupby(["id_category"]).sum().plot(ax=ax2, y="stock_available", kind="bar", grid=True,
                                                        title="Total Number of Products per Category")
        products_df.groupby(["id_category"]).mean().plot(ax=ax3, y="stock_available", kind="bar", grid=True,
                                                         title="Average Price of Products in Category")

        return fig


def update_categories(df):
    if "category" not in df:
        return

    if DBManager.isdataset("categories_data"):
        catdf = DBManager.retrieve_data("categories_data")
    else:
        catdf = DBManager.get_dbdata("categories")

    unknown_cats = df[~df["category"].isin(
        catdf["title"])]["category"].to_frame().drop_duplicates()

    unknown_cats.rename(columns={"category": "title"}, inplace=True)

    if not unknown_cats.empty:
        unknown_cats["id_category"] = np.nan

        catdf = catdf.append(unknown_cats)
        catdf["id_category"]
        DBManager.store_data("categories_data", catdf)


def merge_dfs(df1, df2):
    out_df = pd.merge(df1, df2, how="outer")
    return out_df


if __name__ == "__main__":
    data = {
        "db_data": pd.DataFrame()
    }

    DBManager.init(data=data, passwd="2ZombiesEatBrains?",
                   db="practice", table="products")
    app = Application()
    app.mainloop()
    # import test
