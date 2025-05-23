"""This Module is resonsible for transfrming data into appropiate format as per the business requirment"""

import pandas as pd
from detail_log import logger


class DataFrameCreate:
    """This class is responsible for creating a dataframe"""

    def __init__(self, movies_details_list: list):
        """This will initilize the movie_detail_list

        Args:
            movies_details_list (list): This aregument contain list of all movie details in dictionary.
        """
        self.movies_details_list = movies_details_list

    def create_dataframe(self):
        """This methode will create and return a dataframe.

        Returns:
            movies_dataframe_raw (dataframe): This dataframe contain all the movie information.
        """
        movies_dataframe_raw = pd.DataFrame(self.movies_details_list)
        # pd.set_option('display.max_rows', None)  # Show all rows
        pd.set_option("display.max_columns", None)  # Show all columns
        return movies_dataframe_raw


class DataFrameStructureClean:
    """This Class is responsible for performing DDL operations on dataframe as per the business requirment"""

    def __init__(self, movies_dataframe_raw: pd.DataFrame):
        """Initilising a raw dataframe

        Args:
            movies_dataframe_raw (dataframe): this dataframe contain all the movie information.
        """
        self.movies_dataframe_raw = movies_dataframe_raw

    def convert_cols_lower(self):
        """this methode is responsible for converting all the columns into lower case
            and replace the space with undescore value

        Returns:
            movies_dataframe_raw (DataFrame): DataFrame with all columns in lowercase.
        """

        # converting columns into lowercase
        logger.info("Transform-1 Conveting column names to lower case")
        self.movies_dataframe_raw.columns = (
            self.movies_dataframe_raw.columns.str.lower()
        )
        # creplacing space with underscore
        self.movies_dataframe_raw.columns = (
            self.movies_dataframe_raw.columns.str.replace(" ", "_")
        )
        return self.movies_dataframe_raw

    @staticmethod
    def transformation_on_row(row: pd.Series):
        """This is Static methode takes argument as row and convert each column list into delimited value
           and replace all column NaN values with business default value.

        Args:
            row (Series): Contain Each column value from DataFrame

        Returns:
            row (Series): Returns modified Series.
        """
        if isinstance(row, list):
            # Convertitng List to | delimited values
            row = "|".join(map(str, row))
        elif pd.isna(row):  # Check if the value is NaN
            row = "Data Not Available"
        return row  # Must return the transformed value

    def rowlist_to_delimited(self, cleaned_dataframe):
        """This Method iterate over each column and if there is any list within the column
        convert that list into comma delimited value"""
        logger.info("Transform-2 Conveting list within column to comma separated value")
        for col in cleaned_dataframe.columns:
            cleaned_dataframe[col] = cleaned_dataframe[col].apply(
                self.transformation_on_row
            )
        return cleaned_dataframe

    def date_ransformation_lamda(self, column_value):
        if column_value == "Data Not Available":
            column_value = "9999-12-12"
        if len(column_value.split("(")) > 1:
            column_value = column_value.split("(")[1].split(")")[0]

        return column_value

    def date_transformation(self, cleaned_dataframe, column_name, new_column):
        """This Methode perform transformation on release_date Column"""
        logger.info("Transform-3 Uniforming all date values to one format")
        cleaned_dataframe[new_column] = cleaned_dataframe[column_name].apply(
            self.date_ransformation_lamda
        )
        return cleaned_dataframe

    def running_time_ransformation_lamda(self, column_value):
        if column_value == "Data Not Available":
            column_value = 120
        elif len(column_value.split(" ")) == 2:
            column_value = column_value.split(" ")[0]
        elif len(column_value.split(" ")) == 4:
            column_value = (int(column_value.split(" ")[0]) * 60) + int(
                column_value.split(" ")[2]
            )

        return column_value

    def running_time_conversion(self, cleaned_dataframe, column_name, new_column):
        logger.info("Transform-4 Conveting running time column from hours to minutes")
        cleaned_dataframe[new_column] = cleaned_dataframe[column_name].apply(
            self.running_time_ransformation_lamda
        )
        return cleaned_dataframe

    def drop_duplicate(self, time_transform_df):
        initial_count = len(time_transform_df)
        logger.info("Transform-5 Dropping duplicate rows from dataframe")
        dataframe_unique = time_transform_df.drop_duplicates(keep="last")
        logger.warning(
            f"{initial_count - len(dataframe_unique)} duplicate records found"
        )
        return dataframe_unique


class Transformer:
    """This class is responsible for organising classes with transformer module"""

    def __init__(self, movies_details_list):
        self.movies_details_list = movies_details_list

    def cleaned_dataframe(self):
        dataframe_create = DataFrameCreate(self.movies_details_list)
        movies_dataframe_cleaned = dataframe_create.create_dataframe()
        # Initial status of Dataframe
        logger.info(f"Number of rows in DataFrame - {len(movies_dataframe_cleaned)}")
        logger.info(
            f"Columns present in DataFrame - {list(movies_dataframe_cleaned.columns)}"
        )
        dataframe_clean = DataFrameStructureClean(movies_dataframe_cleaned)
        cleaned_dataframe = dataframe_clean.convert_cols_lower()
        filter_condition = cleaned_dataframe["movie_name"] == "Holi"
        print(cleaned_dataframe.loc[filter_condition])
        list_to_delimited = dataframe_clean.rowlist_to_delimited(cleaned_dataframe)
        filter_condition = list_to_delimited["movie_name"] == "Holi"
        print(list_to_delimited.loc[filter_condition])
        date_transform_df = dataframe_clean.date_transformation(
            list_to_delimited, "release_date", "new_release_date"
        )
        time_transform_df = dataframe_clean.running_time_conversion(
            date_transform_df, "running_time", "running_time_in_minutes"
        )
        uniq_record_dataframe = dataframe_clean.drop_duplicate(time_transform_df)

        return uniq_record_dataframe
