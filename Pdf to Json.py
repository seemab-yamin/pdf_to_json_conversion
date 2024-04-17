import json
import os
import sys
import time

import camelot
import pandas as pd

# base structure of table
BASE_COLUMNS = [
    "s_no",
    "booking_number",
    "accept_date",
    "ride_date",
    "driver",
    "license_plate",
    "pickup_address",
    "destination_duration",
    "bonus",
    "net_amount",
    "net_amount_waiting_time",
    "net_amount_additional_km",
    "b1",
    "total_net_amount",
    "b2",
    "VAT_sales_tax",
    "gross_total_amount",
]

# specify columns whose data is split into different lines
UPDATE_COLUMNS = [
    "accept_date",
    "ride_date",
    "driver",
    "license_plate",
    "pickup_address",
    "destination_duration",
]

# specify price columns for cleaning
PRICE_COLUMNS = [
    "net_amount",
    "net_amount_waiting_time",
    "net_amount_additional_km",
    "total_net_amount",
    "VAT_sales_tax",
    "gross_total_amount",
]


def extract_data(table):
    """
    The `extract_data` function processes a table extracted using Camelot library by manipulating the
    data and applying specific operations to extract relevant information.

    :param table: The `table` parameter in the `extract_data` function is expected to be a DataFrame
    extracted using the Camelot library. The function processes this table by performing various
    operations such as data extraction, manipulation, and transformation to extract relevant information
    from the table
    :return: The function `extract_data` returns a processed DataFrame after performing various
    operations on the input table. The operations include adjusting columns, filling missing values,
    grouping by booking number, concatenating columns within each group, cleaning up data in the
    "license_plate" column, and dropping unnecessary columns. The final processed DataFrame is then
    returned.
    """
    # slice table data frame to fetch only require data points
    df = table.df.iloc[6:-1].copy()
    # set base columns
    df.columns = BASE_COLUMNS
    # reset index values
    df = df.reset_index(drop=True)

    # apply booking number to missing row
    for i, df_rec in df.iterrows():
        booking_number = df_rec["booking_number"]
        for j, sdf in df.iloc[i + 1 :].iterrows():
            # break if booking number is 9 as this is standard
            # else set booking number to respective record
            if len(sdf.loc["booking_number"]) == 9:
                break
            df.loc[j, "booking_number"] = booking_number

    # then apply group by and then concat the whole group df column
    # iterate over group by booking_number df
    for booking_number, group_df in df.groupby("booking_number"):
        for col in UPDATE_COLUMNS:
            # combine records from following record data points
            df.loc[group_df.index[0], col] = " ".join(df.iloc[group_df.index][col])
    # only choose desired records by skipping empty s_no
    df = df[df["s_no"].str.len() > 0].copy()
    # remove spaces from license_plate column
    df["license_plate"] = df["license_plate"].apply(lambda x: x.replace(" ", ""))
    # drop unnecessary columns
    df.drop(columns=["s_no", "b1", "b2"], axis=1, inplace=True)
    return df


if __name__ == "__main__":
    # set start time
    start_time = time.time()

    # receive file path from command argument
    file_path = sys.argv[1]

    # validate if input file has valid pdf extension
    assert file_path.endswith(".pdf"), "Invalid File. Format is Not PDF."
    # validate if the file exists
    assert os.path.exists(file_path), "File Not Found."

    # reading the pdf file
    try:
        tables = camelot.read_pdf(filepath=file_path, pages="all", flavor="stream")
    except Exception as e:
        raise Exception(f"Camelot Parsing Failed. Looks Like invalid PDF format. {e}")
    # generate an empty output data frame
    output_df = pd.DataFrame()
    for i, table in enumerate(tables):
        print(f"Processing Table:\t{i+1}")
        # extract data from the table into a data frame
        # concat the data frame into output data frame
        output_df = pd.concat([extract_data(table), output_df])

    # remove: €
    # replace dots with commas
    for col in PRICE_COLUMNS:
        output_df[col] = output_df[col].apply(
            lambda x: x.replace("€", "").replace(".", ",").strip()
        )

    # get file name by removing file extension
    file_name = file_path.split(".")[0]

    # export data to excel format
    excel_file_path = file_name + ".xlsx"
    try:
        output_df.to_excel(excel_file_path, index=0)
        print(f"Generated Excel files:\t{excel_file_path}")
    except Exception as e:
        print(f"Excel File Generation Failed. {e}")

    # export data to json format
    try:
        json_file_path = file_name + ".json"
        with open(json_file_path, "w") as file:
            file.write(
                json.dumps(
                    {"rider": json.loads(output_df.to_json(index=0, orient="records"))}
                )
            )
        print(f"Generated JSON files:\t{json_file_path}")
    except Exception as e:
        raise Exception(f"JSON File Generation Failed. {e}")

    print(f"Program Completed.\tTime Taken:\t{time.time()-start_time:.2f} Seconds")
