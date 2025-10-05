import pandas as pd
import json
import ast
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# Formatting functions
# Format list to show as pills style
def format_list(ls, color=None):
    if len(ls) > 0:
        if color == "green":
            background_color = "#90ee90"
        elif color == "orange":
            background_color = "#ffa500"
        else:
            background_color = "#f0f2f6"

        pills_html = "".join([
            f"""<span style="
                background-color:{background_color};
                border-radius:10px;
                padding:2px 6px;
                margin:3px;
                font-size:0.9em;
                color:#31333f99;
                display:inline-block;
            ">{t}</span>"""
            for t in ls])
    else:
        pills_html = "<span/>"

    return pills_html

# Show AgGrid table
def show_aggrid_table(df):
    # Handle case of mixed data types
    if "Value" in df.columns:
        df["Value"] = df["Value"].astype(str)

    # Build AgGrid options
    gb = GridOptionsBuilder.from_dataframe(df)

    grid_opts = gb.build()

    # Adjust height
    default_header_height = 35
    default_row_height = 29
    grid_height = df.shape[0] * default_row_height + default_header_height

    # Assign a unique key for each grid on the page
    # When a widget is created, it's assigned an internal key based on its structure. 
    # Multiple widgets with an identical structure will result in the same internal key, which causes "DuplicateWidgetID" error.
    unique_key = str(uuid4())

    # Render the grid
    grid_resp = AgGrid(
        df,
        gridOptions=grid_opts,
        theme="Balham",
        height=grid_height,
        key=unique_key
    )

# Prepare data frame functions

# Special function to handle complex structures
def handle_dict(x):
    return ", ".join(f"{k}: {v}" for k, v in x.items())

def handle_complex_data(x):
    if isinstance(x, list):
        return [handle_dict(i) if isinstance(i, dict) else str(i) for i in x]
    else:
        return [str(x)]

# Merge together config, source data and result
def prepare_merge(source_data, result, common_column, config = None):
    source_df = pd.DataFrame(list(source_data.items()), columns=[common_column, "Source data"])
    result_df = pd.DataFrame(list(result.items()), columns=[common_column, "Result"])

    # Handle case of complex data structures
    source_df["Source data"] = source_df["Source data"].apply(lambda x: handle_complex_data(x))
    result_df["Result"] = result_df["Result"].apply(lambda x: handle_complex_data(x))

    # lambda x: json.loads(x) if pd.notna(x) else x

    if config:
        config_df = pd.concat([pd.DataFrame(list(cfg.items()), columns=[common_column, "Merge configuration"]) for cfg in config], ignore_index=True)

        # Handle case of complex data structures
        config_df["Merge configuration"] = config_df["Merge configuration"].apply(lambda x: handle_complex_data(x))

        # merge all 3 DataFrames
        merge_df = config_df.join(source_df.set_index(common_column), on=common_column, how="outer").join(result_df.set_index(common_column), on=common_column, how="outer")
    else:
        merge_df = source_df.join(result_df.set_index(common_column), on=common_column, how="outer")

    return merge_df
