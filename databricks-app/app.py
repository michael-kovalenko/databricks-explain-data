import os
import streamlit as st
import pandas as pd
import json
from helpers import format_list, show_aggrid_table, prepare_merge
from model_serving_utils import query_endpoint, is_endpoint_supported

# Ensure environment variable is set correctly
serving_endpoint = os.getenv('SERVING_ENDPOINT')
assert serving_endpoint, \
    ("Unable to determine serving endpoint to use for GenAI app. If developing locally, "
     "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
     "deploying to a Databricks app, include a serving endpoint resource named "
     "'serving_endpoint' with CAN_QUERY permissions, as described in "
     "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app")

# Check if the endpoint is supported
endpoint_supported = is_endpoint_supported(serving_endpoint)

# Initialize Streamlit
st.set_page_config(page_title="Explain Data Lineage", layout="wide")

# create a 60% – 40% column split
outer_left, outer_main, outer_right = st.columns([5, 90, 5])

if "lineage_json" not in st.session_state:
    st.session_state.lineage_json = None

with outer_main:
    st.title("Explain Data Lineage")

    st.session_state.lineage_json = {
        "type": "merge by attribute",
        "logic": "Priority rule is applied (per attribute) in following way: among data sources that have non-empty (not null) value for the attribute select one with the lowest priority. If a specific attribute contains only null values across all input data sources, then the result will also be null.",
        "config": {
            "LatitudeNAD27": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ],
            "LongitudeNAD27": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ],
            "LatitudeWGS84": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ],
            "LongitudeWGS84": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ],
            "utmX": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ],
            "utmY": [
                "Calc",
                "Vendor A",
                "Vendor B",
                "Vendor C",
                "Vendor D",
            ]
        },
        "sourceData": {
            "LatitudeNAD27": [
                {
                    "Calc": "47.878760493073"
                },
                {
                    "Vendor A": "-104.661737793398"
                },
                {
                    "Vendor B": "47.8787565021967"
                },
                {
                    "Vendor C": "47.878753563264"
                },
                {
                    "Vendor D": "47.878947311500"
                }
            ],
            "LongitudeNAD27": [
                {
                    "Calc": "-104.661739639218"
                },
                {
                    "Vendor A": "47.878756501258"
                },
                {
                    "Vendor B": "-104.66173779339842"
                },
                {
                    "Vendor C": "-104.661739843785"
                },
                {
                    "Vendor D": "-104.661702298723"
                }
            ],
            "LatitudeWGS84": [
                {
                    "Calc": "47.878699080508"
                },
                {
                    "Vendor A": "47.878695089014"
                },
                {
                    "Vendor B": "47.8786950899522"
                },
                {
                    "Vendor C": "47.878692151000"
                },
                {
                    "Vendor D": "47.878885900000"
                }
            ],
            "LongitudeWGS84": [
                {
                    "Calc": "-104.662381142855"
                },
                {
                    "Vendor A": "-104.662379293577"
                },
                {
                    "Vendor B": "-104.662379293577"
                },
                {
                    "Vendor C": "-104.662381344000"
                },
                {
                    "Vendor D": "-104.662343800000"
                }
            ],
            "utmX": [
                {
                    "Calc": "NULL"
                },
                {
                    "Vendor A": "1723397.753704580700"
                },
                {
                    "Vendor B": "1724615.326021660800"
                },
                {
                    "Vendor C": "1723397.255404133300"
                },
                {
                    "Vendor D": "1723406.156385899300"
                }
                ],
                "utmY": [
                {
                    "Calc": "NULL"
                },
                {
                    "Vendor A": "17397147.569198217000"
                },
                {
                    "Vendor B": "17393750.653766643000"
                },
                {
                    "Vendor C": "17397146.495680280000"
                },
                {
                    "Vendor D": "17397217.184715137000"
                }
            ]
        },
        "result": {
            "LatitudeNAD27": "Calc",
            "LongitudeNAD27": "Calc",
            "LatitudeWGS84": "Calc",
            "LongitudeWGS84": "Calc",
            "utmX": "Vendor A",
            "utmY": "Vendor A"
        }
    }

    if st.session_state.lineage_json:
        lineage = st.session_state.lineage_json

        decision_type = lineage["type"]

        st.markdown("#### Decision")
        st.markdown(f"""**Decision type:** {format_list([decision_type])}""", unsafe_allow_html=True)
        st.markdown(f"**Decision logic:**")
        st.write(lineage["logic"])

        source_data = lineage["sourceData"]
        result = lineage["result"]

        if isinstance(source_data, dict) and source_data and isinstance(result, dict) and result:
            config = lineage["config"]

            if isinstance(config, dict) and config:
                config = [config]
            else:
                config = None

            if isinstance(config, list) and config:
                merge_df = prepare_merge(source_data, result, "Attribute name", config)

                st.markdown("**Merge config, source data and result:**")
                # show_aggrid_table(merge_df)
                st.dataframe(merge_df, 
                                hide_index=True,
                                use_container_width=True)
            else:
                merge_df = prepare_merge(source_data, result, "Data source")

                st.markdown("**Source data and result:**")
                # show_aggrid_table(merge_df)
                st.dataframe(merge_df, 
                                hide_index=True,
                                use_container_width=True)
        else:
            if not isinstance(source_data, dict) or not source_data:
                st.warning("No source data available")
            
            if not isinstance(result, dict) or not result:
                st.warning("No merge result available")

    if st.button("Evaluate the source data for inconsistency"):
        content = f"""{lineage["sourceData"]}
        ----------------
        please find and show me all potential inconsistencies. Do not show the source data again, only conclusions
        """

        messages = [
            {
                'role': 'user',
                'content': content
            }
        ]

        # Define max tokens for this very case, assuming the source data size
        max_tokens = 2048

        response = query_endpoint(serving_endpoint, messages, max_tokens)["content"]

        with st.expander(label="See evaluation results", expanded=True):
            st.write(response)

    if st.button("Explain the result"):
        content = f"""{lineage}
        ----------------
        please explain the decision. Do not show the source data again, only the explanation
        """

        messages = [
            {
                'role': 'user',
                'content': content
            }
        ]

        # Define max tokens for this very case, assuming the source data size
        max_tokens = 4096

        response = query_endpoint(serving_endpoint, messages, max_tokens)["content"]

        with st.expander(label="See the explanation", expanded=True):
            st.write(response)


        
