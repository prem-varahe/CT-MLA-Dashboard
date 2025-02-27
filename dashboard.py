import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
from babel.numbers import format_decimal
from decimal import Decimal, InvalidOperation
from streamlit_option_menu import option_menu
import os
from dotenv import load_dotenv
st.set_page_config(layout="wide",initial_sidebar_state='collapsed')

load_dotenv()

# Get the MongoDB URI
mongo_uri = os.getenv("MONGO_URL")
# Load your dataset
@st.cache_data
def load_data():
    # Replace this with your actual data loading code
    from pymongo import MongoClient
    mongo_client = MongoClient(mongo_uri)
    host_info = mongo_client['HOST']
    print ("\nhost:", host_info)
    df = pd.DataFrame(mongo_client['cati_central']['HP_MLA_feedback_raw_response'].find({'callRemark': {'$exists': True},'sync_date':{'$ne':"NaT"}}))
    df=df.head(50000)
    df['sync_date'] = pd.to_datetime(df['sync_date'])
    return df
df = load_data()
def creds_enter():
    if st.session_state['user'].strip()=='admin' and st.session_state['passwd'].strip()=='admin':
        st.session_state['authenticated']=True
    else:
        st.session_state['authenticated']=False
        if not st.session_state['passwd']:
            st.warning('Please Enter the Password')
        elif not st.session_state['user']:
            st.warning('Please Enter the Username')
        else:
            st.error('Invalid Username/Password')
def authenticate_user():
    if "authenticated" not in st.session_state:
        st.text_input(label="Username : ",value="",key='user',on_change=creds_enter)
        st.text_input(label="Password : ",value="",key='passwd',on_change=creds_enter,type="password")
        return False
    else:
        if st.session_state["authenticated"]:
            return True
        else:
            st.text_input(label="Username : ",value="",key='user',on_change=creds_enter)
            st.text_input(label="Password : ",value="",key='passwd',on_change=creds_enter)
            return False
    return False
if authenticate_user():
        st.markdown(
            """
            <style>
            /* Navbar styling */
            .stTabs [role="tablist"] {
                background-color: #213555; /* Background color of the navbar */
                padding: 15px;
                border-radius: 10px;
                position: sticky;
                top: 0;
                z-index: 9999;
            }
            /* Individual tab styling */
            .stTabs [role="tab"] {
                font-size: 25px; /* Font size */
                font-weight: bold; /* Bold text */
                color: white; /* White font color */
                margin: 0 40px; /* Spacing between tabs */
                padding: 10px 20px; /* Padding inside tabs */
                border-radius: 8px; /* Rounded edges */
            }
            /* Active tab styling */
            .stTabs [role="tab"][aria-selected="true"] {
                background-color: #3E5879; /* Highlight active tab */
                color: white; /* Active tab font color */
            }
            /* Hover effect for tabs */
            .stTabs [role="tab"]:hover {
                background-color: #D8C4B6; /* Hover background color */
                color: black; /* Hover font color */
                cursor: pointer;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            ["Calls & Agent Monitoring", 
            "AC-Wise Calls Monitoring", 
            "Performance of MLA", 
            "AC-Wise Summary Report", 
            "Graph View"]
        )
        # Title
        with tab1:
            st.markdown("<h2 style='text-align: center;'>Select Start & End Date: </h2>", unsafe_allow_html=True)
            col1, col2 = st.columns([4, 4])  # Two equal columns
            # PC Name selection in the first column
            with col1:
                start_date = st.date_input(
                "Start Date", 
                value=df['sync_date'].min(), 
                min_value=df['sync_date'].min(), 
                max_value=df['sync_date'].max()
                )
            with col2:
                end_date = st.date_input(
                "End Date", 
                value=df['sync_date'].max(), 
                min_value=df['sync_date'].min(), 
                max_value=df['sync_date'].max()
                )
            filtered_df = df[(df['sync_date'] >= pd.to_datetime(start_date)) & (df['sync_date'] <= pd.to_datetime(end_date))]

            # Filter the DataFrame based on the selected start and end dates
            total_days = (end_date - start_date).days + 1
            total_agents = filtered_df["agentId"].nunique()
            total_calls_made = filtered_df["callRemark"].notnull().sum()
            connected_calls = (filtered_df["callRemark"] == "Connected").sum()
            valid_calls = (filtered_df["final_rejection"] == True).sum()
            average_calls_per_agent = total_calls_made / total_agents if total_agents > 0 else 0
            average_valid_calls_per_agent = valid_calls / total_agents if total_agents > 0 else 0
            
            # Correct calculation for Avg. Agents per Day (average number of unique agents per day)
            daily_agents_count = filtered_df.groupby(filtered_df['sync_date'].dt.date)['agentId'].nunique()
            avg_agents_per_day = daily_agents_count.mean() if len(daily_agents_count) > 0 else 0
            
            avg_calls_per_day = total_calls_made / total_days if total_days > 0 else 0
            avg_connected_calls_per_day = connected_calls / total_days if total_days > 0 else 0
            avg_valid_calls_per_day = valid_calls / total_days if total_days > 0 else 0

            avg_connected_calls_per_agent = connected_calls / total_agents if total_agents > 0 else 0

            # Calculate Avg Calls per Day by Agent, Avg Connected Calls per Day by Agent, and Avg Valid Calls per Day by Agent
            calls_per_day_by_agent = filtered_df.groupby([filtered_df['sync_date'].dt.date, 'agentId']).size().reset_index(name='calls_per_day')
            avg_calls_per_day_by_agent = calls_per_day_by_agent.groupby('agentId')['calls_per_day'].mean().mean() if not calls_per_day_by_agent.empty else 0
            
            connected_calls_per_day_by_agent = filtered_df[filtered_df["callRemark"] == "Connected"].groupby([filtered_df['sync_date'].dt.date, 'agentId']).size().reset_index(name='connected_calls_per_day')
            avg_connected_calls_per_day_by_agent = connected_calls_per_day_by_agent.groupby('agentId')['connected_calls_per_day'].mean().mean() if not connected_calls_per_day_by_agent.empty else 0
            
            valid_calls_per_day_by_agent = filtered_df[filtered_df["final_rejection"] == True].groupby([filtered_df['sync_date'].dt.date, 'agentId']).size().reset_index(name='valid_calls_per_day')
            avg_valid_calls_per_day_by_agent = valid_calls_per_day_by_agent.groupby('agentId')['valid_calls_per_day'].mean().mean() if not valid_calls_per_day_by_agent.empty else 0

            avg_calls_made_by_agent = calls_per_day_by_agent.groupby('agentId')['calls_per_day'].mean().mean() if not calls_per_day_by_agent.empty else 0
            avg_calls_connected_by_agent = connected_calls_per_day_by_agent.groupby('agentId')['connected_calls_per_day'].mean().mean() if not connected_calls_per_day_by_agent.empty else 0
            avg_valid_samples_by_agent = valid_calls_per_day_by_agent.groupby('agentId')['valid_calls_per_day'].mean().mean() if not valid_calls_per_day_by_agent.empty else 0

            # Format numbers in Indian number format, replacing invalid numbers with 0 or rounded value
            def format_number(value):
                try:
                    return format_decimal(Decimal(str(value)), locale='en_IN')
                except InvalidOperation:
                    return "0"
            total_days = format_number(total_days)
            total_agents = format_number(total_agents)
            total_calls_made = format_number(total_calls_made)
            connected_calls = format_number(connected_calls)
            valid_calls = format_number(valid_calls)
            average_calls_per_agent = format_number(round(average_calls_per_agent))
            average_valid_calls_per_agent = format_number(round(average_valid_calls_per_agent))
            avg_agents_per_day = format_number(round(avg_agents_per_day))
            avg_calls_per_day = format_number(round(avg_calls_per_day))
            avg_connected_calls_per_day = format_number(round(avg_connected_calls_per_day))
            avg_valid_calls_per_day = format_number(round(avg_valid_calls_per_day))
            avg_connected_calls_per_agent = format_number(round(avg_connected_calls_per_agent))
            avg_calls_per_day_by_agent = format_number(round(avg_calls_per_day_by_agent))
            avg_connected_calls_per_day_by_agent = format_number(round(avg_connected_calls_per_day_by_agent))
            avg_valid_calls_per_day_by_agent = format_number(round(avg_valid_calls_per_day_by_agent))
            avg_calls_made_by_agent = format_number(round(avg_calls_made_by_agent))
            avg_calls_connected_by_agent = format_number(round(avg_calls_connected_by_agent))
            avg_valid_samples_by_agent = format_number(round(avg_valid_samples_by_agent))

            # Display metrics in styled cards with a modern color palette
            st.write("### Performance Metrics")

            # Helper function to render a card
            def render_card(label, value, icon, color):
                st.markdown(f"""
                    <div style="background-color:{color}; color:white; padding:20px; border-radius:12px; display:flex; align-items:center; margin-bottom:20px;">
                        <div style="font-size:24px; margin-right:10px;">{icon}</div>
                        <div>
                            <div style="font-size:18px; font-weight:bold;">{label}</div>
                            <div style="font-size:24px; font-weight:bold;">{value}</div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

            # Create a row of cards, with more spacing between them
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                render_card("Total Duration (Days)", total_days, "📅", "#D8C4B6")
                render_card("Unique Agents Involved", total_agents, "📊", "#9F8383")
                render_card("Average Agents per Day", avg_agents_per_day, "👥", "#D8C4B6")

            with col2:
                render_card("Total Calls Made", total_calls_made, "📞", "#9F8383")
                render_card("Average Calls per Day", avg_calls_per_day,"📞", "#D8C4B6")
                render_card("Avg. Calls per Day per Agent", avg_calls_per_day_by_agent, "📈", "#9F8383")

            with col3:
                render_card("Total Calls Connected", connected_calls, "🔗", "#D8C4B6")
                render_card("Avg. Connected Calls per Day", avg_connected_calls_per_day, "📈", "#9F8383")
                render_card("Avg. Connected Calls per Day per Agent", avg_connected_calls_per_day_by_agent, "🔗", "#D8C4B6")

            with col4:
                render_card("Total Valid Calls", valid_calls, "✅", "#9F8383")
                render_card("Avg. Valid Calls per Day", avg_valid_calls_per_day, "📈", "#D8C4B6")
                render_card("Avg. Valid Calls per Day per Agent", avg_valid_calls_per_day_by_agent, "✅", "#9F8383")

                # Custom CSS for borders, styling, and uniform box dimensions, with space between columns 
                st.markdown("""
                <style>
                .metric-column {
                    border: 2px solid #3E5879; /* Darker border color */
                    border-radius: 8px; /* Rounded corners */
                    padding: 10px;
                    text-align: center;
                    background-color: #D8C4B6; /* Updated box body color */
                    margin: 10px;
                    min-width: 150px; /* Minimum width */
                    height: 120px; /* Set fixed height */
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                }
                .metric-value {
                    font-size: 24px;
                    color: #3E5879; /* Darker text color for values */
                    font-weight: bold; /* Bold number */
                }
                .metric-label {
                    font-size: 14px;
                    color: #3E5879; /* Darker text color for labels */
                }
                .dataframe thead th {
                    background-color: #3E5879; /* Header background color */
                    color: #FFFFFF; /* Header text color */
                }
                .dataframe tbody tr:nth-child(even) td {
                    background-color: #FFFFFF; /* White for even rows */
                    color: #3E5879; /* Text color for even rows */
                }
                .dataframe tbody tr:nth-child(odd) td {
                    background-color: #D8C4B6; /* Beige for odd rows */
                    color: #3E5879; /* Text color for odd rows */
                }
                .col-wrapper {
                    display: flex;
                    justify-content: space-between; /* Space between columns */
                    align-items: center;
                }
                </style>
            """, unsafe_allow_html=True)
                
                # Pivot the data
            pivot_df = (
                df.groupby('sync_date')
                .agg(
                unique_agentid_count=('agentId', pd.Series.nunique),  # Unique agent IDs
                callremark_exists_count=('callRemark', lambda x: x.notnull().sum()),  # Count where callremark exists
                callremark_connected_count=('callRemark', lambda x: (x == 'Connected').sum()),  # Count where callremark is 'Connected'
                final_rejection_true_count=('v1_rejection', lambda x: (x == True).sum())  # Count where final rejection is True
                )
                .reset_index()
                )

            pivot_df.rename(
                columns={
                'sync_date': 'Date',
                'unique_agentid_count': 'No of Agents',
                'callremark_exists_count': 'No of Calls Made',
                'callremark_connected_count': 'Connected Calls',
                'final_rejection_true_count': 'Valid Sample After V1'
                },
                inplace=True
                )

            # Styling the table with pandas
            def style_table(df):
                return df.style.set_properties(**{
                'text-align': 'center',  # Center-align all text
                'border': '3px solid #3E5879',  # Border color matches the dark color
                'padding': '10px'  # Add padding for readability
                }).set_table_styles([{
                    'selector': 'th',
                    'props': [
                    ('background-color', '#3E5879'),  # Header background color
                    ('color', '#FFFFFF'),  # Header text color
                    ('font-weight', 'bold'),  # Bold header text
                    ('text-align', 'center')  # Center-align header text
                    ]
                    }, {
                        'selector': 'td',
                        'props': [
                            ('background-color', '#D8C4B6'),  # Table body background color
                            ('color', '#3E5879'),  # Text color for table body
                            ('border', '1px solid #3E5879')  # Border color for table cells
                            ]
                            }])
            st.markdown("<h2 style='text-align: center;'>Date-Wise Call Monitoring</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{pivot_df.to_html(index=False)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            # Download options
            csv1 = pivot_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Date-Wise Data",
                data=csv1,
                file_name="date_wise_data.csv",
                mime="text/csv",
                )
            # Agent-wise pivot
            filtered_df['agent_name'] = filtered_df['agentId'].apply(lambda x: x.split('-')[0])
            pivot_df2 = (
                filtered_df.groupby('agent_name').agg(
                callremark_exists_count=('callRemark', lambda x: x.notnull().sum()),  # Count where callremark exists
                callremark_connected_count=('callRemark', lambda x: (x == 'Connected').sum()),  # Count where callremark is 'Connected'
                final_rejection_true_count=('v1_rejection', lambda x: (x == True).sum())  # Count where final rejection is True
                )
                .reset_index()
                )

            pivot_df2.rename(
                columns={
                'agent_name': 'Agent Name',
                'callremark_exists_count': 'Total No of Calls Made',
                'callremark_connected_count': 'Connected Calls',
                'final_rejection_true_count': 'Valid Sample After V1'
                },
                inplace=True
                )

            with st.container():
                st.markdown("<h2 style='text-align: center;'>Agent-Wise Call Monitoring</h2>", unsafe_allow_html=True)
                st.markdown(f'<div class="dataframe">{pivot_df2.to_html(index=False)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            # Download options
            csv2 = pivot_df2.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Agent-Wise Data",
            data=csv2,
            file_name="agent_wise_data.csv",
            mime="text/csv",
            )
        with tab2:
            mapping_df=pd.read_excel(r"HP AC PC Mapping.xlsx")
            new_df = pd.merge(df, mapping_df, on="ac_no", how="left")
            AC_wise = (
                new_df.groupby(['ac_no','AC_Name']).agg(
                callremark_exists_count=('callRemark', lambda x: x.notnull().sum()),  # Count where callremark exists
                callremark_connected_count=('callRemark', lambda x: (x == 'Connected').sum()),  # Count where callremark is 'Connected'
                final_rejection_true_count=('v1_rejection', lambda x: (x == True).sum())  # Count where final rejection is True
                )
                .reset_index()
                )

            AC_wise.rename(
                columns={
                'ac_no': 'AC NO',
                'AC_Name':'AC Name',
                'callremark_exists_count': 'Total No of Calls Made',
                'callremark_connected_count': 'Connected Calls',
                'final_rejection_true_count': 'Valid Sample After V1'
                },
                inplace=True
                )
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{AC_wise.to_html(index=False)}</div>',unsafe_allow_html=True)

            # Download options
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            ac_csv = AC_wise.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download AC-Wise Data",
            data=ac_csv,
            file_name="ac_wise_data.csv",
            mime="text/csv",
            )
        with tab3:
            col1, col2 = st.columns([4, 4])  # Two equal columns

            # PC Name selection in the first column
            with col1:
                selected_pc_name = st.selectbox(
                "Select PC Name:",
                options=new_df["PC_Name"].unique(),
                help="Select a PC name from the list to filter data."
                )

            # AC Name selection in the second column
            with col2:
                filtered_df = new_df[new_df["PC_Name"] == selected_pc_name]
                selected_ac_name = st.selectbox(
                    "Select AC Name:",
                    options=filtered_df["AC_Name"].unique(),
                    help="Select an AC name from the list to filter data."
                    )

        # Further filtering based on selected AC name
            filtered_df = filtered_df[filtered_df["AC_Name"] == selected_ac_name]


            total_calls_made = filtered_df["callRemark"].notnull().sum()
            connected_calls = (filtered_df["callRemark"] == "Connected").sum()
            valid_calls = (filtered_df["final_rejection"] == True).sum()

            # Center-aligned metrics display
            st.markdown("<h2 style='text-align: center;'>Metrics</h2>", unsafe_allow_html=True)

            # Custom CSS for borders, styling, and uniform box dimensions, with space between columns
            st.markdown("""
            <style>
            .metric-column {
                border: 2px solid #3E5879; /* Dark border */
                border-radius: 8px; /* Rounded corners */
                padding: 15px;
                text-align: center;
                background-color: #D8C4B6; /* Light beige background */
                margin: 10px;
                min-width: 100px; /* Minimum width */
                height: 120px; /* Set fixed height */
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
            }
            table {
                width: 100%; 
                border-collapse: collapse; 
            }
            th, td {
                padding: 10px; 
                text-align: center; 
                font-size: 14px; 
                border: 1px solid #3E5879; 
            }
            th {
                background-color: #3E5879; 
                color: white; 
                font-weight: bold;
            }
            tr:nth-child(even) {
                background-color: #ffffff; /* White for even rows */
            }
            tr:nth-child(odd) {
                background-color: #D8C4B6; /* Light beige for odd rows */
            }
            tr:hover {
                background-color: #C4B8A6; /* Hover effect */
            }
            </style>
        """, unsafe_allow_html=True)


            st.markdown("""
            <style>
            .dataframe {
                border: 2px solid #3E5879; /* Dark border around the table */
                border-radius: 8px; /* Rounded corners */
                max-width: 100%;
                overflow-x: auto;
                overflow-y: auto;
                background-color: #D8C4B6; /* Light beige background for table */
                box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1); /* Subtle shadow */
                margin: 10px 0;
            }
            
            .dataframe th {
                background-color: #3E5879; /* Dark header background */
                color: white; /* White text for headers */
                font-size: 16px;
                padding: 10px;
                text-align: center;
                font-weight: bold;
            }
            
            .dataframe td {
                text-align: center; /* Center-align cell text */
                padding: 10px;
                font-size: 18px;
                color: #3E5879; /* Dark text for rows */
            }
            
            .dataframe tr:nth-child(even) {
                background-color: #f2e9e4; /* Slightly lighter beige for even rows */
            }
            
            .dataframe tr:nth-child(odd) {
                background-color: #D8C4B6; /* Light beige for odd rows */
            }
            
            .dataframe tr:hover {
                background-color: #C4B8A6; /* Slightly darker beige hover effect */
            }
            </style>
        """, unsafe_allow_html=True)

            # Create a container with space between the columns
            col1, col2, col3= st.columns([6,6,6],gap='small')

            # Wrapper div to space the columns apart
            with col1:
                st.markdown(f"""
                <div class="metric-column">
                    <div class="metric-label">Total Calls Made</div>
                    <div class="metric-value">{total_calls_made}</div>
                </div>
            """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-column">
                    <div class="metric-label">Connected Calls</div>
                    <div class="metric-value">{connected_calls}</div>
                </div>
            """, unsafe_allow_html=True)

            with col3:
                st.markdown(f"""
                <div class="metric-column">
                    <div class="metric-label">Valid Calls</div>
                    <div class="metric-value">{valid_calls}</div>
                </div>
            """, unsafe_allow_html=True)
                filtered_df = filtered_df[(filtered_df['incumabancy_choice_mla'] != 'nan') & (filtered_df['incumabancy_choice_mla'] != '')]
                R_df = (
                filtered_df.groupby('incumabancy_choice_mla').agg(
                    uid_count=('uid', 'count')  # Count the 'uid' in each group
                    )).reset_index()
                total_uids = R_df['uid_count'].sum()  # Total count of all 'uid'
                R_df['percentage'] = R_df['uid_count'].apply(
                    lambda x: f"{(x / total_uids * 100):.2f}%" if total_uids > 0 else "0%"
                    )
                R_df.rename(
                columns={
                'incumabancy_choice_mla': 'Re-Electing MLAs',
                'uid_count':'Sample'
                },
                inplace=True
                )
                R_df = R_df.sort_values(by='Sample', ascending=False).reset_index(drop=True)
            st.markdown("<h2 style='text-align: center;'>Re-Electing MLAs</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{R_df.to_html(index=False)}</div>', unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)

            # Download options
            R_csv = R_df.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Re-Electing Data",
            data=R_csv,
            file_name="re-elect_data.csv",
            mime="text/csv",
            )
            filtered_df = filtered_df[(filtered_df['frequency_visit_mla'] != 'nan') & (filtered_df['frequency_visit_mla'] != '')]
            F_df = pd.pivot_table(
            filtered_df,
            index='frequency_visit_mla',  # Rows as 'frequency_visit_mla'
            columns='incumabancy_choice_mla',  # Columns as 'incumabancy_choice_mla'
            values='uid',  # Values for the pivot table
            aggfunc='count',  # Count the 'uid'
            fill_value=0  # Fill missing values with 0
            )
            F_df_percentage = F_df.div(F_df.sum(axis=1), axis=0) * 100
            # Optionally, round percentages to 2 decimal places
            F_df_percentage = F_df_percentage.round(2).astype(str) + '%'
            F_df_percentage.rename(
                columns={
                    'frequency_visit_mla':'Frequency Visits MLAs'},inplace=True
                    )
            st.markdown("<h2 style='text-align: center;'>Frquency Visits MLAs vs Re-Elect MLAs</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{F_df_percentage.to_html(index=True)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)

            F_csv = F_df_percentage.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Visits MLAs vs Re-Elect MLAs",
            data=F_csv,
            file_name="Frequency_data.csv",
            mime="text/csv",
            )

            filtered_df = filtered_df[(filtered_df['development_rating_mla'] != 'nan') & (filtered_df['development_rating_mla'] != '')]
            D_df = pd.pivot_table(
            filtered_df,
            index='development_rating_mla',  # Rows as 'frequency_visit_mla'
            columns='incumabancy_choice_mla',  # Columns as 'incumabancy_choice_mla'
            values='uid',  # Values for the pivot table
            aggfunc='count',  # Count the 'uid'
            fill_value=0  # Fill missing values with 0
            )
            D_df_percentage = D_df.div(D_df.sum(axis=1), axis=0) * 100
            # Optionally, round percentages to 2 decimal places
            D_df_percentage = D_df_percentage.round(2).astype(str) + '%'
            D_df_percentage.rename(
                columns={
                    'development_rating_mla':'Development Rating MLAs'},inplace=True
                    )
            st.markdown("<h2 style='text-align: center;'>Development Rating MLAs vs Re-Elect MLAs</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{D_df_percentage.to_html(index=True)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            D_csv = D_df_percentage.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Rating MLAs vs Re-Elect MLAs",
            data=D_csv,
            file_name="Frequency_data.csv",
            mime="text/csv",
            )
            

            filtered_df = filtered_df[(filtered_df['accessibilty_mla'] != 'nan') & (filtered_df['accessibilty_mla'] != '')]
            A_df = pd.pivot_table(
            filtered_df,
            index='accessibilty_mla',  # Rows as 'frequency_visit_mla'
            columns='incumabancy_choice_mla',  # Columns as 'incumabancy_choice_mla'
            values='uid',  # Values for the pivot table
            aggfunc='count',  # Count the 'uid'
            fill_value=0  # Fill missing values with 0
            )
            A_df_percentage = A_df.div(A_df.sum(axis=1), axis=0) * 100
            # Optionally, round percentages to 2 decimal places
            A_df_percentage = A_df_percentage.round(2).astype(str) + '%'
            A_df_percentage.rename(
                columns={
                    'accessibilty_mla':'Accessibility of MLAs'},inplace=True
                    )
            st.markdown("<h2 style='text-align: center;'>Accessibility of MLAs vs Re-Elect MLAs</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{A_df_percentage.to_html(index=True)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            A_csv = A_df_percentage.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Accessibility MLAs vs Re-Elect MLAs",
            data=A_csv,
            file_name="Frequency_data.csv",
            mime="text/csv",
            )
            
            filtered_df = filtered_df[(filtered_df['past_vote_pref'] != 'nan') & (filtered_df['past_vote_pref'] != '')]
            party_df = pd.pivot_table(
            filtered_df,
            index='past_vote_pref',  # Rows as 'frequency_visit_mla'
            columns='incumabancy_choice_mla',  # Columns as 'incumabancy_choice_mla'
            values='uid',  # Values for the pivot table
            aggfunc='count',  # Count the 'uid'
            fill_value=0  # Fill missing values with 0
            )
            party_df_percentage = party_df.div(party_df.sum(axis=1), axis=0) * 100
            # Optionally, round percentages to 2 decimal places
            party_df_percentage = party_df_percentage.round(2).astype(str) + '%'
            st.markdown("<h2 style='text-align: center;'>Past AE Preference vs Re-Elect MLAs</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{party_df_percentage.to_html(index=True)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            p_csv = party_df_percentage.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download Past AE Pref vs Re-Elect MLAs",
            data=p_csv,
            file_name="past_ae_data.csv",
            mime="text/csv",
            )

            p_df = (
                filtered_df.groupby('past_vote_pref').agg(
                    uid_count=('uid', 'count')  # Count the 'uid' in each group
                    )).reset_index()
            total_uids = p_df['uid_count'].sum()  # Total count of all 'uid'
            p_df['percentage'] = p_df['uid_count'].apply(
                    lambda x: f"{(x / total_uids * 100):.2f}%" if total_uids > 0 else "0%"
                    )
            p_df = p_df.sort_values(by='uid_count', ascending=False).reset_index(drop=True)
            st.markdown("<h2 style='text-align: center;'>Past AE Preference</h2>", unsafe_allow_html=True)
            st.markdown(f'<div class="dataframe">{p_df.to_html(index=False)}</div>',unsafe_allow_html=True)
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            p2_csv = p_df.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Past AE Preference",
            data=p2_csv,
            file_name="past_ae_data.csv",
            mime="text/csv",
            )
        with tab4:
            Overall_df = (
                new_df.groupby(['PC_Name','district','ac_no','AC_Name']).agg(
                Valid_Counts=('final_rejection', lambda x: (x == True).sum())  # Count where final rejection is True
                )
                .reset_index()
                )
            rating_counts = (
                new_df.groupby('ac_no')['development_rating_mla']
                .value_counts(normalize=False)
                .unstack(fill_value=0)
                .reset_index()
                )
            rating_counts['Total'] = (
            rating_counts[['Excellent', 'Very Good', 'Average','Poor','No development work initiated']].sum(axis=1)
            )
            rating_counts['Development Rating'] = ((
            (5*rating_counts['Excellent'] + 4*rating_counts['Very Good']+3*rating_counts['Average']
            +2*rating_counts['Poor']+1*rating_counts['No development work initiated'])/ rating_counts['Total'])
            ).round(2).fillna(0)
            rating_counts['Development Rating %'] = ((
            (5*rating_counts['Excellent'] + 4*rating_counts['Very Good']+3*rating_counts['Average']
            +2*rating_counts['Poor']+1*rating_counts['No development work initiated'])/ rating_counts['Total']* 100)/5
            ).round(2).fillna(0).astype(str) + '%'
            merged_df = pd.merge(Overall_df, rating_counts[['ac_no','Development Rating','Development Rating %']], on='ac_no', how='right')
            
            rating_counts2 = (
                new_df.groupby('ac_no')['frequency_visit_mla']
                .value_counts(normalize=False)
                .unstack(fill_value=0)
                .reset_index()
                )
            rating_counts2['Total'] = (
            rating_counts2[['Very Frequently', 'Frequently', 'Occasionally','Rarely','Never']].sum(axis=1)
            )
            rating_counts2['Frequency Rating'] = ((
            (5*rating_counts2['Very Frequently'] + 4*rating_counts2['Frequently']+3*rating_counts2['Occasionally']
            +2*rating_counts2['Rarely']+1*rating_counts2['Never'])/ rating_counts2['Total'])
            ).round(2).fillna(0)
            rating_counts2['Frequency Rating %'] = ((
            (5*rating_counts2['Very Frequently'] + 4*rating_counts2['Frequently']+3*rating_counts2['Occasionally']
            +2*rating_counts2['Rarely']+1*rating_counts2['Never'])/ rating_counts2['Total']* 100)/5
            ).round(2).fillna(0).astype(str) + '%'
            merged_df = pd.merge(merged_df, rating_counts2[['ac_no','Frequency Rating','Frequency Rating %']], on='ac_no', how='right')

            rating_counts3 = (
                new_df.groupby('ac_no')['accessibilty_mla']
                .value_counts(normalize=False)
                .unstack(fill_value=0)
                .reset_index()
                )
            rating_counts3['Total'] = (
            rating_counts3[['Highly Accessible', 'Occasionally Accessible', 'Moderately Accessible','Poor','Inaccessible']].sum(axis=1)
            )
            rating_counts3['Accessibility Rating'] = ((
            (5*rating_counts3['Highly Accessible'] + 4*rating_counts3['Occasionally Accessible']+3*rating_counts3['Moderately Accessible']
            +2*rating_counts3['Poor']+1*rating_counts3['Inaccessible'])/ rating_counts3['Total'])
            ).round(2).fillna(0)
            rating_counts3['Accessibility Rating %'] = ((
            (5*rating_counts3['Highly Accessible'] + 4*rating_counts3['Occasionally Accessible']+3*rating_counts3['Moderately Accessible']
            +2*rating_counts3['Poor']+1*rating_counts3['Inaccessible'])/ rating_counts3['Total']*100)/5
            ).round(2).fillna(0).astype(str) + '%'
            merged_df = pd.merge(merged_df, rating_counts3[['ac_no','Accessibility Rating','Accessibility Rating %']], on='ac_no', how='right')
            

            new_df = new_df[(new_df['incumabancy_choice_mla'] != 'nan') & (new_df['incumabancy_choice_mla'] != '')]
            Ov_df = pd.pivot_table(
            new_df,
            index='ac_no',  # Rows as 'frequency_visit_mla'
            columns='incumabancy_choice_mla',  # Columns as 'incumabancy_choice_mla'
            values='uid',  # Values for the pivot table
            aggfunc='count',  # Count the 'uid'
            fill_value=0  # Fill missing values with 0
            ).reset_index()
            Ov_df_percentage = Ov_df.set_index('ac_no').div(Ov_df.set_index('ac_no').sum(axis=1), axis=0) * 100
            Ov_df_percentage = Ov_df_percentage.applymap(lambda x: f"{x:.2f}%")
            Ov_df_percentage = Ov_df_percentage.reset_index()  # Reset the index
            Ov_df_percentage = Ov_df_percentage.round(2)  # Round to 2 decimal places
            
            # Merge with the main DataFrame
            merged_df = pd.merge(
            merged_df,
            Ov_df_percentage[['ac_no', 'Yes', 'No', 'Have not decided']],  # Select required columns
            on='ac_no',
            how='right'
            )

            # Optionally, round percentages to 2 decimal places
        # Round the values to 2 decimal places
            merged_df['Overall Rating']=merged_df['Development Rating']+merged_df['Frequency Rating']+merged_df['Accessibility Rating']
            merged_df['Overall Rating %']=(merged_df['Development Rating %'].str.rstrip('%').astype(float)+merged_df['Frequency Rating %'].str.rstrip('%').astype(float)+merged_df['Accessibility Rating %'].str.rstrip('%').astype(float))/3
            merged_df = merged_df.sort_values(by='ac_no', ascending=True).reset_index(drop=True)
            merged_df = merged_df.rename(columns={
            "PC_Name":"PC Name",
            "district":"District",
            "ac_no":"AC No",
            "AC_Name":"AC Name",
            "Valid_Counts":"Valid Sample",
            "Yes": "Re-Elect MLA (Yes)",
            "No": "Re-Elect MLA (No)",
            "Have not decided": "Re-Elect MLA (HND)"
            })
        # Add a new column for performance category
            def categorize(row):
                if row['Overall Rating %'] < 50:
                    return 'Poor'
                elif 50 <= row['Overall Rating %'] < 60:
                    return 'Average'
                else:
                    return 'Good'

            # Apply the function to create the 'Performance' column
            merged_df['Performance'] = merged_df.apply(lambda row: categorize(row), axis=1)

            # Streamlit formatting for color-coded cells in the 'Performance' column
            def highlight_performance(val):
                if val == 'Poor':
                    color = 'red'
                elif val == 'Average':
                    color = 'lightgreen'
                elif val == 'Good':
                    color = 'darkgreen'
                else:
                    color = 'white'  # Default color
                return f'background-color: {color}'

            # Display the DataFrame in Streamlit with color applied only to the 'Performance' column
            st.markdown("""
            <style>
            .dataframe-container {
                width: 100%; /* Ensure the table fits within the container */
                margin: 0 auto; /* Center the table */
                overflow-x: auto; /* Enable horizontal scrolling if needed */
                border: 1px solid #3E5879; /* Dark border around the table */
                border-radius: 8px; /* Rounded corners */
                background-color: #D8C4B6; /* Light beige background for the container */
            }

            table.dataframe {
                width: 100%;
                border-collapse: collapse; /* Remove extra spacing between cells */
            }

            th, td {
                padding: 10px; /* Adjust padding for better readability */
                text-align: center; /* Center-align text */
                font-size: 14px; /* Set font size */
                border: 1px solid #3E5879; /* Border color matching the theme */
            }

            th {
                background-color: #3E5879; /* Dark header background */
                color: white; /* White text for headers */
                font-weight: bold; /* Bold font for headers */
                text-align: center;
            }

            tr:nth-child(even) {
                background-color: #ffffff; /* White for even rows */
            }

            tr:nth-child(odd) {
                background-color: #D8C4B6; /* Light beige for odd rows */
            }

            tr:hover {
                background-color: #C4B8A6; /* Slightly darker beige hover effect */
            }
            </style>
            """, unsafe_allow_html=True)


            styled_df=merged_df.style.applymap(
                highlight_performance, subset=['Performance']  # Apply formatting to 'Performance' column only
                ).format(
                {'Overall Rating %': '{:.2f}%'}  # Format 'Overall Rating %' column as percentage
                )
            html_table = styled_df.to_html(index=False)
            st.markdown(f'<div class="dataframe-container">{html_table}</div>',unsafe_allow_html=True)

            # Download options
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            O_csv = merged_df.to_csv(index=False).encode("utf-8")
            st.download_button(
            label="Download ESR Report",
            data=O_csv,
            file_name="overall_data.csv",
            mime="text/csv",
            )
        with tab5:
            col1, col2,col3 = st.columns([4, 4, 4])  # Two equal columns
            # Date selection in the first column
            with col1:
                selected_date = st.selectbox(
                "Select Date:",
                options=df["sync_date"].unique(),
                help="Select a date.",
                key="date_select"  # Unique key for the widget
                )

            # PC Name selection in the second column
            with col2:
                filtered_df = new_df[new_df["sync_date"] == selected_date]
                selected_pc_name = st.selectbox(
                "Select PC Name:",
                options=filtered_df["PC_Name"].unique(),
                help="Select a PC name from the list to filter data.",
                key="pc_name_select"  # Unique key for the widget
                )

            # AC Name selection in the third column
            with col3:
                filtered_df = filtered_df[filtered_df["PC_Name"] == selected_pc_name]
                selected_ac_name = st.selectbox(
                "Select AC Name:",
                options=filtered_df["AC_Name"].unique(),
                help="Select an AC name from the list to filter data.",
                key="ac_name_select"  # Unique key for the widget
                )

            # Final filtering based on selected AC name
            filtered_df = filtered_df[filtered_df["AC_Name"] == selected_ac_name]
            filtered_df = filtered_df[(filtered_df['development_rating_mla'] != 'nan') & (filtered_df['development_rating_mla'] != '')]
            D_D=(
                filtered_df.groupby('development_rating_mla')
                .agg(
                sample=('uid','count')
                )
                .reset_index()
                )
            filtered_df = filtered_df[(filtered_df['frequency_visit_mla'] != 'nan') & (filtered_df['frequency_visit_mla'] != '')]
            F_D=(
                filtered_df.groupby('frequency_visit_mla')
                .agg(
                sample=('uid','count')
                )
                .reset_index()
                )
            filtered_df = filtered_df[(filtered_df['accessibilty_mla'] != 'nan') & (filtered_df['accessibilty_mla'] != '')]
            A_D=(
                filtered_df.groupby('accessibilty_mla')
                .agg(
                sample=('uid','count')
                )
                .reset_index()
                )
            labels1 = D_D['development_rating_mla']
            values1 = D_D['sample']
            color_mapping1 = {
            "Excellent": "#228B22",
            "Very Good": "#3ACF3A",  # You can specify the exact light green shade if needed
            "Average": "#A9EAA9",  # Same as Very Good (light green)
            "Poor": "#B9B7BD",  # Light red shade
            "No development work initiated": "#868B8E"
            }
            colors1 = [color_mapping1[label] for label in labels1]
            color_mapping2 = {
            "Very Frequently": "#228B22",
            "Frequently": "#3ACF3A",  # You can specify the exact light green shade if needed
            "Occasionally": "#A9EAA9",  # Same as Very Good (light green)
            "Rarely": "#B9B7BD",  # Light red shade
            "Never": "#868B8E"
            }
            labels2 = F_D['frequency_visit_mla']
            values2 = F_D['sample']
            colors2 = [color_mapping2[label] for label in labels2]
            labels3 = A_D['accessibilty_mla']
            values3 = A_D['sample']
            color_mapping3 = {
            "Highly Accessible": "#228B22",
            "Occasionally Accessible": "#3ACF3A",  # You can specify the exact light green shade if needed
            "Moderately Accessible": "#A9EAA9",  # Same as Very Good (light green)
            "Poor": "#B9B7BD",  # Light red shade
            "Inaccessible": "#868B8E"
            }
            colors3 = [color_mapping3[label] for label in labels3]
            
            # Create the donut chart
            fig = go.Figure()
            fig.add_trace(go.Pie(
            labels=labels1,
            values=values1,
            hole=0.5,
            marker=dict(colors=colors1),
            textinfo='label+percent',
            insidetextorientation='radial',
            domain=dict(x=[0, 0.28], y=[0, 1])  # Place on the top left
            ))
            fig.add_trace(go.Pie(
            labels=labels2,
            values=values2,
            hole=0.5,
            marker=dict(colors=colors2),
            textinfo='label+percent',
            insidetextorientation='radial',
            domain=dict(x=[0.36, 0.64], y=[0, 1])  
            ))
            fig.add_trace(go.Pie(
            labels=labels3,
            values=values3,
            hole=0.5,
            marker=dict(colors=colors2),
            textinfo='label+percent',
            insidetextorientation='radial',
            domain=dict(x=[0.72, 1], y=[0, 1])  
            ))
            # Update layout for title and formatting
            fig.update_layout(
            title_text="MLA Performance Rating",
            title_x=0.5,  # Center the title
            margin=dict(t=50, b=50, l=50, r=50),  # Adjust margins
            height=300,  # Chart height
            showlegend=False,
            width=700   # Chart width
            )

            # Render the chart in Streamlit
            st.plotly_chart(fig, use_container_width=False)
            fig2 = go.Figure()
            pivot_df['Avg_Calls']=pivot_df['No of Calls Made']/pivot_df['No of Agents']
            fig2.add_trace(go.Scatter(
            x=pivot_df['Date'],
            y=pivot_df['Connected Calls'],
            mode='lines+markers',  # Show both lines and markers for data points
            name='Connected Calls',
            line=dict(color='#3D550C', width=4),  # Line color and thickness
            marker=dict(symbol='circle', size=6)  # Markers at data points
            ))

            # Add trace for 'call_made' column
            fig2.add_trace(go.Scatter(
            x=pivot_df['Date'],
            y=pivot_df['No of Calls Made'],
            mode='lines+markers',  # Show both lines and markers for data points
            name='Call Made',
            line=dict(color='#81B622', width=4),  # Line color and thickness
            marker=dict(symbol='circle', size=6)  # Markers at data points
            ))

            # Add trace for 'valid' column
            fig2.add_trace(go.Scatter(
            x=pivot_df['Date'],
            y=pivot_df['Avg_Calls'],
            mode='lines+markers',  # Show both lines and markers for data points
            name='Average Calls',
            line=dict(color='#ECF87F', width=4),  # Line color and thickness
            marker=dict(symbol='circle', size=6)  # Markers at data points
            ))

            # Update layout for better styling
            fig2.update_layout(
            title="Date-wise Connected, Call Made, and Average Calls",
            xaxis_title="Date",
            yaxis_title="Values",
            template="plotly_dark",  # Dark theme for better visuals
            xaxis=dict(tickformat='%Y-%m-%d', tickangle=45),  # Format and rotate date labels
            margin=dict(t=50, b=50, l=50, r=50),  # Adjust margins for better readability
            )
            # Display the graph in Streamlit
            st.plotly_chart(fig2, use_container_width=True)


            fig3 = go.Figure()

            fig3.add_trace(go.Scatter(
            x=AC_wise["AC Name"],  # AC Names
            y=AC_wise["Connected Calls"],  # Connected Calls
            mode='lines',  # Use lines for area chart
            fill='tozeroy',  # Fill area under the curve to the X-axis
            line=dict(color='#94C973', width=2),  # Customize line style
            name="Connected Calls"
            ))

            # Update layout for better appearance
            fig3.update_layout(
            title="AC-wise Connected Calls",
            xaxis_title="Assembly Constituency (AC)",
            yaxis_title="Connected Calls",
            template="plotly_white",  # Use a clean white theme
            margin=dict(t=50, b=50, l=50, r=50),  # Adjust margins
            height=500,  # Set chart height
            width=800,  # Set chart width
            )
            st.plotly_chart(fig3, use_container_width=True)
