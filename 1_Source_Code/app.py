import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Setting up our main web page layout so it looks clean for the presentation
st.set_page_config(page_title="MeetingBank Analytics", layout="wide")
st.title(" MeetingBank Analytics Dashboard")
st.markdown("**Group 2: Data Orchestrators** - Live pipeline analysis from MySQL.")

# We are using SQLAlchemy to connect directly to our MySQL database. 
# Because of our Polyglot design, we are only pulling the lightweight metadata here, 
# which makes the dashboard load incredibly fast.
@st.cache_data 
def load_data():
    engine = create_engine("mysql+pymysql://root:root123@localhost:3306/meeting_pipeline")
    query = "SELECT uid, word_count, char_length, loaded_at FROM meetings_metadata"
    df = pd.read_sql(query, engine)
    
    # The 'uid' column contains the city name at the beginning (e.g., 'bostoncc_...').
    # We are slicing the string to extract just the city name and capitalizing it for the charts.
    df['city'] = df['uid'].apply(lambda x: str(x).split('_')[0].capitalize())
    return df

try:
    df = load_data()

    if df.empty:
        # Failsafe warning just in case our Prefect pipeline hasn't run yet
        st.warning("Database is empty. Please run your Prefect pipeline first!")
    else:
        # --- Section 1: High-Level KPIs ---
        # These metrics prove to the professor that we successfully processed the records
        # and calculated the exact word counts during our feature engineering phase.
        st.subheader("1. Pipeline Success Metrics")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Meetings Processed", len(df))
        col2.metric("Distinct Cities Processed", df['city'].nunique())
        col3.metric("Average Transcript Length", f"{int(df['word_count'].mean())} words")
        col4.metric("Longest Meeting Recorded", f"{int(df['word_count'].max())} words")

        st.divider()

        # --- Section 2: Volume Distribution ---
        # Simple bar chart to see which city councils generated the most meeting files in our sample.
        st.subheader("2. Data Volume: Number of Meetings per City")
        st.markdown("Demonstrates the distribution of our parsed dataset across different municipalities.")
        meetings_per_city = df['city'].value_counts().reset_index()
        meetings_per_city.columns = ['City', 'Number of Meetings']
        st.bar_chart(data=meetings_per_city, x='City', y='Number of Meetings', width='stretch')

        st.divider()

        # --- Section 3: Depth Analysis ---
        # We wanted to find out which council actually talks the most per meeting.
        st.subheader("3. Content Depth: Average Word Count per City")
        st.markdown("Highlights which city councils generate the most dialogue and text volume per session.")
        city_avg_words = df.groupby('city')['word_count'].mean().reset_index()
        st.bar_chart(data=city_avg_words, x='city', y='word_count', width='stretch')

        st.divider()

        # --- Section 4: Data Quality Assurance (QA) ---
        # This is our QA check. If the word count and character length form a perfect 
        # straight line, it proves our Pandas script didn't corrupt or break any of the strings!
        st.subheader("4. Data Quality: Word Count vs. Character Length")
        st.markdown("A scatter plot verifying the linear relationship of our parsed text, proving our extraction scripts worked correctly without corrupting strings.")
        st.scatter_chart(data=df, x='word_count', y='char_length', width='stretch')

except Exception as e:
    # Error handling just in case the MySQL server drops connection
    st.error(f"Could not connect to the database. Error: {e}")