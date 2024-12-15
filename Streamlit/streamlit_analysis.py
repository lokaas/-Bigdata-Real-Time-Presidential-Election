import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, when
from pyspark.sql.types import StructType, StringType, IntegerType, LongType
import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeElectionVisualization") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# Define the schema for the voter data
schema = StructType() \
    .add("id", LongType()) \
    .add("gender", StringType()) \
    .add("age", IntegerType()) \
    .add("state", StringType()) \
    .add("decision", StringType())

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "voter_data") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter out voters under 18 and create custom age groups starting from 18
df = df.filter(col("age") >= 18)
age_group = (
    df.withColumn("age_group", 
                  when((col("age") >= 18) & (col("age") <= 28), "18-28")
                  .when((col("age") >= 29) & (col("age") <= 38), "29-38")
                  .when((col("age") >= 39) & (col("age") <= 48), "39-48")
                  .when((col("age") >= 49) & (col("age") <= 58), "49-58")
                  .when((col("age") >= 59) & (col("age") <= 68), "59-68")
                  .otherwise("69+"))
    .groupBy("age_group", "decision")
    .agg(count("id").alias("count"))
)

# Other aggregations remain the same
winning_counts = df.groupBy("decision").agg(count("id").alias("count"))
total_votes = df.groupBy().agg(count("id").alias("total_count"))
gender_counts = df.groupBy("gender", "decision").agg(count("id").alias("count"))
state_counts = df.groupBy("state", "decision").agg(count("id").alias("count"))

# [Previous streaming queries remain the same]
query_winning_counts = winning_counts.writeStream \
    .format("memory") \
    .queryName("winning_counts") \
    .outputMode("complete") \
    .start()

query_total_votes = total_votes.writeStream \
    .format("memory") \
    .queryName("total_votes") \
    .outputMode("complete") \
    .start()

query_gender_counts = gender_counts.writeStream \
    .format("memory") \
    .queryName("gender_counts") \
    .outputMode("complete") \
    .start()

query_age_group = age_group.writeStream \
    .format("memory") \
    .queryName("age_group") \
    .outputMode("complete") \
    .start()

query_state_counts = state_counts.writeStream \
    .format("memory") \
    .queryName("state_counts") \
    .outputMode("complete") \
    .start()

# Start the Streamlit app
st.title("Real-Time Presidential Election - Who's Winning?")

# Create placeholders for summary and charts
summary_placeholder = st.empty()
bar_chart_placeholder = st.empty()
gender_chart_placeholder = st.empty()
age_chart_placeholder = st.empty()
state_chart_placeholder = st.empty()

# Function to display data
def display_data():
    try:
        # Query winning counts and total votes
        winning_data = spark.sql("SELECT * FROM winning_counts ORDER BY decision").toPandas()

        # Check if the DataFrame is not empty
        if not winning_data.empty:
            # Calculate total valid votes (sum of all candidate votes)
            valid_votes = winning_data['count'].sum()

            # Calculate percentages based on total valid votes
            winning_data['percentage'] = (winning_data['count'] / valid_votes * 100).round(2)
            
            # Format percentage string with % sign
            winning_data['percentage'] = winning_data['percentage'].astype(str) + '%'

            # Prepare summary data
            summary_data = winning_data[['decision', 'count', 'percentage']]
            summary_data.columns = ['Candidate', 'Vote Count', 'Percentage (%)']

            # Update summary table
            summary_placeholder.table(summary_data)

            #chart for winning candidates
            plt.figure(figsize=(4, 3))
            colors = ["#FF0000" if candidate == "Donald Trump" else "#0000FF" 
                     for candidate in winning_data['decision']]
            plt.bar(winning_data['decision'], winning_data['count'], color=colors)
            plt.title("Vote Count by Candidate", pad=10)
            plt.xlabel("Candidates")
            plt.ylabel("Vote Count")
            plt.xticks(rotation=0, fontsize=8)
            plt.yticks(fontsize=8)
            plt.tight_layout()
            bar_chart_placeholder.pyplot(plt)
            plt.close()
            
            #gender chart
            gender_data = spark.sql("SELECT * FROM gender_counts").toPandas()
            gender_pivot = gender_data.pivot(index='gender', columns='decision', values='count').fillna(0)
            if 'Donald Trump' in gender_pivot.columns and 'Kamala Harris' in gender_pivot.columns:
                gender_pivot = gender_pivot[['Donald Trump', 'Kamala Harris']]
            plt.figure(figsize=(4, 3))
            gender_pivot.plot(kind='bar', stacked=True, color=["#FF0000", "#0000FF"], ax=plt.gca())
            plt.title("Votes by Gender", pad=10)
            plt.xlabel("Gender")
            plt.ylabel("Vote Count")
            plt.xticks(rotation=0, fontsize=8)
            plt.yticks(fontsize=8)
            plt.legend(fontsize=6)
            plt.tight_layout()
            gender_chart_placeholder.pyplot(plt)
            plt.close()

            #age group chart
            age_data = spark.sql("SELECT * FROM age_group").toPandas()
            age_pivot = age_data.pivot(index='age_group', columns='decision', values='count').fillna(0)
            if 'Donald Trump' in age_pivot.columns and 'Kamala Harris' in age_pivot.columns:
                age_pivot = age_pivot[['Donald Trump', 'Kamala Harris']]
            plt.figure(figsize=(4, 3))
            age_pivot.plot(kind='bar', stacked=True, color=["#FF0000", "#0000FF"], ax=plt.gca())
            plt.title("Votes by Age Group", pad=10)
            plt.xlabel("Age Group")
            plt.ylabel("Vote Count")
            plt.xticks(fontsize=6)
            plt.yticks(fontsize=8)
            plt.legend(fontsize=6)
            plt.tight_layout()
            age_chart_placeholder.pyplot(plt)
            plt.close()

            #state chart
            state_data = spark.sql("SELECT * FROM state_counts").toPandas()
            state_pivot = state_data.pivot(index='state', columns='decision', values='count').fillna(0)
            if 'Donald Trump' in state_pivot.columns and 'Kamala Harris' in state_pivot.columns:
                state_pivot = state_pivot[['Donald Trump', 'Kamala Harris']]
            
            # Create figure and axis objects for more control
            fig, ax = plt.subplots(figsize=(10, 8))
            state_pivot.plot(kind='barh', stacked=True, color=["#FF0000", "#0000FF"], ax=ax)
         
            # Format x-axis to show integer values
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
            
            plt.title("Votes by State", pad=10, fontsize=25)
            plt.xlabel("Vote Count", fontsize=25)
            plt.ylabel("State", fontsize=25)
            plt.yticks(fontsize=9)
            plt.xticks(fontsize=10)
            plt.legend(fontsize=9)
            plt.tight_layout()
            state_chart_placeholder.pyplot(fig)
            plt.close()


        else:
            summary_placeholder.write("Waiting for data...")

    except Exception as e:
        st.error(f"An error occurred: {e}")

# Continuously update the data
while True:
    time.sleep(5)
    display_data()

# Stop the streaming queries when exiting
query_winning_counts.awaitTermination()
query_total_votes.awaitTermination()
query_gender_counts.awaitTermination()
query_age_group.awaitTermination()
query_state_counts.awaitTermination()
