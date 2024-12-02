# Real-Time Presidential Election Processing
## Project Overview
This project simulates a real-time presidential election data pipeline, allowing for live updates and data analysis. Leveraging a custom API for data generation, Kafka for data streaming, HDFS for data storage, Spark Streaming for processing, and Streamlit for visualization, this setup provides a comprehensive look at how data engineering can support real-world election analytics.
________________________________________
## Architecture
### Pipeline Flow:
1.	Data Generation: An API generates voter data including ID, gender, age, state, and candidate decision.
2.	Data Ingestion to Kafka: The generated data is streamed into a Kafka topic (voter_data), serving as a message broker to allow seamless data flow.
3.	Data Storage in HDFS: Using a Flume agent, the data from Kafka is continuously ingested into HDFS, enabling robust data storage and easy access for large-scale data processing.
4.	Data Processing with Spark Streaming: Spark Streaming reads the data directly from Kafka, processes it in real-time, and computes key metrics
5.	Real-Time Visualization in Streamlit: The processed data is passed to Streamlit, where it’s dynamically visualized. Key insights include live tracking of the winning candidate, demographics, and geographic distribution of votes.


![project_flowchart](https://github.com/user-attachments/assets/c9a516d0-8444-4da8-a84a-c11a2114d380)

________________________________________
## Components
1. API for Data Generation
The API creates randomized voter data to mimic real-world scenarios, generating fields such as:
•	ID: Unique identifier for each voter
•	Gender: Voter's gender
•	Age: Voter's age group
•	State: U.S. state for geographic distribution
•	Decision: Candidate selected by the voter
2. Kafka for Data Streaming
Apache Kafka serves as the message broker, managing the data flow between the API and downstream processes.
3. Flume Agent for HDFS Storage
Flume acts as an intermediary agent, ingesting data from Kafka and storing it into HDFS, ensuring high availability and durability of the data.
4. Spark Streaming for Data Processing
Spark Streaming processes the data from Kafka, aggregating information and generating real-time metrics for visualization.
5. Streamlit for Real-Time Visualization
The processed metrics are rendered live in Streamlit with interactive charts and visuals to give a comprehensive view of election dynamics, allowing users to:
•	View overall and candidate-specific vote counts and percentages
•	Analyze voter demographics across gender and age groups
•	See state-by-state vote distributions
________________________________________
## Results
This project demonstrates the complete workflow of real-time data streaming, processing, and visualization in a way that mirrors live election monitoring. Through detailed and continuously updated visuals, it’s possible to analyze voting trends and demographic breakdowns that inform and engage users.
________________________________________
## Technologies Used
•	Kafka: Message broker for real-time data streaming
•	HDFS (via Flume): Distributed storage for large-scale data handling
•	Spark Streaming: Real-time data processing
•	Streamlit: Live data visualization
•	Python: API development and data handling
•	Plotly & Matplotlib: Custom data visualizations
________________________________________
## How to Run
1.	Set up the API to begin data generation.
2.	Start Kafka and produce data to the voter_data topic.
3.	Deploy the Flume Agent to pull from Kafka and store data in HDFS.
4.	Run Spark Streaming to process data from Kafka.
5.	Launch Streamlit to visualize the real-time data.
________________________________________
This project exemplifies end-to-end data engineering, combining real-time data generation, processing, and visualization to mimic election tracking at scale. It demonstrates the practical application of big data tools and stream processing in real-world scenarios, with implications for multiple industries beyond elections.
