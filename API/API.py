from flask import Flask, Response
from kafka import KafkaProducer
import random
import json
import time

app = Flask(__name__)

# list of all 50 states
states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Sample candidates
candidates = ["Donald Trump", "Kamala Harris"]

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization for Kafka
)

# Streaming endpoint
@app.route('/api/stream_votes', methods=['GET'])
def stream_votes():
    def generate_vote():
        while True:
            # Generate a random 16-digit integer ID
            voter_id = random.randint(10**15, 10**16 - 1)
            
            # Generate random voter data
            voter = {
                "id": int(voter_id),
                "gender": random.choice(["Male", "Female"]),
                "age": random.randint(18, 90),
                "state": random.choice(states),
                "decision": str(random.choice(candidates)) 
            }
            # Send data to Kafka
            producer.send('voter_data', voter)
            yield f"data:{json.dumps(voter)}\n\n"  # Display data in API response for testing
            time.sleep(1)  # Simulate real-time delay

    return Response(generate_vote(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(debug=True, port=5000)  # Runs on port 5000

