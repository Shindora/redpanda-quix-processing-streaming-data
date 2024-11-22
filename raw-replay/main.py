from quixstreams import Application
import json
import time
import os
import uuid
from faker import Faker
from dotenv import load_dotenv
from structlog import get_logger

log = get_logger()
load_dotenv(override=False)

app = Application()

topic_name = os.getenv("output")
if not topic_name:
    raise ValueError(
        "The 'output' environment variable is required. This is the output topic that data will be published to."
    )

topic = app.topic(topic_name)

fake = Faker()
cs_job_titles = [
    "Software Engineer",
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Researcher",
    "DevOps Engineer",
    "Cybersecurity Analyst",
    "Full Stack Developer",
    "Frontend Developer",
    "Backend Developer",
    "Cloud Architect",
    "Database Administrator",
    "System Administrator",
    "Network Engineer",
    "Data Engineer",
    "Product Manager",
    "QA Engineer",
    "Game Developer",
    "UI/UX Designer",
]


def generate_fake_message():
    """
    Generate a fake message with a job title restricted to the computer science domain.
    """
    return {
        "id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "phone_number": fake.phone_number(),
        "date_of_birth": fake.date_of_birth().isoformat(),
        "company": fake.company(),
        "job_title": fake.random_element(cs_job_titles),
        "created_at": fake.iso8601(),
        "updated_at": fake.iso8601(),
    }


number_of_messages = 100000

# Produce messages to Kafka topic
with app.get_producer() as producer:
    message_key = str(uuid.uuid4())[:10]

    for _ in range(number_of_messages):
        message = generate_fake_message()

        producer.produce(topic.name, json.dumps(message), message_key)

        log.info("Message published", message=message)
        time.sleep(0.01)

    producer.flush(30)
    log.info("All messages have been flushed to the Kafka topic")
