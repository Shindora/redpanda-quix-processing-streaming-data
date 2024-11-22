import os
from quixstreams import Application
from console_sink import ConsoleSink
from dotenv import load_dotenv
from datetime import timedelta

# Load environment variables
load_dotenv()

# Initialize application and topics
app = Application(consumer_group="gaion", auto_offset_reset="earliest")

input_topic = app.topic(os.environ.get("input", "raw-data"))
output_topic = app.topic(os.environ.get("output", "normalized-data"))
console_sink = ConsoleSink()

# Read messages from input topic
sdf = app.dataframe(input_topic)

# Process data
sdf = (
    sdf.apply(lambda value: {"job_title": value["job_title"]})
    .group_by("job_title")
    # Define a hopping window of 1 hour with 10-minute step
    .hopping_window(duration_ms=timedelta(hours=1), step_ms=timedelta(minutes=10))
    .count()
    .current()
    .apply(
        lambda result: {
            "count": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

sdf = sdf.update(console_sink.print_with_metadata, metadata=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    try:
        app.run(sdf)
    except Exception as e:
        print(f"Application error: {e}")
