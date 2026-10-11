import boto3
import os
import logging
from urllib.parse import unquote_plus
from pathlib import Path


# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------
# AWS CLIENTS
# ---------------------------------------------------------

s3 = boto3.client("s3")
polly = boto3.client("polly")
sns = boto3.client("sns")


# ---------------------------------------------------------
# ENVIRONMENT VARIABLES
# ---------------------------------------------------------

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

POLLY_VOICE_ID = os.environ.get("POLLY_VOICE_ID", "Mia")
POLLY_ENGINE = os.environ.get("POLLY_ENGINE", "standard")


# ---------------------------------------------------------
# READ TEXT FILE FROM S3
# ---------------------------------------------------------

def get_text_from_s3(bucket, key):
    """
    Downloads a TXT file from S3 and returns its contents.
    """

    logger.info(f"Reading file from s3://{bucket}/{key}")

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    file_content = response["Body"].read()

    # utf-8-sig also handles files containing a UTF-8 BOM
    text = file_content.decode("utf-8-sig").strip()

    if not text:
        raise ValueError("The text file is empty.")

    return text


# ---------------------------------------------------------
# CONVERT TEXT TO AUDIO WITH AMAZON POLLY
# ---------------------------------------------------------

def convert_text_to_speech(text):
    """
    Sends text to Amazon Polly and returns MP3 audio bytes.
    """

    logger.info(
        f"Generating audio with Polly. Voice={POLLY_VOICE_ID}, "
        f"Engine={POLLY_ENGINE}"
    )

    response = polly.synthesize_speech(
        Text=text,
        OutputFormat="mp3",
        VoiceId=POLLY_VOICE_ID,
        Engine=POLLY_ENGINE
    )

    audio_stream = response.get("AudioStream")

    if not audio_stream:
        raise RuntimeError("Amazon Polly did not return an audio stream.")

    audio = audio_stream.read()

    logger.info(f"Audio generated successfully. Size: {len(audio)} bytes")

    return audio


# ---------------------------------------------------------
# SAVE MP3 TO S3
# ---------------------------------------------------------

def save_audio_to_s3(bucket, key, audio):
    """
    Saves MP3 audio into S3.
    """

    logger.info(f"Saving audio to s3://{bucket}/{key}")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=audio,
        ContentType="audio/mpeg"
    )

    logger.info("Audio saved successfully.")


# ---------------------------------------------------------
# SEND SNS NOTIFICATION
# ---------------------------------------------------------

def send_notification(source_file, output_bucket, output_key):
    """
    Sends an SNS notification when the MP3 file is ready.
    """

    if not SNS_TOPIC_ARN:
        logger.info(
            "SNS_TOPIC_ARN is not configured. "
            "Skipping SNS notification."
        )
        return

    subject = "Audio generated successfully"

    message = f"""
Your text file has been converted to audio successfully.

Original file:
{source_file}

Generated audio:
s3://{output_bucket}/{output_key}

Voice:
{POLLY_VOICE_ID}

Engine:
{POLLY_ENGINE}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    logger.info("SNS notification sent successfully.")


# ---------------------------------------------------------
# PROCESS ONE S3 OBJECT
# ---------------------------------------------------------

def process_s3_record(record):
    """
    Processes one S3 event record.
    """

    source_bucket = record["s3"]["bucket"]["name"]

    encoded_key = record["s3"]["object"]["key"]

    source_key = unquote_plus(encoded_key)

    logger.info(
        f"Received S3 event: s3://{source_bucket}/{source_key}"
    )

    # -----------------------------------------------------
    # Ignore anything that is not a TXT file
    # -----------------------------------------------------

    if not source_key.lower().endswith(".txt"):
        logger.info(
            f"File ignored because it is not TXT: {source_key}"
        )
        return {
            "status": "ignored",
            "file": source_key
        }

    # -----------------------------------------------------
    # Extra protection against output files
    # -----------------------------------------------------

    if source_key.startswith(OUTPUT_PREFIX):
        logger.info(
            f"File ignored because it is inside output directory: "
            f"{source_key}"
        )

        return {
            "status": "ignored",
            "file": source_key
        }

    # -----------------------------------------------------
    # Determine output bucket
    # -----------------------------------------------------

    output_bucket = OUTPUT_BUCKET or source_bucket

    # -----------------------------------------------------
    # Generate MP3 filename
    # -----------------------------------------------------

    file_name = Path(source_key).stem

    output_key = f"{OUTPUT_PREFIX}{file_name}.mp3"

    logger.info(
        f"Output will be saved at "
        f"s3://{output_bucket}/{output_key}"
    )

    # -----------------------------------------------------
    # Read text
    # -----------------------------------------------------

    text = get_text_from_s3(
        source_bucket,
        source_key
    )

    logger.info(f"Text length: {len(text)} characters")

    # -----------------------------------------------------
    # Polly
    # -----------------------------------------------------

    audio = convert_text_to_speech(text)

    # -----------------------------------------------------
    # Save MP3
    # -----------------------------------------------------

    save_audio_to_s3(
        output_bucket,
        output_key,
        audio
    )

    # -----------------------------------------------------
    # SNS notification
    # -----------------------------------------------------

    send_notification(
        source_file=f"s3://{source_bucket}/{source_key}",
        output_bucket=output_bucket,
        output_key=output_key
    )

    return {
        "status": "success",
        "source": f"s3://{source_bucket}/{source_key}",
        "output": f"s3://{output_bucket}/{output_key}"
    }


# ---------------------------------------------------------
# LAMBDA HANDLER
# ---------------------------------------------------------

def lambda_handler(event, context):
    """
    Main AWS Lambda entry point.
    """

    logger.info("Lambda execution started.")
    logger.info(f"Number of records: {len(event.get('Records', []))}")

    results = []

    try:

        records = event.get("Records", [])

        if not records:
            raise ValueError("No S3 records were found in the event.")

        for record in records:

            event_source = record.get("eventSource")

            if event_source != "aws:s3":
                logger.warning(
                    f"Unsupported event source: {event_source}"
                )
                continue

            result = process_s3_record(record)

            results.append(result)

        logger.info("Lambda execution completed successfully.")

        return {
            "statusCode": 200,
            "message": "Processing completed",
            "results": results
        }

    except Exception as error:

        logger.exception(
            f"Error processing Lambda execution: {str(error)}"
        )

        raise