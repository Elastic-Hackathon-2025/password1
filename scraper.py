# !pip install boto3
# !pip install requests elasticsearch
# !pip install telethon elasticsearch schedule python-dotenv
# !pip install telethon elasticsearch python-dotenv
# !pip install transformers torch

import asyncio
import nest_asyncio
import time
from telethon import TelegramClient, events
from elasticsearch import Elasticsearch
from datetime import datetime
import boto3
import json

nest_asyncio.apply()

# AWS Credentials and Region
AWS_ACCESS_KEY_ID = "secret"
AWS_SECRET_ACCESS_KEY = "+secret"
AWS_REGION = "us-west-2"
MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"  # or "anthropic.claude-3-5-sonnet-20241022-v2:0"

api_id = 23087224
api_hash = "secret"
cloud_id = "My_deployment:secret"
es_username = "secret"
es_password = "#secret"
channel_name = "cnalatest"

# Elasticsearch client
es = Elasticsearch(
    cloud_id=cloud_id,
    basic_auth=(es_username, es_password),
)

client = TelegramClient('anon', api_id, api_hash)

def query_bedrock(prompt, model_id, access_key, secret_key, region):
    """Queries Amazon Bedrock with the Messages API."""
    try:
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 400,
            "temperature": 0.5,
        })

        accept = "application/json"
        content_type = "application/json"

        response = bedrock_runtime.invoke_model(
            body=body, modelId=model_id, accept=accept, contentType=content_type,
        )
        response_body = json.loads(response.get("body").read())
        return response_body["content"][0]["text"]

    except Exception as e:
        print(f"{datetime.now()} Error querying Bedrock: {e}")
        return None

async def scrape_and_send():
    await client.start()

    try:
        channel = await client.get_entity(channel_name)
    except ValueError:
        print(f"{datetime.now()} Channel '{channel_name}' not found.")
        return

    async for message in client.iter_messages(channel, limit=1000):
        if message.text:
            data = {
                "text": message.text,
                "timestamp": message.date.isoformat(),
                "source": "CNA Telegram",
            }
            try:
                # Classify the message using Bedrock
                #categories_str = ", ".join(categories_descriptions.keys())
                prompt = f"Classify the following text into 3 news genre. Text: '{message.text}'. return as a string, joined by ' ' in lowercase."
                bedrock_response = query_bedrock(prompt, MODEL_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

                if bedrock_response:
                    # Extract the classified category
                    classified_category = bedrock_response.strip()
                    if True:
                        data['category'] = classified_category
                        es.index(index="cna_news", document=data, id=message.id)
                        print(f"{datetime.now()} Indexed message: {data['timestamp']}, Category: {data['category']}")
                    else:
                        print(f"{datetime.now()} Bedrock returned an invalid category: {classified_category}")
                else:
                    print(f"{datetime.now()} Failed to get response from Bedrock.")

            except Exception as e:
                print(f"{datetime.now()} Error indexing message: {e}")
    await client.disconnect()

async def main():
    await scrape_and_send()

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            print(f"{datetime.now()} An error occurred: {e}")
        time.sleep(300)