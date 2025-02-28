# !pip install elasticsearch boto3

import boto3
import json
from elasticsearch import Elasticsearch

# AWS Credentials and Region
AWS_ACCESS_KEY_ID = "secret"
AWS_SECRET_ACCESS_KEY = "+secret"
AWS_REGION = "us-west-2"
MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"  # or "anthropic.claude-3-5-sonnet-20241022-v2:0"

# Elasticsearch Cloud Configuration
cloud_id = "My_deployment:secret"
es_username = "secret"
es_password = "secret"

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
        print(f"Error querying Bedrock: {e}")
        return None

def generate_user_profile(customer_id, cloud_id, es_username, es_password, model_id, aws_access_key_id, aws_secret_access_key, aws_region):
    """Generates a user profile string using Elasticsearch and Bedrock."""
    try:
        es = Elasticsearch(
            cloud_id=cloud_id,
            http_auth=(es_username, es_password)
        )

        query = {
            "query": {
                "term": {
                    "customer_id": customer_id
                }
            },
            "size": 2000,
            "sort": [{"date": {"order": "asc"}}]
        }

        response = es.search(index="payment_history", body=query)
        hits = response["hits"]["hits"]

        grouped_data = []
        for hit in hits:
            source = hit["_source"]
            grouped_data.append({
                "date": source["date"],
                "merchant_category": source["merchant_category"],
                "merchant": source["merchant"],
                "amount": source["amount"]
            })

        payment_history_str = json.dumps(grouped_data, indent=2)

        bedrock_prompt = f"""
with the transaction history of the user, data provided below:
{payment_history_str}
i would like to profile this user with 10 categorical keywords.
"""

        bedrock_response = query_bedrock(bedrock_prompt, model_id, aws_access_key_id, aws_secret_access_key, aws_region)

        if bedrock_response:
            keywords = []
            lines = bedrock_response.split('\n')
            for line in lines:
                if ". **" in line:
                    keyword = line.split(". **")[1].split("**")[0]
                    keywords.append(keyword)
            return ",".join(keywords)
        else:
            return "Failed to get response from Bedrock."
    except Exception as e:
        return "Failed to retrieve payment history from Elasticsearch Cloud."

def search_elasticsearch_by_keywords(keywords, cloud_id, es_username, es_password):
    """Searches Elasticsearch for documents matching the given keywords."""
    try:
        es = Elasticsearch(
            cloud_id=cloud_id,
            http_auth=(es_username, es_password)
        )

        query = {
            "query": {
                "multi_match": {
                    "query": keywords,
                    "fields": ["text"],
                    "operator": "or"
                }
            }
        }

        response = es.search(index="cna_news", body=query)
        hits = response["hits"]["hits"]

        results = []
        for hit in hits:
            results.append(hit["_source"]["text"])
        return results

    except Exception as e:
        print(f"Error searching Elasticsearch: {e}")
        return []


def generate_user_profile2(customer_id):
    return generate_user_profile(
        customer_id,
        cloud_id,
        es_username,
        es_password,
        MODEL_ID,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION,
    )

def search_elasticsearch_by_keywords2(keywords):
    return search_elasticsearch_by_keywords(keywords, cloud_id, es_username, es_password)

def main():
    customer_id = "71cb1979"
    profile_string = generate_user_profile(
        customer_id,
        cloud_id,
        es_username,
        es_password,
        MODEL_ID,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION,
    )

    if profile_string and "Failed" not in profile_string:
        keywords = profile_string
        search_results = search_elasticsearch_by_keywords(keywords, cloud_id, es_username, es_password)
        for result in search_results:
            print(result)
    else:
        print("Failed to generate user profile.")

if __name__ == "__main__":
    main()