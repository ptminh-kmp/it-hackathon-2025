import boto3
import os
import json
import traceback

BEDROCK_AGENT_RUNTIME_CLIENT = None
KNOWLEDGE_BASE_ID = os.environ.get('KNOWLEDGE_BASE_ID')
AWS_REGION_KB = os.environ.get('AWS_REGION', 'us-east-1')

def get_bedrock_agent_runtime_client():
    global BEDROCK_AGENT_RUNTIME_CLIENT
    if BEDROCK_AGENT_RUNTIME_CLIENT is None:
        print(f"KB Client: Initializing Bedrock Agent Runtime client for region: {AWS_REGION_KB}")
        try:
            BEDROCK_AGENT_RUNTIME_CLIENT = boto3.client(
                service_name='bedrock-agent-runtime',
                region_name=AWS_REGION_KB
            )
        except Exception as e_client:
            print(f"ERROR KB Client: Failed to initialize Bedrock Agent Runtime client: {e_client}")
            traceback.print_exc()
            raise 
    return BEDROCK_AGENT_RUNTIME_CLIENT

def retrieve_from_knowledge_base(query_text: str, number_of_results: int = 3):
    if not KNOWLEDGE_BASE_ID:
        print("ERROR KB Client: KNOWLEDGE_BASE_ID env var not set.")
        return [] 

    try:
        client = get_bedrock_agent_runtime_client()
    except Exception as client_e:
        print(f"ERROR KB Client: Cannot get Bedrock Agent Runtime client: {client_e}")
        return []
    
    print(f"KB Client: Retrieving from KB ID '{KNOWLEDGE_BASE_ID}' with query (first 300 chars): '{query_text[:300]}...'")
    try:
        response = client.retrieve(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrievalQuery={'text': query_text},
            retrievalConfiguration={
                'vectorSearchConfiguration': {'numberOfResults': number_of_results}
            }
        )
        retrieval_results = response.get('retrievalResults', [])
        print(f"DEBUG KB Client: Retrieved {len(retrieval_results)} chunks from KB.")
        return retrieval_results
    except Exception as e:
        print(f"ERROR KB Client: Exception during KB retrieval for KB ID '{KNOWLEDGE_BASE_ID}': {e}")
        traceback.print_exc()
        return []