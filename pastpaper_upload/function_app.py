import logging 
import azure.functions as func 
from azure.storage.blob import BlobServiceClient, ContentSettings 
from azure.cosmos import CosmosClient 
import hashlib 
import json 
import os 
import uuid 
 
app = func.FunctionApp() 
 
def hash_function(data): 
    selected_values = { 
        "course_code": data.get("course_code"), 
        "pastpaper_type": data.get("pastpaper_type"), 
        "pastpaper_number": data.get("pastpaper_number"), 
        "year": data.get("year"), 
        "tenure": data.get("tenure") 
    } 
    data_str = json.dumps(selected_values, sort_keys=True) 
    data_bytes = data_str.encode('utf-8') 
    sha256_hash = hashlib.sha256() 
    sha256_hash.update(data_bytes) 
    hashed_data = sha256_hash.hexdigest() 
    return hashed_data 
 
@app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION) 
def http_trigger(req: func.HttpRequest) -> func.HttpResponse: 
    logging.info('Processing file upload and metadata.') 
 
    try: 
        # Retrieve metadata from the form data 
        metadata = req.form.get('metadata') 
        if not metadata: 
            return func.HttpResponse("Metadata is required.", status_code=400) 
 
        # Parse the metadata (assuming it's passed as a JSON string) 
        try: 
            metadata = json.loads(metadata) 
            logging.info(f"Parsed metadata: {metadata}")
        except json.JSONDecodeError as e: 
            return func.HttpResponse(f"Error parsing metadata: {str(e)}", status_code=400) 
 
        file = req.files.get('file') 
        if not file: 
            return func.HttpResponse("File is required.", status_code=400) 
        
        logging.info(f"File received: {file.filename}")
 
        # Retrieve the container name from the request
        container_name = req.form.get('container_name')
        if not container_name:
            return func.HttpResponse("Container name is required.", status_code=400)
        
        logging.info(f"Container name: {container_name}")
 
        # Create a unique filename for the PDF 
        file_name = f"{hash_function(metadata)}.pdf" 
        logging.info(f"Generated file name: {file_name}")
 
        # Retrieve connection strings from environment variables 
        blob_connection_string = os.getenv('AzureWebJobsStorage') 
        cosmos_connection_string = os.getenv('CosmosDBConnectionString') 
 
        # Upload the file to Azure Blob Storage 
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=yousuf4594;AccountKey=F3RU+DFlttVsLB4E4XLSyGUE3nATWPiZk2t8f/dGMjcqEvzLY5xv311srh9OJLsdtQjgnRpv0otk+ASt8dMLOA==;EndpointSuffix=core.windows.net")
        blob_container_client = blob_service_client.get_container_client("pastpapers") 
 
        # Upload the file to blob storage 
        blob_client = blob_container_client.get_blob_client(file_name) 
        file_content = file.read()
        logging.info(f"File content length: {len(file_content)} bytes")
        blob_client.upload_blob(file_content, overwrite=True, content_settings=ContentSettings(content_type='application/pdf')) 
 
        # Generate Blob Storage URL 
        file_url_in_blob = blob_client.url 
        logging.info(f"File URL in blob: {file_url_in_blob}")
 
        # Add file URL to the metadata 
        metadata['pdf_url'] = file_url_in_blob 
 
        # Add an 'id' field to the metadata 
        metadata['id'] = str(uuid.uuid4()) 
 
        # Save metadata in Cosmos DB 
        cosmos_client = CosmosClient.from_connection_string("AccountEndpoint=https://yousuf4594.documents.azure.com:443/;AccountKey=dGIeRstSAW30CMoQ388vV9e8wKuZYwC801VCq1X7g1PyUbA3S808TKX9Oo3ytmXEGe5rRCzVEiOmACDbF9Latw==;")
        database = cosmos_client.get_database_client("pastpapers") 
        container = database.get_container_client(container_name)
 
        # Insert metadata into Cosmos DB 
        result = container.create_item(body=metadata)
        logging.info(f"Item created in Cosmos DB: {result}")
 
        return func.HttpResponse(f"File and metadata uploaded successfully.", status_code=200) 
 
    except Exception as e: 
        logging.error(f"Error processing file or metadata: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Error processing file or metadata: {str(e)}", status_code=500)


# import azure.functions as func
# import datetime
# import json
# import logging

# app = func.FunctionApp()

# @app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION)
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     name = req.params.get('name')
#     if not name:
#         try:
#             req_body = req.get_json()
#         except ValueError:
#             pass
#         else:
#             name = req_body.get('name')

#     if name:
#         return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
#     else:
#         return func.HttpResponse(
#              "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
#              status_code=200
#         )
