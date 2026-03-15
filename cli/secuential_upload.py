import asyncio
import os
import uuid
import time
import pandas
import numpy
import io
import json
import csv
from datetime import datetime, timezone
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from azure.monitor.ingestion.aio import LogsIngestionClient

BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = f'{BASE_DIR}/env.env'
load_dotenv(dotenv_path)

STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
EXCEL_BASE_PATH = os.environ.get("EXCEL_BASE_PATH")
RUNS_BASE_PATH = os.environ.get("RUNS_BASE_PATH")

USE_AZURE_LOGS = os.environ.get("USE_AZURE_LOGS") is not None
if USE_AZURE_LOGS:
    LOGS_DCE_URI = os.environ.get("LOGS_DCE_URI")
    DCR_IMMUTABLE_ID = os.environ.get("DCR_IMMUTABLE_ID")
    STREAM_NAME = os.environ.get("STREAM_NAME")

async def log_event_to_azure(client, sender, message):
    """Envía un log asíncrono a Log Analytics."""

    body = [
        {
            "TimeGenerated": datetime.now(timezone.utc).isoformat(),
            "Sender": sender,
            "RawData": message
        }
    ]
    await client.upload(
        rule_id=DCR_IMMUTABLE_ID,
        stream_name=STREAM_NAME,
        logs=body
    )

async def upload_worker(row, container_client, ingestion_client):
    file_source_path = row['path']
    name = file_source_path.split('/')[-1]
    file_id = uuid.uuid4().hex[:6]
    blob_name = f"{name}_{file_id}"
        
    start_time = time.perf_counter()
    start_str = datetime.now().strftime('%H:%M:%S')

    def read_file_sync(path):
        with open(path, 'rb') as f:
            data = f.read()
        df_temp = pandas.read_csv(
            io.BytesIO(data), 
            sep=None, 
            engine='python', 
            usecols=[0], 
            quoting=csv.QUOTE_NONE,
            encoding='utf-8',
            on_bad_lines='skip'
        )
        return data, len(df_temp)
        

    content, row_count = await asyncio.to_thread(read_file_sync, file_source_path)
    file_size = round(len(content) / 1024 / 1024, 2)
        
    blob_client = container_client.get_blob_client(blob_name)
    await blob_client.upload_blob(
        content, 
        overwrite=True,
        max_concurrency=8,
        length=len(content),          
        validate_content=True,
    )
        
    end_time = time.perf_counter()
    duration = end_time - start_time
        
    print(f"✅ Finished: {blob_name} in {duration:.2f}s at {start_str} | {row_count:,} rows | {file_size} MB")

    dct = {
        "path": file_source_path,
        "name": blob_client.url,
        "size": file_size,
        "rows": row_count,
        "start" : start_time,
        "duration" : duration
    } 

    if ingestion_client:
        await log_event_to_azure(ingestion_client, "Secuencial Uploader", json.dumps(dct))
    
    return dct


async def command(container, samples, run_name):

    print(f"Using Azure Custom Logs: {USE_AZURE_LOGS}")
    print(f"Reading from {EXCEL_BASE_PATH}...")
    df_base = pandas.read_excel(f'{BASE_DIR}/{EXCEL_BASE_PATH}')
    skipped = df_base.loc[df_base['skip'],:].shape[0]
    print(f"Skipped {skipped} rows ...")

    df_base = df_base.loc[df_base['skip'] == False,:].reset_index(drop=True)
    idx = numpy.random.randint(low=0, high=df_base.shape[0], size=samples)
    df_sample = df_base.loc[idx, ['path']]

    credential = DefaultAzureCredential()
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

    blob_service_client = BlobServiceClient(
        account_url, 
        credential=credential
    )
    ingestion_client = None
    
    if USE_AZURE_LOGS:
        ingestion_client = LogsIngestionClient(endpoint=LOGS_DCE_URI, credential=credential)
    
    print(f"Starting uploading with {len(df_sample)} samples ...")
    results = []
    
    async with blob_service_client:
        container_client = blob_service_client.get_container_client(container)
        
        if ingestion_client:
            async with ingestion_client:
                for _, row in df_sample.iterrows():
                    result = await upload_worker(row, container_client, ingestion_client)
                    results.append(result)
        else:
            for _, row in df_sample.iterrows():
                result = await upload_worker(row, container_client, None)
                results.append(result)


    run_full_name = f"{run_name}_SEQ_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    run_df = pandas.DataFrame(results)
    run_df.to_excel(f'{BASE_DIR}/{RUNS_BASE_PATH}/{run_full_name}.xlsx')
    
    await credential.close()
    print(f"Sequential uploading finished | {run_df['size'].sum():.2f} MB | {run_df['rows'].sum():,} rows")
