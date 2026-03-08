import asyncio
import os
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '../env.env')
load_dotenv(dotenv_path)

STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")

async def _delete_blob(blob_name, container_client, semaphore):
    async with semaphore:
        await container_client.delete_blob(blob_name)

async def _clean_container(container_name, service_client, semaphore):  
    container_client = service_client.get_container_client(container_name)
    
    tasks = []
    async for blob in container_client.list_blobs():
        tasks.append(_delete_blob(blob.name, container_client, semaphore))
    
    if not tasks:
        return

    await asyncio.gather(*tasks)
    print(f"✅ Finished: {container_name}")

async def command(container):
    print(f"🚀 Starting cleaning: {container}")
    
    credential = DefaultAzureCredential()
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    
    async with BlobServiceClient(account_url, credential=credential) as service_client:
        semaphore = asyncio.Semaphore(100)       
        await _clean_container(container, service_client, semaphore) 
    
    await credential.close()
    print("🏁 Container cleaning finished.")