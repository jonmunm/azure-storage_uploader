import asyncio
import os
import json
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '../env.env')
load_dotenv(dotenv_path)

# --- CONFIGURACIÓN ---
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
CONTAINERS = json.loads(os.getenv('CONTAINERS', '[]'))

async def _delete_blob(blob_name, container_client, semaphore):
    async with semaphore:
        await container_client.delete_blob(blob_name)

async def _clean_container(container_name, service_client, semaphore):  
    container_client = service_client.get_container_client(container_name)
    
    # Listamos todos los blobs en el contenedor
    tasks = []
    async for blob in container_client.list_blobs():
        tasks.append(_delete_blob(blob.name, container_client, semaphore))
    
    if not tasks:
        return

    await asyncio.gather(*tasks)
    print(f"✅ Finished: {container_name}")

async def command():
    print(f"🚀 Starting cleaning: {CONTAINERS}")
    
    credential = DefaultAzureCredential()
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    
    # Usamos el ServiceClient para movernos entre contenedores
    async with BlobServiceClient(account_url, credential=credential) as service_client:
        # Semáforo para no exceder límites de conexiones simultáneas
        semaphore = asyncio.Semaphore(100) 
        
        # Creamos una tarea por cada contenedor
        container_tasks = [
            _clean_container(container, service_client, semaphore) 
            for container in CONTAINERS
        ]
        
        await asyncio.gather(*container_tasks)
    
    await credential.close()
    print("🏁 All containers cleaned.")