# Azure Blob Storage Performance Uploader

A high-performance Python utility designed to benchmark and execute file uploads to Azure Blob Storage. It supports both **sequential** and **parallel** execution modes, with integrated observability via **Azure Monitor Custom Logs**.

---

## 🚀 Features
* **Dual Mode:** Choose between sequential or high-concurrency parallel uploads.
* **Smart Sampling:** Randomly selects a subset of files from a manifest for stress testing.
* **Azure Observability:** Direct ingestion to Log Analytics via Data Collection Endpoints (DCE).
* **Async Core:** Built on `asyncio` for maximum I/O throughput.

---

## 🛠 Prerequisites
* Python 3.11+
* Azure CLI (for `DefaultAzureCredential`)
* Active Azure Storage Account

---

## ⚙️ Environment Variables
Configure these variables in your `env.env` file or system environment:

| Variable | Description | Required |
| :--- | :--- | :--- |
| `STORAGE_ACCOUNT_NAME` | Name of your Azure Storage Account | Yes |
| `EXCEL_BASE_PATH` | Path to the `.xlsx` manifest file | Yes |
| `USE_AZURE_LOGS` | Set to any value to enable Custom Logs | No |
| `LOGS_DCE_URI` | The URI of your Data Collection Endpoint | If Logs enabled |
| `DCR_IMMUTABLE_ID` | The Immutable ID of your DCR | If Logs enabled |
| `STREAM_NAME` | The name of the custom log stream | If Logs enabled |

---

## 📋 Usage

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Running the Uploader

The tool is invoked via utils.py using the following parameters:

* ```container```: The target Azure Blob container.
* ```samples```: Number of random files to pick from the manifest.
* ```run-name```: A custom label for the execution.

### 3. Parallel Upload

```bash
python utils.py parallel-upload --container raw-data --samples 10 --run-name test-01
```

### 3. Sequential Upload

```bash
python utils.py sequential-upload --container raw-data --samples 10 --run-name test-01
```

### 📊 Monitoring

If ```USE_AZURE_LOGS``` is enabled, the tool sends real-time metrics to Log Analytics, including:

* *Duration*: Precise upload time per file.
* *Throughput*: File size and row counts (optimized via fast-count).
* *Execution Metadata*: Start times and Blob URIs.

### 📝 Manifest Format

The tool expects an Excel file with the following columns:

* *path*: Local path to the file.
* *skip*: Boolean (True/False) to filter out files.