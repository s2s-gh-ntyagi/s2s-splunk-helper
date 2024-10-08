# Splunk Service Dockerized Application

This project provides a Dockerized Python application to interact with Splunk, retrieve indices and sourcetypes, and extract fields for each sourcetype. The results are saved as JSON files in a timestamped output directory.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. **Clone the Repository**

   ```bash
   git clone https://github.com/s2s-gh-ntyagi/s2s-splunk-helper.git
   cd s2s-splunk-helper
   ```
   
2. **Environment Variables**

   Create a .env file in the root directory with the following variables:

    ``` bash
   SPLUNK_URL=your_splunk_url
   SPLUNK_API_TOKEN=your_api_token
   ```

3. **Install Dependencies**
 
   Ensure you have a requirements.txt file with all necessary Python dependencies.

## Usage

1. **Build the Docker Image**

    ```bash
   docker compose build
    ```
   
2. **Run the Docker Image**

    ```bash
   docker compose up
    ```
   This command will start the application, which will connect to Splunk, perform the queries, and save the results

## Output

**Logs**: Stored in the ***logs*** directory.

**Results**: JSON files are saved in a timestamped directory under ***output***.

## Accessing Results
After running the application, you can find the logs and output JSON files in the respective directories on your local file system.

## Stopping and Removing the Container

To stop the running container, press Ctrl+C in the terminal where the container is running.

To remove the stopped container, run:

```bash
    docker compose down
```

    