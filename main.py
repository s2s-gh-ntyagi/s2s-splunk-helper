import os
import time
import json
import asyncio
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from dotenv import load_dotenv
import splunklib.client as client
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from datetime import datetime

# Create directories for logs and output files
os.makedirs("logs", exist_ok=True)

# Set up logging with loguru
log_filename = f"logs/splunk_service_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger.add(log_filename, rotation="1 MB", retention="10 days")

# Create a timestamped folder for output JSON files
output_folder = f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
os.makedirs(output_folder, exist_ok=True)

class SplunkService:
    """
    A service class to interact with Splunk.
    """

    def __init__(self):
        """
        Initialize the SplunkService by loading environment variables and connecting to Splunk.
        """
        load_dotenv()  # Load environment variables from .env file

        splunk_url = os.getenv('SPLUNK_URL')
        api_token = os.getenv('SPLUNK_API_TOKEN')

        if not splunk_url:
            raise ValueError("Splunk URL not provided")
        if not api_token:
            raise ValueError("Splunk API token not provided")

        parsed_url = urlparse(splunk_url)

        self.service = client.connect(
            scheme=parsed_url.scheme,
            host=parsed_url.hostname,
            port=parsed_url.port,
            token=api_token
        )
        logger.info("Splunk service created")

    def search(self, query: str, verbose: bool = True, count: int = 0):
        """
        Execute a search query on Splunk and return the results.

        :param query: The search query to execute.
        :param verbose: Whether to log progress information.
        :param count: The number of results to return. Default is 0 (all results).
        :return: A list of dictionaries containing the search results.
        """
        try:
            # Create a search job
            job = self.service.jobs.create(query)
            job_id = job.sid
            logger.info(f"Processing search query: {query}, jobId: {job_id}")

            # Poll until the search is complete
            while not job.is_done():
                job.refresh()

                if verbose:
                    progress = f"{float(job['doneProgress']) * 100:.1f}%"
                    stats = f"-- {job_id} jobId {progress} done"
                    logger.info(stats)

                time.sleep(1)

            # Once the search is done, get the results
            raw_results = job.results(count=count)

            # Log the raw results
            raw_content = raw_results.read()
            logger.debug(f"Raw results: {raw_content[:1000]}...")  # Log first 1000 characters

            # Parse XML results
            root = ET.fromstring(raw_content)
            results_list = []
            for result in root.findall('.//result'):
                result_dict = {}
                for field in result.findall('field'):
                    field_name = field.get('k')
                    field_value = field.find('value/text').text if field.find(
                        'value/text') is not None else None
                    result_dict[field_name] = field_value
                results_list.append(result_dict)

            logger.debug(f"First few results: {json.dumps(results_list[:5], indent=2)}")

            # Cancel the job
            job.cancel()

            return results_list

        except ET.ParseError as e:
            logger.error(f"XML Parse Error: {str(e)}")
            logger.error(f"Raw results causing the error: {raw_content}")
            raise
        except Exception as e:
            logger.error(f"Error during search: {str(e)}")
            if 'raw_content' in locals():
                logger.error(f"Raw results: {raw_content}")
            raise

    def get_indices_and_sourcetypes(self):
        """
        Retrieve all indices and sourcetypes from Splunk.

        :return: A list of dictionaries containing index and sourcetype information.
        """
        query = "| tstats count where index=* by index, sourcetype | fields - count"
        return self.search(query)

    def get_fields_for_sourcetype(self, index: str, sourcetype: str):
        """
        Retrieve all fields for a given sourcetype in a specific index.

        :param index: The index to search.
        :param sourcetype: The sourcetype to search.
        :return: A list of field names.
        """
        query = f'search index="{index}" sourcetype="{sourcetype}"| fields * | fields - _* | transpose | rename column as field | fields field'
        results = self.search(query)
        if results:
            # Extract unique field names from the results
            fields = set()
            for result in results:
                fields.update(result.values())
            return list(fields)
        return []


async def process_batch(splunk_service, batch, executor):
    """
    Process a batch of index-sourcetype combinations asynchronously.

    :param splunk_service: An instance of SplunkService.
    :param batch: A list of dictionaries containing index and sourcetype information.
    :param executor: A ThreadPoolExecutor instance.
    :return: A list of results from processing the batch.
    """
    tasks = []
    for item in batch:
        index = item['index']
        sourcetype = item['sourcetype']
        task = asyncio.get_event_loop().run_in_executor(
            executor, process_item, splunk_service, index, sourcetype)
        tasks.append(task)
    return await asyncio.gather(*tasks)

def process_item(splunk_service, index, sourcetype):
    """
    Process a single index-sourcetype combination.

    :param splunk_service: An instance of SplunkService.
    :param index: The index to search.
    :param sourcetype: The sourcetype to search.
    :return: A dictionary containing the index, sourcetype, and fields.
    """
    logger.info(f"Retrieving fields for index='{index}' sourcetype='{sourcetype}'")
    fields = splunk_service.get_fields_for_sourcetype(index, sourcetype)
    logger.info(f"Found {len(fields)} fields")

    # Save the results to a separate JSON file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_folder}/{index}_{sourcetype}_fields_{timestamp}.json"
    with open(filename, 'w') as f:
        json.dump({"index": index, "sourcetype": sourcetype, "fields": fields}, f, indent=2)
    logger.info(f"Results saved to {filename}")

    return {
        "index": index,
        "sourcetype": sourcetype,
        "fields": fields
    }

async def main():
    """
    Main function to retrieve all indices and sourcetypes, and then retrieve all fields for each sourcetype in each index in batches.

    :return: None
    """
    splunk_service = SplunkService()

    # 1. Retrieve all indices and Log sources
    logger.info("Retrieving all indices and sourcetypes...")
    indices_and_sourcetypes = splunk_service.get_indices_and_sourcetypes()
    logger.info(f"Found {len(indices_and_sourcetypes)} index-sourcetype combinations")

    # 2. Retrieve all fields for each sourcetype in each index in batches of 5
    batch_size = 5
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, len(indices_and_sourcetypes), batch_size):
            batch = indices_and_sourcetypes[i:i+batch_size]
            batch_results = await process_batch(splunk_service, batch, executor)
            results.extend(batch_results)

    # 3. Output the results as JSON
    print(json.dumps(results, indent=2))

    # 4. Optionally, save to a file
    with open(f'{output_folder}/splunk_fields.json', 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to {output_folder}/splunk_fields.json")

if __name__ == "__main__":
    asyncio.run(main())
