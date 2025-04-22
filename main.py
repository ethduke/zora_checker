import asyncio
import aiohttp
import random
import json
import logging
import config
from utils.proxy_helper import ProxyHandler

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def fetch_zora_data(session, address, proxy_handler, query_template, max_retries=3):
    query = query_template.format(address=address)
    payload = {"query": query}
    headers = {"Content-Type": "application/json"}
    current_proxy_url = proxy_handler.get_initial_proxy()
    retries = 0

    while retries <= max_retries:
        proxy_display = proxy_handler.get_display_proxy(current_proxy_url)
        try:
            async with session.post(config.API_URL, json=payload, headers=headers, proxy=current_proxy_url, timeout=aiohttp.ClientTimeout(total=45)) as response:
                status = response.status
                response_text = await response.text()

                is_rate_limited = (status == 429 or "Ratelimit exceeded please try again after" in response_text)

                if is_rate_limited:
                    # Don't retry internally, signal for batch retry
                    logging.warning(f"Rate Limit Detected (Status {status}) for {address} using {proxy_display}. Will retry at batch level.")
                    return {"address": address, "error": "RATE_LIMITED", "status": status, "last_proxy": proxy_display}

                if status == 200:
                    try:
                        data = await response.json(content_type=None)
                        if data.get('data', {}).get('zoraTokenAllocation', {}).get('totalTokensEarned') is not None:
                            token_amount_str = data.get('data', {}).get('zoraTokenAllocation', {}).get('totalTokensEarned', {}).get('totalTokens', 'N/A')
                            logging.info(f"Success: Address: {address}, Proxy: {proxy_display}, Tokens: {token_amount_str}")
                            return {"address": address, "data": data}
                        elif "errors" in data:
                            # GraphQL errors are now treated as final for this attempt
                            logging.warning(f"GraphQL Error (Status 200) for {address} using {proxy_display}: {data['errors']}, Response: {response_text[:100]}...")
                            return {"address": address, "error": f"GraphQL Error: {data['errors']}", "status": status, "last_proxy": proxy_display}
                        else:
                            logging.warning(f"Unexpected 200 Response Structure for {address} using {proxy_display}: {response_text[:100]}...")
                            return {"address": address, "error": "Unexpected 200 Response Structure", "status": status, "last_proxy": proxy_display}
                    except Exception as json_e:
                        logging.error(f"JSON Decode Error for {address} using {proxy_display}: {json_e}, Response text: {response_text[:100]}...")
                        return {"address": address, "error": f"JSON Decode Error: {json_e}", "status": status, "last_proxy": proxy_display}
                else:
                    # Handle other non-200, non-rate-limit HTTP errors
                    logging.error(f"HTTP Error for {address} using {proxy_display}: Status {status}, Response: {response_text[:100]}...")
                    return {"address": address, "error": f"HTTP Status {status}", "status": status, "last_proxy": proxy_display}

        except asyncio.TimeoutError:
            logging.warning(f"Timeout Error for {address} using {proxy_display}")
            retries += 1
            if retries <= max_retries:
                wait_time = random.uniform(1, 2)
                logging.info(f"Timeout for {address}. Retrying ({retries}/{max_retries}) in {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
                # Get new proxy on timeout retry
                current_proxy_url = proxy_handler.get_new_random_proxy(current_proxy_url) 
                continue
            else:
                logging.error(f"Timeout Max Retries Exceeded for address {address}")
                return {"address": address, "error": "Timeout Max Retries Exceeded", "last_proxy": proxy_display}
        except aiohttp.ClientProxyConnectionError as proxy_e:
            logging.warning(f"Proxy Connection Error for {address} using {proxy_display}: {proxy_e}")
            retries += 1
            if retries <= max_retries:
                wait_time = random.uniform(0.5, 1.5)
                new_proxy_url = proxy_handler.get_new_random_proxy(current_proxy_url)
                proxy_display_new = proxy_handler.get_display_proxy(new_proxy_url)
                logging.info(f"Proxy Error for {address}. Retrying ({retries}/{max_retries}) in {wait_time:.2f}s with new proxy: {proxy_display_new}")
                await asyncio.sleep(wait_time)
                current_proxy_url = new_proxy_url
                continue
            else:
                logging.error(f"Proxy Connection Error Max Retries Exceeded for address {address}")
                return {"address": address, "error": f"Proxy Connection Error: {proxy_e}", "last_proxy": proxy_display}
        except aiohttp.ClientError as client_e:
            logging.error(f"Client Error for {address} using {proxy_display}: {client_e}")
            return {"address": address, "error": f"Client Error: {client_e}", "last_proxy": proxy_display}
        except Exception as e:
            logging.exception(f"Generic Request Failed for {address} using {proxy_display}")
            return {"address": address, "error": f"Unknown Error: {type(e).__name__}", "last_proxy": proxy_display}

    logging.error(f"Max retries loop ended unexpectedly for {address} (likely within fetch_zora_data)")
    return {"address": address, "error": "Max retries loop ended unexpectedly", "last_proxy": proxy_display}


async def process_results(all_results, output_json_file):
    # --- Process results, calculate sum, and save to JSON ---
    success_count = 0
    error_count = 0
    total_tokens_sum = 0.0
    results_dict = {}

    logging.info("Processing final results...")
    for r in all_results:
        address = r.get("address")
        if 'data' in r and 'error' not in r and address:
            try:
                token_data = r.get('data', {}).get('data', {}).get('zoraTokenAllocation', {}).get('totalTokensEarned', {})
                if token_data and 'totalTokens' in token_data:
                    token_amount = token_data['totalTokens']
                    if token_amount is not None:
                        try:
                            token_amount_float = float(token_amount)
                            results_dict[address] = token_amount_float
                            total_tokens_sum += token_amount_float
                            success_count += 1
                        except (ValueError, TypeError):
                            logging.warning(f"Could not convert token amount '{token_amount}' to float for address {address}")
                            error_count += 1
                    else:
                        logging.warning(f"'totalTokens' is None for address {address}")
                        error_count += 1
                else:
                    logging.warning(f"'totalTokensEarned' or 'totalTokens' field missing in successful response data for {address}")
                    error_count += 1
            except Exception as e:
                logging.exception(f"Error processing result data for {address}: {e}")
                error_count += 1
        else:
            if address and isinstance(r, dict) and 'error' in r:
                # Log specific errors encountered during fetching/batch processing
                if r.get("error") != "RATE_LIMITED": # Don't double-log rate limits handled by retry
                     logging.error(f"Processing failed for {address}: {r.get('error', 'Unknown fetch error')}, Status: {r.get('status', 'N/A')}, Last Proxy: {r.get('last_proxy', 'N/A')}")
            else:
                logging.error(f"Processing failed for unknown reason or bad result format: {r}")
            error_count += 1

    logging.info("--- Summary ---")
    logging.info(f"Total addresses processed: {len(all_results)}")
    logging.info(f"Successfully retrieved token data for: {success_count} addresses")
    logging.info(f"Errors/Timeouts/RateLimits/Missing Data: {error_count}") # Adjusted label
    logging.info(f"Total Tokens Sum from successful results: {total_tokens_sum:.6f}")

    try:
        with open(output_json_file, 'w') as f:
            json.dump(results_dict, f, indent=4)
        logging.info(f"Successfully saved address:token data to {output_json_file}")
    except Exception as e:
        logging.exception(f"Error saving results to {output_json_file}: {e}")


async def process_single_batch(session, batch_addresses, proxy_handler, query_template, batch_number, total_batches, initial_retry_delay, max_persistent_retries_per_address):
    """Processes a single batch of addresses, including initial fetch and persistent retries for rate limits."""
    logging.info(f"Processing Batch {batch_number}/{total_batches} (Addresses {batch_addresses[0]}...{batch_addresses[-1]}) - Size: {len(batch_addresses)}...")

    final_batch_results = {} # Stores final result {address: result_dict} for this batch
    addresses_to_retry = set(batch_addresses) # Initially, all addresses need to be attempted
    current_persistent_retry_attempt = 0

    while addresses_to_retry and current_persistent_retry_attempt <= max_persistent_retries_per_address:
        if current_persistent_retry_attempt > 0:
            # It's a retry attempt
            wait_time = initial_retry_delay * current_persistent_retry_attempt
            logging.warning(f"Batch {batch_number}: Rate limit hit for {len(addresses_to_retry)} address(es). Retrying (Attempt {current_persistent_retry_attempt}/{max_persistent_retries_per_address}) in {wait_time:.2f} seconds...")
            await asyncio.sleep(wait_time)

        current_batch_addresses = list(addresses_to_retry) # Addresses to process in this attempt
        tasks = []
        task_to_address = {}
        for address in current_batch_addresses:
            task = asyncio.create_task(fetch_zora_data(session, address, proxy_handler, query_template))
            tasks.append(task)
            task_to_address[task] = address

        results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)

        still_needs_retry = set()
        for i, result in enumerate(results_or_exceptions):
            address = current_batch_addresses[i] 

            if isinstance(result, Exception):
                logging.error(f"Task for address {address} failed with exception during batch processing: {result}")
                final_batch_results[address] = {"address": address, "error": f"Task Exception: {result}"}
            elif isinstance(result, dict):
                if result.get("error") == "RATE_LIMITED":
                    # Still rate limited, mark for next retry round if limit not reached
                    if current_persistent_retry_attempt < max_persistent_retries_per_address:
                         still_needs_retry.add(address)
                    else:
                         # Max retries reached for this address
                         logging.error(f"Batch {batch_number}: Max persistent retries reached for {address}. Marking as failed.")
                         final_batch_results[address] = {"address": address, "error": "Persistent Rate Limit Max Retries Exceeded", "last_proxy": result.get("last_proxy")}
                else:
                    # Success or other error - this address is done for this batch
                    final_batch_results[address] = result
            else:
                # Should not happen if fetch_zora_data always returns dict or raises
                logging.error(f"Unexpected result type for address {address}: {type(result)}. Result: {result}")
                final_batch_results[address] = {"address": address, "error": "Unexpected result type from fetch"}
        
        addresses_to_retry = still_needs_retry # Update the set for the next loop iteration
        current_persistent_retry_attempt += 1

    logging.info(f"Batch {batch_number} finished processing.")
    return list(final_batch_results.values())


async def main():
    api_url = config.API_URL
    addresses_file = config.ADDRESSES_FILE
    proxies_file = config.PROXIES_FILE
    query_file = config.QUERY_FILE
    output_json_file = config.OUTPUT_JSON_FILE
    batch_size = config.BATCH_SIZE
    initial_retry_delay = config.INITIAL_RETRY_DELAY
    max_persistent_retries_per_address = config.MAX_PERSISTENT_RETRIES_PER_ADDRESS

    query_template = config.read_file_content(query_file)
    if query_template is None:
        exit(1)

    addresses = config.read_file_lines(addresses_file)
    proxies = config.read_file_lines(proxies_file)

    if not addresses:
        logging.error(f"No addresses found in {addresses_file}. Exiting.")
        exit(1)

    proxy_handler = ProxyHandler(proxies)
    if not proxies:
        logging.warning(f"No proxies found in {proxies_file}. Using direct connection.")

    all_results = []

    logging.info(f"Using API URL: {api_url}")
    logging.info(f"Total addresses to process: {len(addresses)}")
    logging.info(f"Processing in batches of {batch_size}...")

    current_index = 0
    total_batches = (len(addresses) + batch_size - 1) // batch_size

    async with aiohttp.ClientSession() as session:
        while current_index < len(addresses):
            batch_addresses = addresses[current_index : current_index + batch_size]
            batch_number = (current_index // batch_size) + 1

            batch_results = await process_single_batch(
                session,
                batch_addresses,
                proxy_handler,
                query_template,
                batch_number,
                total_batches,
                initial_retry_delay,
                max_persistent_retries_per_address
            )

            all_results.extend(batch_results)

            # Move to the next batch index
            current_index += len(batch_addresses)

    # Process final results and save to JSON
    await process_results(all_results, output_json_file)

if __name__ == "__main__":
    asyncio.run(main())
