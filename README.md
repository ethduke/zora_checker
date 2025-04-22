# Zora Token Checker (Unofficial)

This script fetches Zora token allocation data for a list of Ethereum addresses. It uses proxies to handle potential rate limits. **The data might not be perfectly accurate.**

## How it Works

1.  Reads addresses from `data/addresses.txt` (one Ethereum address per line, e.g., `0x...`).
2.  Reads proxies from `data/proxies.txt` (if any) in format `http://user:pass@host:port` or `http://host:port`.
3.  Uses the GraphQL query below to ask the Zora API about token allocations for each address.
4.  Handles rate limits by retrying with different proxies.
5.  Saves the results (address: token_amount) to `data/output.json`.

## Configuration

Edit `config.py` to change file paths, API URL, batch size, and retry settings.

## GraphQL Query Used (`data/query.graphql`)

```graphql
query MyQuery {{
  zoraTokenAllocation(
    identifierWalletAddresses: "{address}"
    zoraClaimContractEnv: PRODUCTION
  ) {{
    totalTokensEarned {{
      totalTokens
    }}
  }}
}}
```

## Running

Make sure you have the requirements:

```bash
pip install -r requirements.txt
```

Then just run:

```bash
python main.py 