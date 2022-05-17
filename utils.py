import os
import csv
from client import ApiClient
from concurrent.futures import ThreadPoolExecutor

MAX_PARALLEL_REQUESTS = 2

def get_owners(slug, api_client, limit_requests=1):
    try:
        col_owners = api_client.get_collection_owners(
            slug, limit_requests=limit_requests,
        )
    except Exception as e:
        print(f"slug: {slug}")
        print(e)
    else:
        return col_owners

def get_wallet_assets(wallet, api_client, output_list, limit_requests=1):
    try:
        wal_assets = api_client.get_wallet_assets(
            wallet, limit_requests=limit_requests,
        )
    except Exception as e:
        print(f"wallet: {wallet}")
        print(e)
    else:
        output_list.extend(wal_assets)

def get_wallet_transactions(wallet, api_client, output_list, limit_requests=1):
    try:
        wal_hist = api_client.get_wallet_transactions(
            wallet, limit_requests=limit_requests,
        )
    except Exception as e:
        print(f"wallet: {wallet}")
        print(e)
    else:
        output_list.extend(wal_hist)

def get_collection_sales(slug, api_client, output_list, limit_requests=1):
    try:
        col_sales = api_client.get_collection_sales(
            slug, limit_requests=limit_requests,
        )
    except Exception as e:
        print(f"slug: {slug}")
        print(e)
    else:
        output_list.extend(col_sales)

def write_transactions_to_file(transactions, path):
    with open(path, 'w') as f:
        tr_writer = csv.DictWriter(f, fieldnames=[
            "asset_url",
            "contract_address",
            "token_id",
            "collection",
            "seller",
            "buyer",
            "price",
            "coin",
            "price_usd",
            "timestamp",
        ])
        tr_writer.writeheader()
        tr_writer.writerows(transactions)

def get_and_write_data(
    api_key,
    slugs,
    get_owners_request_limit=1,
    get_wallet_transactions_request_limit=1,
    get_wallet_nfts_request_limit=1,
    get_collection_sales_request_limit=1,
    output_dir='./results',
):
    """ This function performs all the requested data
    extraction, and writes the results to csv files
    within the specified output dir.

    - get_owners_request_limit: limits the ammount of
    requests performed when getting a list of owners
    for a collection. 50 owners are returned for each
    request.

    - get_wallet_transactions_request_limit: limits
    the ammount of requests performed when getting a
    list of transactions for a single owner.
    300 transactions are returned for each request.

    - get_wallet_nfts_request_limit: limits the
    ammount of requests performed when getting a
    list of nfts owned by an owner or seller.
    300 transactions are returned for each request.

    - get_collection_sales_request_limit: limits the
    ammount of requests performed when getting a list
    of sales regarding a collection. 300 sales are
    returned for each request. """
        
    api_client = ApiClient(api_key=api_key)

    for slug in slugs:
        os.makedirs(os.path.join(output_dir, slug), exist_ok=True)

        # get a list of owners for this collection
        # remove duplicate owners, if any
        col_owners = list(set(get_owners(
            slug,
            api_client=api_client,
            limit_requests=get_owners_request_limit,
        )))

        # get the sales referring to this collection
        col_sales = list()
        get_collection_sales(
            slug,
            api_client=api_client,
            output_list=col_sales,
            limit_requests=get_collection_sales_request_limit,
        )
        # write the collection sales to a csv file
        write_transactions_to_file(
            transactions=col_sales,
            path=os.path.join(output_dir, slug, 'collection_sales.csv'),
        )

        # get the sellers from the previous list
        sellers = [sale["seller"] for sale in col_sales]
        owners_and_sellers = list(set(col_owners + sellers))

        # for these sellers and owners,
        # get a list of their nfts
        owner_and_seller_assets = list()
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_REQUESTS) as executor:
            for wallet in owners_and_sellers:
                executor.submit(
                    get_wallet_assets,
                    wallet=wallet,
                    api_client=api_client,
                    limit_requests=get_wallet_nfts_request_limit,
                    output_list=owner_and_seller_assets,
                )


        # write those nfts to a csv file
        with open(
            os.path.join(output_dir, slug, 'owner_and_seller_nfts.csv'), 'w'
        ) as f:
            nft_writer = csv.DictWriter(f, fieldnames=[
                "contract_address",
                "token_id",
                "collection",
            ])

            nft_writer.writeheader()
            nft_writer.writerows(owner_and_seller_assets)

        # get the transaction history for the collection owners
        owner_transactions = list()
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_REQUESTS) as executor:
            for wallet in col_owners:
                executor.submit(
                    get_wallet_transactions,
                    wallet=wallet,
                    api_client=api_client,
                    limit_requests=get_wallet_transactions_request_limit,
                    output_list=owner_transactions,
                )
        # write those transactions to a csv file
        write_transactions_to_file(
            transactions=owner_transactions,
            path=os.path.join(output_dir, slug, 'owner_transactions.csv'),
        )