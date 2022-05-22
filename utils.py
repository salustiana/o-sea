import os
import csv
import time
from client import ApiClient
from concurrent.futures import ThreadPoolExecutor

THREAD_OFFSET = 0.5

def save_wallet_assets(wallet, api_client, file_path, limit_requests=1):
    for assets_list in api_client.get_wallet_assets(
        wallet, limit_requests=limit_requests
    ):
        write_nfts_to_file(
            nfts=assets_list,
            path=file_path,
            fieldnames=api_client.nft_fields,
        )

def save_wallet_transactions(wallet, api_client, file_path, limit_requests=1):
    for wal_hist_list in api_client.get_wallet_transactions(
        wallet, limit_requests=limit_requests
    ):
        write_transactions_to_file(
            transactions=wal_hist_list,
            path=file_path,
            fieldnames=api_client.transaction_fields,
        )

def write_transactions_to_file(transactions, path, fieldnames):
    with open(path, 'a') as f:
        tr_writer = csv.DictWriter(f, fieldnames=fieldnames)
        tr_writer.writerows(transactions)

def write_nfts_to_file(nfts, path, fieldnames):
    with open(path, 'a') as f:
        nft_writer = csv.DictWriter(f, fieldnames=fieldnames)
        nft_writer.writerows(nfts)

def get_and_write_data(
    api_key,
    slugs,
    get_collection_nfts_request_limit=1,
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
        info_path = os.path.join(output_dir, slug, 'info.csv')
        nft_data_path = os.path.join(output_dir, slug, 'nft_data.csv')
        collection_sales_path = os.path.join(output_dir, slug, 'collection_sales.csv')
        owner_transactions_path = os.path.join(output_dir, slug, 'owner_transactions.csv')
        owner_and_seller_nfts_path = os.path.join(output_dir, slug, 'owner_and_seller_nfts.csv')

        # write csv headers
        with open(info_path, 'a') as f:
            info_writer = csv.DictWriter(f, fieldnames=api_client.col_fields)
            info_writer.writeheader()

        with open(nft_data_path, 'a') as f:
            data_writer = csv.DictWriter(f, fieldnames=api_client.data_fields)
            data_writer.writeheader()

        with open(collection_sales_path, 'a') as f:
            tr_writer = csv.DictWriter(f, fieldnames=api_client.transaction_fields)
            tr_writer.writeheader()

        with open(owner_transactions_path, 'a') as f:
            tr_writer = csv.DictWriter(f, fieldnames=api_client.transaction_fields)
            tr_writer.writeheader()

        with open(owner_and_seller_nfts_path, 'a') as f:
            nft_writer = csv.DictWriter(f, fieldnames=api_client.nft_fields)
            nft_writer.writeheader()

        # get info for this collection
        col_info = api_client.get_collection_info(slug)
        # write info to csv file
        with open(info_path, 'a') as f:
            info_writer = csv.DictWriter(f, fieldnames=api_client.col_fields)
            info_writer.writerow(col_info)

        # save a list of nft data for this collection
        with open(nft_data_path, 'a') as f:
            data_writer = csv.DictWriter(f, fieldnames=api_client.data_fields)
            for data_list in api_client.get_col_assets_data(
                slug, limit_requests=get_collection_nfts_request_limit
            ):
                data_writer.writerows(data_list)

        # get and write the collection sales to a csv file
        for sales_list in api_client.get_collection_sales(
            slug, limit_requests=get_collection_sales_request_limit
        ):
            write_transactions_to_file(
                transactions=sales_list,
                path=collection_sales_path,
                fieldnames=api_client.transaction_fields,
            )

        # get a list of owners for this collection
        # remove duplicate owners, if any
        col_owners = set()
        with open(nft_data_path, 'r') as f:
            data_reader = csv.DictReader(f)
            for line in data_reader:
                col_owners.add(line["owner"])

        # add the sellers from sales file
        owners_and_sellers = col_owners.copy()
        with open(collection_sales_path, 'r') as f:
            sales_reader = csv.DictReader(f)
            for line in sales_reader:
                owners_and_sellers.add(line["seller"])

        # for these sellers and owners, get a list
        # of their nfts and save them to a csv file
        with ThreadPoolExecutor(max_workers=api_client.RATE) as executor:
            for wallet in owners_and_sellers:
                # offset the threads
                time.sleep(THREAD_OFFSET)
                executor.submit(
                    save_wallet_assets,
                    wallet=wallet,
                    api_client=api_client,
                    limit_requests=get_wallet_nfts_request_limit,
                    file_path=owner_and_seller_nfts_path,
                )

        # get the transaction histories for the
        # collection owners and save them to a csv file
        with ThreadPoolExecutor(max_workers=api_client.RATE) as executor:
            for wallet in col_owners:
                # offset the threads
                time.sleep(THREAD_OFFSET)
                executor.submit(
                    save_wallet_transactions,
                    wallet=wallet,
                    api_client=api_client,
                    limit_requests=get_wallet_transactions_request_limit,
                    file_path=owner_transactions_path,
                )
