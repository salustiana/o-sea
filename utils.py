import os
import csv
import time
from threading import RLock
from concurrent.futures import ThreadPoolExecutor

from client import ApiClient

THREAD_OFFSET = 0.5
rlock = RLock()

def write_things_to_file(things, path, fieldnames):
    with rlock:
        with open(path, 'a') as f:
            thing_writer = csv.DictWriter(f, fieldnames=fieldnames)
            thing_writer.writerows(things)

def save_wallet_assets(wallet, api_client, file_path, limit_requests=1):
    try:
        for assets_list in api_client.get_wallet_assets(
            wallet, limit_requests=limit_requests
        ):
            write_things_to_file(
                things=assets_list,
                path=file_path,
                fieldnames=api_client.nft_fields,
            )
    except Exception as e:
        print(e)

def save_wallet_transactions(wallet, api_client, file_path, limit_requests=1):
    try:
        for wal_hist_list in api_client.get_wallet_transactions(
            wallet, limit_requests=limit_requests
        ):
            write_things_to_file(
                things=wal_hist_list,
                path=file_path,
                fieldnames=api_client.transaction_fields,
            )
    except Exception as e:
        print(e)

def save_asset_listings(
    contr_addr,
    token_id,
    asset_url,
    image_url,
    api_client,
    file_path,
):
    try:
        listings = api_client.get_asset_listings(
            contr_addr, token_id
        )
        for listing in listings:
            listing["asset_url"] = asset_url
            listing["image_url"] = image_url
        write_things_to_file(
            things=listings,
            path=file_path,
            fieldnames=api_client.listing_fields,
        )
    except Exception as e:
        print(e)

def get_and_write_data(
    api_key,
    slugs,
    get_collection_nfts_request_limit=1,
    get_listings_request_limit=1,
    get_wallet_transactions_request_limit=1,
    get_wallet_nfts_request_limit=1,
    get_collection_sales_request_limit=1,
    output_dir='./results',
):
    """ This function performs all the requested data
    extraction, and writes the results to csv files
    within the specified output dir.

    All limits can be set to 0 to skip that data,
    or to None to fetch everything available. Do this
    with care, since collections can have up to 10k
    nfts, and wallet_transactions and wallet_nfts
    grow exponentially.

    - get_collection_nfts_request_limit: limits the
    ammount of requests performed when getting the list
    of nfts for a collection. 50 nfts are returned
    for each request.

    - get_listings_request_limit: limits the
    ammount of requests performed when getting the
    listings for the collection nfts. Every request
    fetches all active listings for an nft.

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
        listings_path = os.path.join(output_dir, slug, 'listings.csv')
        collection_sales_path = os.path.join(output_dir, slug, 'collection_sales.csv')
        owner_transactions_path = os.path.join(output_dir, slug, 'owner_transactions.csv')
        owner_and_seller_nfts_path = os.path.join(output_dir, slug, 'owner_and_seller_nfts.csv')

        # write csv headers
        for path, fields in [
            (info_path, api_client.col_fields),
            (nft_data_path, api_client.data_fields),
            (listings_path, api_client.listing_fields),
            (collection_sales_path, api_client.transaction_fields),
            (owner_transactions_path, api_client.transaction_fields),
            (owner_and_seller_nfts_path, api_client.nft_fields),
        ]:
            with open(path, 'a') as f:
                thing_writer = csv.DictWriter(f, fieldnames=fields)
                thing_writer.writeheader()

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

        # get the list of nfts for this collection
        assets = list()
        with open(nft_data_path, 'r') as f:
            data_reader = csv.DictReader(f)
            for line in data_reader:
                assets.append(
                    {
                        "asset_url": line["asset_url"],
                        "image_url": line["image_url"],
                        "contract_address": line["contract_address"],
                        "token_id": line["token_id"],
                    }
                )

        # get the listings for the
        # collection nfts and save them to a csv file
        with ThreadPoolExecutor(max_workers=api_client.RATE) as executor:
            for i, asset in enumerate(assets):
                if i+1 > get_listings_request_limit:
                    break
                # offset the threads
                time.sleep(THREAD_OFFSET)
                executor.submit(
                    save_asset_listings,
                    contr_addr=asset["contract_address"],
                    token_id=asset["token_id"],
                    asset_url=asset["asset_url"],
                    image_url=asset["image_url"],
                    api_client=api_client,
                    file_path=listings_path,
                )

        # get and write the collection sales to a csv file
        for sales_list in api_client.get_collection_sales(
            slug, limit_requests=get_collection_sales_request_limit
        ):
            write_things_to_file(
                things=sales_list,
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
