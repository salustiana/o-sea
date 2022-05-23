from utils import get_and_write_data

if __name__ == '__main__':

    slugs = [
        "rtfkt-capsule-space-drip-1-2",
    ]

    get_and_write_data(
        api_key="",
        slugs=slugs,
        get_collection_nfts_request_limit=1,
        get_listings_request_limit=50,
        get_collection_sales_request_limit=1,
        get_wallet_transactions_request_limit=1,
        get_wallet_nfts_request_limit=1,
        output_dir='./i-results',
    )

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
