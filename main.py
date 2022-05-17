from utils import get_and_write_data

if __name__ == '__main__':

    slugs = [
        "neotokyo-citizens"
    ]

    get_and_write_data(
        api_key="",
        slugs=slugs,
        get_owners_request_limit=1,
        get_wallet_transactions_request_limit=1,
        get_wallet_nfts_request_limit=1,
        get_collection_sales_request_limit=None,
        output_dir='./d-results',
    )

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
