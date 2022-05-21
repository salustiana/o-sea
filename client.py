import time
import requests
from ratelimit import limits, sleep_and_retry

class OSAPIError(Exception):
    pass

""" API rate limit: 4/sec ??????"""


class ApiClient:
    API_URL = "https://api.opensea.io/api/v1/"
    ASSETS_URL = API_URL + "assets/"
    EVENTS_URL = API_URL + "events/"
    transaction_fields = [
        "asset_url",
        "image_url",
        "contract_address",
        "token_id",
        "collection",
        "seller",
        "buyer",
        "price",
        "coin",
        "price_usd",
        "timestamp",
    ]
    nft_fields = [
        "asset_url",
        "image_url",
        "contract_address",
        "token_id",
        "collection",
    ]


    def __init__(self, api_key):
        self.api_key = api_key
        self.s = requests.Session()
        self.s.headers.update({"X-API-KEY": self.api_key})
        self.timeout = 4

    @sleep_and_retry
    @limits(calls=3, period=1)
    def _get(self, *args, **kwargs):
        r = self.s.get(*args, **kwargs)
        while r.status_code == 429:
            print(f"429. Sleeping for {self.timeout} seconds")
            time.sleep(self.timeout)
            self.timeout += 4
            r = self.s.get(*args, **kwargs)
        if r.status_code != 200:
            raise OSAPIError(f"API returned {r.status_code} for {kwargs['url']}")
        if self.timeout > 4:
            self.timeout -= 4
        return r

    def parse_transaction(self, event):
        transaction = {
            field: None for field in self.transaction_fields
        }

        total_price = int(event["total_price"])
        if event["payment_token"]:
            decimals = int(event["payment_token"]["decimals"])
            price = total_price/(10**decimals)
            usd_value = float(event["payment_token"]["usd_price"])
            transaction["coin"] = event["payment_token"]["symbol"]
            transaction["price"] = price
            transaction["price_usd"] = price*usd_value

        transaction["seller"] = event["seller"]["address"]
        transaction["timestamp"] = event["transaction"]["timestamp"]
        if event["transaction"]["from_account"]:
            transaction["buyer"] = event["transaction"]["from_account"]["address"]

        if event["asset"]:
            transaction["asset_url"] = event["asset"]["permalink"]
            transaction["image_url"] = event["asset"]["image_url"]
            transaction["token_id"] = event["asset"]["token_id"]
            transaction["contract_address"] = event["asset"]["asset_contract"]["address"]
            if event["asset"]["collection"]:
                transaction["collection"] = event["asset"]["collection"]["slug"]

        return transaction

    def parse_nft(self, asset):
        nft = {
            field: None for field in self.nft_fields
        }

        nft["asset_url"] = asset["permalink"]
        nft["image_url"] = asset["image_url"]
        nft["token_id"] = asset["token_id"]
        nft["contract_address"] = asset["asset_contract"]["address"]
        if asset["collection"]:
            nft["collection"] = asset["collection"]["slug"]

        return nft

    def get_collection_owners(self, slug, limit_requests=1):
        owners = list()
        params = {
            "cursor": None,
            "collection": slug,
            "limit": 50,
        }

        req_n = 1
        r = self._get(url=self.ASSETS_URL, params=params)
        print(f"Got owners for {slug} Request number {req_n} cursor: {params['cursor']}")
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for asset in r_json["assets"]:
            owners.append(asset["owner"]["address"])

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
                print(f"Got owners for {slug} Request number {req_n} cursor: {params['cursor']}")
            except OSAPIError as e:
                print(e)
                return owners
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

                for asset in r_json["assets"]:
                    owners.append(asset["owner"]["address"])

        return owners

    def get_wallet_transactions(self, wallet, limit_requests=1):
        # skip default (null) wallet
        if wallet == "0x0000000000000000000000000000000000000000":
            return

        transactions = list()
        params = {
            "cursor": None,
            "account_address": wallet,
            "event_type": "successful",
            "limit": 300,
        }

        req_n = 1
        r = self._get(url=self.EVENTS_URL, params=params)
        print(f"Got transactions for {wallet} Request number {req_n} cursor: {params['cursor']}")
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for event in r_json["asset_events"]:
            transactions.append(
                self.parse_transaction(event)
            )

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
                print(f"Got transactions for {wallet} Request number {req_n} cursor: {params['cursor']}")
            except OSAPIError as e:
                print(e)
                return transactions
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

                for event in r_json["asset_events"]:
                    transactions.append(
                        self.parse_transaction(event)
                    )

        return transactions

    def get_collection_sales(self, slug, limit_requests=1):
        sales = list()
        params = {
            "cursor": None,
            "collection_slug": slug,
            "event_type": "successful",
            "limit": 300,
        }

        req_n = 1
        r = self._get(url=self.EVENTS_URL, params=params)
        print(f"Got sales for {slug} Request number {req_n} cursor: {params['cursor']}")
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for event in r_json["asset_events"]:
            sales.append(
                self.parse_transaction(event)
            )

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
                print(f"Got sales for {slug} Request number {req_n} cursor: {params['cursor']}")
            except OSAPIError as e:
                print(e)
                return sales
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

                for event in r_json["asset_events"]:
                    sales.append(
                        self.parse_transaction(event)
                    )

        return sales

    def get_wallet_assets(self, wallet, limit_requests=1):
        # skip default (null) wallet
        if wallet == "0x0000000000000000000000000000000000000000":
            return

        nfts = list()
        params = {
            "cursor": None,
            "owner": wallet,
            "limit": 50,
        }

        req_n = 1
        r = self._get(url=self.ASSETS_URL, params=params)
        print(f"Got assets for {wallet} Request number {req_n} cursor: {params['cursor']}")
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for asset in r_json["assets"]:
            nfts.append(
                self.parse_nft(asset)
            )

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
                print(f"Got assets for {wallet} Request number {req_n} cursor: {params['cursor']}")
            except OSAPIError as e:
                print(e)
                return nfts
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

                for asset in r_json["assets"]:
                    nfts.append(
                        self.parse_nft(asset)
                    )

        return nfts
