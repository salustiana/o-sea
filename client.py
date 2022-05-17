import time
import requests
from datetime import datetime

class OSAPIError(Exception):
    pass

""" API rate limit: 4/sec """


class ApiClient:
    API_URL = "https://api.opensea.io/api/v1/"
    ASSETS_URL = API_URL + "assets/"
    EVENTS_URL = API_URL + "events/"

    def __init__(self, api_key):
        self.api_key = api_key
        self.s = requests.Session()
        self.s.headers.update({"X-API-KEY": self.api_key})
        self.last_req = None

    def _get(self, *args, **kwargs):
        start = datetime.now()
        r = self.s.get(*args, **kwargs)
        end = datetime.now()
        must_wait = 1 - (end - start).total_seconds()
        if must_wait > 0:
            time.sleep(must_wait)
        while r.status_code == 429:
            print(f"API returned 429 for {kwargs['url']}. Sleeping for 1 second")
            time.sleep(1)
            r = self.s.get(*args, **kwargs)
        if r.status_code != 200:
            raise OSAPIError(f"API returned {r.status_code} for {kwargs['url']}")
        return r

    def parse_transaction(self, event):
        transaction = {
            "asset_url": None,
            "contract_address": None,
            "token_id": None,
            "collection": None,
            "seller": None,
            "buyer": None,
            "price": None,
            "coin": None,
            "price_usd": None,
            "timestamp": None,
        }
        decimals = int(event["payment_token"]["decimals"])
        total_price = int(event["total_price"])
        price = total_price/(10**decimals)
        usd_value = float(event["payment_token"]["usd_price"])

        transaction["seller"] = event["seller"]["address"]
        transaction["buyer"] = event["transaction"]["from_account"]["address"]
        transaction["timestamp"] = event["transaction"]["timestamp"]
        transaction["coin"] = event["payment_token"]["symbol"]

        transaction["price"] = price
        transaction["price_usd"] = price*usd_value
        if event["asset"]:
            transaction["asset_url"] = event["asset"]["permalink"]
            transaction["token_id"] = event["asset"]["token_id"]
            transaction["contract_address"] = event["asset"]["asset_contract"]["address"]
            if event["asset"]["collection"]:
                transaction["collection"] = event["asset"]["collection"]["slug"]

        return transaction

    def get_collection_owners(self, slug, limit_requests=1):
        owners = list()
        params = {
            "cursor": None,
            "collection": slug,
            "limit": 50,
        }

        req_n = 1
        print(f"Getting owners for {slug}. Request number {req_n}, cursor: {params['cursor']}")
        r = self._get(url=self.ASSETS_URL, params=params)
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for asset in r_json["assets"]:
            owners.append(asset["owner"]["address"])

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            print(f"Getting owners for {slug}. Request number {req_n}, cursor: {params['cursor']}")
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
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
        # return if the wallet is default null address
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
        print(f"Getting transactions for {wallet}. Request number {req_n}, cursor: {params['cursor']}")
        r = self._get(url=self.EVENTS_URL, params=params)
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for event in r_json["asset_events"]:
            transactions.append(
                self.parse_transaction(event)
            )

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            print(f"Getting transactions for {wallet}. Request number {req_n}, cursor: {params['cursor']}")
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
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
        print(f"Getting sales for {slug}. Request number {req_n}, cursor: {params['cursor']}")
        r = self._get(url=self.EVENTS_URL, params=params)
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for event in r_json["asset_events"]:
            sales.append(
                self.parse_transaction(event)
            )

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            print(f"Getting sales for {slug}. Request number {req_n}, cursor: {params['cursor']}")
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
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
        # return if the wallet is default null address
        if wallet == "0x0000000000000000000000000000000000000000":
            return

        nfts = list()
        params = {
            "cursor": None,
            "owner": wallet,
            "limit": 50,
        }

        req_n = 1
        print(f"Getting assets for {wallet}. Request number {req_n}, cursor: {params['cursor']}")
        r = self._get(url=self.ASSETS_URL, params=params)
        r_json = r.json()
        params["cursor"] = r_json["next"]

        for asset in r_json["assets"]:
            nft = {
                "contract_address": None, "token_id": None, "collection": None,
            }
            nft["token_id"] = asset["token_id"]
            nft["contract_address"] = asset["asset_contract"]["address"]
            if asset["collection"]:
                nft["collection"] = asset["collection"]["slug"]

            nfts.append(nft)

        while params["cursor"] and (limit_requests == None or req_n < limit_requests):
            req_n += 1
            print(f"Getting assets for {wallet}. Request number {req_n}, cursor: {params['cursor']}")
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
            except OSAPIError as e:
                print(e)
                return nfts
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

                for asset in r_json["assets"]:
                    nft = {
                        "contract_address": None, "token_id": None, "collection": None,
                    }
                    nft["token_id"] = asset["token_id"]
                    nft["contract_address"] = asset["asset_contract"]["address"]
                    if asset["collection"]:
                        nft["collection"] = asset["collection"]["slug"]

                    nfts.append(nft)

        return nfts

