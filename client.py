import time
import requests
from ratelimit import limits, sleep_and_retry

class OSAPIError(Exception):
    pass

""" API rate limit: 4/sec """


class ApiClient:
    RATE = 4
    API_URL = "https://api.opensea.io/api/v1/"
    ASSETS_URL = API_URL + "assets/"
    EVENTS_URL = API_URL + "events/"
    COLLECTION_URL = API_URL + "collection/"
    data_fields = [
        "contract_address",
        "token_id",
        "owner",
    ]
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
    col_fields = [
        "one_day_volume",
        "one_day_change",
        "one_day_sales",
        "one_day_average_price",
        "seven_day_volume",
        "seven_day_change",
        "seven_day_sales",
        "seven_day_average_price",
        "thirty_day_volume",
        "thirty_day_change",
        "thirty_day_sales",
        "thirty_day_average_price",
        "total_volume",
        "total_sales",
        "total_supply",
        "count",
        "num_owners",
        "average_price",
        "num_reports",
        "market_cap",
        "floor_price",
    ]

    def __init__(self, api_key):
        self.api_key = api_key
        self.s = requests.Session()
        self.s.headers.update({"X-API-KEY": self.api_key})
        self.timeout = 4
        self.must_wait = False

    @sleep_and_retry
    @limits(calls=RATE, period=1)
    def _get(self, *args, **kwargs):
        while self.must_wait:
            time.sleep(self.timeout)
        r = self.s.get(*args, **kwargs)
        while r.status_code == 429:
            self.must_wait = True
            print(f"429. Sleeping for {self.timeout} seconds")
            self.timeout += 4
            time.sleep(self.timeout)
            r = self.s.get(*args, **kwargs)
        self.must_wait = False
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

    def parse_col_info(self, col_json):
        col_info = {
            field: None for field in self.col_fields
        }

        stats = col_json["stats"]

        col_info["one_day_volume"] = stats["one_day_volume"]
        col_info["one_day_change"] = stats["one_day_change"]
        col_info["one_day_sales"] = stats["one_day_sales"]
        col_info["one_day_average_price"] = stats["one_day_average_price"]
        col_info["seven_day_volume"] = stats["seven_day_volume"]
        col_info["seven_day_change"] = stats["seven_day_change"]
        col_info["seven_day_sales"] = stats["seven_day_sales"]
        col_info["seven_day_average_price"] = stats["seven_day_average_price"]
        col_info["thirty_day_volume"] = stats["thirty_day_volume"]
        col_info["thirty_day_change"] = stats["thirty_day_change"]
        col_info["thirty_day_sales"] = stats["thirty_day_sales"]
        col_info["thirty_day_average_price"] = stats["thirty_day_average_price"]
        col_info["total_volume"] = stats["total_volume"]
        col_info["total_sales"] = stats["total_sales"]
        col_info["total_supply"] = stats["total_supply"]
        col_info["count"] = stats["count"]
        col_info["num_owners"] = stats["num_owners"]
        col_info["average_price"] = stats["average_price"]
        col_info["num_reports"] = stats["num_reports"]
        col_info["market_cap"] = stats["market_cap"]
        col_info["floor_price"] = stats["floor_price"]

        return col_info

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

    def get_collection_info(self, slug):
        r = self._get(url=self.COLLECTION_URL+slug)
        print(f"Got info for {slug}")
        r_json = r.json()
        col_json = r_json["collection"]

        return self.parse_col_info(col_json)

    def get_col_assets_data(self, slug, limit_requests=1):
        params = {
            "cursor": None,
            "collection": slug,
            "limit": 50,
        }
        req_n = 1
        first = True
        while (first or params["cursor"]) and (
            limit_requests == None or req_n <= limit_requests
        ):
            first = False
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
                print(f"Got data for {slug} assets Request number {req_n}")
            except OSAPIError as e:
                print(e)
                return list()
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

            req_n += 1
            yield [
                {
                    "contract_address": asset["asset_contract"]["address"],
                    "token_id": asset["token_id"],
                    "owner": asset["owner"]["address"],
                } for asset in r_json["assets"]
            ]

    def get_wallet_transactions(self, wallet, limit_requests=1):
        # skip default (null) wallet
        if wallet == "0x0000000000000000000000000000000000000000":
            return list()
        params = {
            "cursor": None,
            "account_address": wallet,
            "event_type": "successful",
            "limit": 300,
        }
        req_n = 1
        first = True
        while (first or params["cursor"]) and (
            limit_requests == None or req_n <= limit_requests
        ):
            first = False
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
                print(f"Got transactions for {wallet} Request number {req_n}")
            except OSAPIError as e:
                print(e)
                return list()
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

            req_n += 1
            yield [
                self.parse_transaction(event)
                for event in r_json["asset_events"]
            ]

    def get_collection_sales(self, slug, limit_requests=1):
        params = {
            "cursor": None,
            "collection_slug": slug,
            "event_type": "successful",
            "limit": 300,
        }
        req_n = 1
        first = True
        while (first or params["cursor"]) and (
            limit_requests == None or req_n <= limit_requests
        ):
            first = False
            try:
                r = self._get(url=self.EVENTS_URL, params=params)
                print(f"Got sales for {slug} Request number {req_n}")
            except OSAPIError as e:
                print(e)
                return list()
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

            req_n += 1
            yield [
                self.parse_transaction(event)
                for event in r_json["asset_events"]
            ]

    def get_wallet_assets(self, wallet, limit_requests=1):
        # skip default (null) wallet
        if wallet == "0x0000000000000000000000000000000000000000":
            return list()
        params = {
            "cursor": None,
            "owner": wallet,
            "limit": 50,
        }
        req_n = 1
        first = True
        while (first or params["cursor"]) and (
            limit_requests == None or req_n <= limit_requests
        ):
            first = False
            try:
                r = self._get(url=self.ASSETS_URL, params=params)
                print(f"Got assets for {wallet} Request number {req_n}")
            except OSAPIError as e:
                print(e)
                return list()
            else:
                r_json = r.json()
                params["cursor"] = r_json["next"]

            req_n += 1
            yield [
                self.parse_nft(asset)
                for asset in r_json["assets"]
            ]
