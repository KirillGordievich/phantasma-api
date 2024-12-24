from datetime import datetime
from typing import Dict, Optional, Set, List, Any
import pandas as pd
import logging
import requests
from decimal import Decimal


from type_handlers import StrictTypeHandler
from interfaces import CompleteDataProvider

logger = logging.getLogger(__name__)


class PhantasmaAPI(CompleteDataProvider):
    """Handles Phantasma Explorer API interaction."""

    def __init__(self):
        self.base_url = "https://api-explorer.phantasma.info/api/v1"
        self.type_handler = StrictTypeHandler()

    def _make_request(self, endpoint: str, params: Dict = None) -> Any:
        """Make request to Phantasma Explorer API."""
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise ValueError(f"API request failed with code {response.status_code}: {response.text}")

        return response.json()

    def get_symbol_by_token_address(self, token_address: str) -> str:
        params = {"address": token_address}
        results = self._make_request("addresses", params)
        result = results['addresses']

        if not result:
            raise ValueError(f"No token with {token_address}")

        return result[0]['address_name']

    def fetch_token_info(self, token_address: str) -> Optional[Dict]:
        """Fetch token information."""
        try:
            token_symbol = self.get_symbol_by_token_address(token_address)
            params = {"symbol": token_symbol, "create_event": 1}
            results = self._make_request("tokens", params)
            result = results['tokens'][0]
            created_ts = 0

            if "create_event" in result:
                created_ts = int(result["create_event"]['creation_date'])

            token_data = {
                'created_timestamp': created_ts,
                'decimals': result['decimals'],
                'symbol': str(result['symbol']),
                'name': str(result['name']),
                'supply': result['current_supply'],
            }
            return self.type_handler.format_token_info(token_data)
        except Exception as e:
            logger.error(f"Token info fetch failed for {token_address}: {e}")
            return None


    def fetch_holders(self,
                      token_address: str,
                      min_balance: Decimal = Decimal(0)) -> Optional[pd.DataFrame]:
        """Fetch token holders."""
        try:
            token_symbol = self.get_symbol_by_token_address(token_address)

            params = {
                'offset': 0,
                'limit': 20000,
                'contract': token_symbol,
                'with_balance': 1
            }

            all_holders = []
            while True:
                result = self._make_request("addresses", params)

                if not result['addresses']:
                    break

                for holder in result['addresses']:
                    balance = next((b['amount'] for b in holder['balances'] if b['token']['symbol'] == token_symbol), '0')
                    if Decimal(balance) >= min_balance:
                        all_holders.append({
                            'owner_address': holder['address'],
                            'balance': balance
                        })
                if len(result['addresses']) < params['limit']:
                    break

                params['offset'] += params['limit']
            df = pd.DataFrame(all_holders)
            return self.type_handler.format_holders_df(df)
        except Exception as e:
            logger.error(f"Holders fetch failed for {token_address}: {e}")
            return None

    def fetch_historical_owners(self,
                                token_address: str,
                                addresses: List[str],
                                start_date: datetime,
                                end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch historical token balances."""
        pass

    def fetch_transfers(self,
                        token_address: str,
                        addresses: Optional[Set[str]] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        from_address_only: bool = False,
                        min_value: Decimal = Decimal(0)) -> Optional[pd.DataFrame]:
        """
        Fetch and format transfer information.

        Args:
            token_address: Token contract address
            addresses: Set of addresses to filter by
            start_date: Start date for transfers
            end_date: End date for transfers
            from_address_only: If True, only get transfers FROM the addresses
            min_value: Minimum transfer value to include
        """
        try:
            token_symbol = self.get_symbol_by_token_address(token_address)

            params = {
                'date_greater': int(start_date.timestamp()) if start_date else None,
                'date_less': int(end_date.timestamp()) if end_date else None,
                'offset': 0,
                'limit': 20_000,
                'contract': token_symbol,
                'with_event_data': 1
            }

            all_transfers = []
            while True:
                result_send = self._make_request(
                    "events",
                    {'event_kind': 'TokenSend', **params}
                )
                result_receive = self._make_request(
                    "events",
                    {'event_kind': 'TokenReceive', **params}
                )

                if not result_send['events'] or not result_receive['events']:
                    break

                for index, event_send in enumerate(result_send['events']):
                    event_receive = result_receive['events'][index]

                    if Decimal(event_send['token_event']['value']) < min_value:
                        continue

                    if addresses:
                        if from_address_only:
                            if event_send['address'] not in addresses:
                                continue
                        else:
                            if not any(address in addresses for address in (event_send['address'], event_receive['address'])):
                                continue

                    all_transfers.append({
                        'timestamp': int(event_send['date']),
                        'from_address': event_send['address'],
                        'to_address': event_receive['address'],
                        'quantity': event_send['token_event']['value'],
                        'transaction_hash': event_send['transaction_hash']
                    })

                if len(result_send['events']) < params['limit']:
                    break

                params['offset'] += params['limit']

            df = pd.DataFrame(all_transfers)
            return self.type_handler.format_transfers_df(df)
        except Exception as e:
            logger.error(f"Transfers fetch failed for {token_address}: {e}")
            return None

    def fetch_early_transfers(self,
                        token_address: str,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch token transfers."""
        try:
            token_symbol = self.get_symbol_by_token_address(token_address)

            params = {
                'date_greater': int(start_date.timestamp()) if start_date else None,
                'date_less': int(end_date.timestamp()) if end_date else None,
                'offset': 0,
                'limit': 20_000,
                'contract': token_symbol,
                'with_event_data': 1
            }

            all_transfers = []
            while True:
                result_send = self._make_request(
                    "events",
                    {'event_kind': 'TokenSend', **params}
                )
                result_receive = self._make_request(
                    "events",
                    {'event_kind': 'TokenReceive', **params}
                )

                if not result_send['events'] or not result_receive['events']:
                    break

                for index, event_send in enumerate(result_send['events']):
                    event_receive = result_receive['events'][index]

                    # оплата комсы тоже считается за трансфер, игонириуем такие эвенты
                    if token_symbol == "KCAL" and Decimal(event_send['token_event']['value']) < Decimal(0.005):
                        continue

                    all_transfers.append({
                        'timestamp': int(event_send['date']),
                        'from_address': event_send['address'],
                        'to_address': event_receive['address'],
                        'quantity': event_send['token_event']['value'],
                        'transaction_hash': event_send['transaction_hash']
                    })

                if len(result_send['events']) < params['limit']:
                    break

                params['offset'] += params['limit']

            df = pd.DataFrame(all_transfers)
            return self.type_handler.format_transfers_df(df)

        except Exception as e:
            logger.error(f"Early transfers fetch failed for {token_address}: {e}")
            return None

    def fetch_prices(self, token_address: str,
                     start_date: datetime,
                     end_date: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch token prices."""
        try:
            token_symbol = self.get_symbol_by_token_address(token_address)

            # no price data by time
            params = {
                # 'date_greater': int(start_date.timestamp()) if start_date else None,
                # 'date_less': int(end_date.timestamp()) if end_date else None,
                'symbol': token_symbol
            }

            result = self._make_request("historyPrices", params)

            prices = []
            for price_point in result['history_prices']:
                prices.append({
                    'timestamp': int(price_point['date']),
                    'average_price': price_point['price']['usd'],
                    'close_price': price_point['price']['usd'],
                    'volume': price_point.get('volume', '0')
                })

            df = pd.DataFrame(prices)
            return self.type_handler.format_prices_df(df)

        except Exception as e:
            logger.error(f"Prices fetch failed for {token_address}: {e}")
            return None

