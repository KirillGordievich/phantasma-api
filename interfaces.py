from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional, Set, List
from decimal import Decimal
import pandas as pd


class TokenDataProvider(ABC):
    """Interface for token-related data."""

    @abstractmethod
    def fetch_token_info(self, token_address: str) -> Optional[Dict]:
        """Fetch basic token information."""
        pass


class TransferDataProvider(ABC):
    """Interface for transfer-related data."""

    @abstractmethod
    def fetch_transfers(self,
                        token_address: str,
                        addresses: Optional[Set[str]] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        min_value: Decimal = Decimal(0)) -> Optional[pd.DataFrame]:
        """Fetch transfer data."""
        pass

    @abstractmethod
    def fetch_early_transfers(self,
                              token_address: str,
                              start_date: datetime,
                              end_date: datetime) -> Optional[pd.DataFrame]:
        """Fetch early token transfers."""
        pass


class HolderDataProvider(ABC):
    """Interface for holder-related data."""

    @abstractmethod
    def fetch_holders(self,
                      token_address: str,
                      min_balance: Decimal = Decimal(0)) -> Optional[pd.DataFrame]:
        """Fetch current token holders."""
        pass

    @abstractmethod
    def fetch_historical_owners(self,
                                token_address: str,
                                addresses: List[str],
                                start_date: datetime,
                                end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch historical token balances."""
        pass


class PriceDataProvider(ABC):
    """Interface for price-related data."""

    @abstractmethod
    def fetch_prices(self,
                     token_address: str,
                     start_date: datetime,
                     end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch token price history."""
        pass


class CompleteDataProvider(TokenDataProvider, TransferDataProvider, HolderDataProvider, PriceDataProvider):
    """Interface combining all data provider capabilities."""
    pass
