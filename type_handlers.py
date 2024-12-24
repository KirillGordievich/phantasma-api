from datetime import datetime
from decimal import Decimal
from typing import Any, Dict
import pandas as pd


class StrictTypeHandler:
    """Enforces strict data typing and format consistency."""

    @staticmethod
    def to_timestamp(value: Any) -> datetime:
        print(value, type(value))
        """Convert to timestamp, ensuring timezone is removed."""
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        try:
            # Convert to pandas timestamp and remove timezone
            ts = pd.to_datetime(value)
            return ts.replace(tzinfo=None)
        except:
            raise ValueError(f"Cannot convert {value} to timestamp")

    @staticmethod
    def to_decimal(value: Any) -> Decimal:
        """Convert to Decimal."""
        if pd.isna(value):
            return Decimal('0')
        try:
            return Decimal(str(value))
        except:
            raise ValueError(f"Cannot convert {value} to Decimal")

    @staticmethod
    def to_float(value: Any) -> float:
        """Convert to float."""
        if pd.isna(value):
            return 0.0
        try:
            return float(value)
        except:
            raise ValueError(f"Cannot convert {value} to float")

    @staticmethod
    def to_int(value: Any) -> int:
        """Convert to int."""
        try:
            return int(float(str(value)))
        except:
            raise ValueError(f"Cannot convert {value} to int")

    @classmethod
    def format_token_info(cls, data: Dict) -> Dict:
        """Format token info to standard structure."""
        base_symbol = str(data.get('symbol', '')).strip()
        result = {
            'created_timestamp': cls.to_timestamp(data['created_timestamp']),
            'decimals': cls.to_int(data.get('decimals', 0)),
            'symbol': base_symbol,
            'symbol_formats': {
                'base': base_symbol,  # Original/base symbol
                'lunar': base_symbol.upper(),  # LunarCrush format
                'ccxt': f"{base_symbol.upper()}/USDT",  # CCXT format
                'lower': base_symbol.lower(),  # Lowercase
            },
            'name': str(data.get('name', '')).strip(),
            'supply': cls.to_decimal(data.get('supply', 0))
        }

        # Add creators if present
        if 'creators' in data:
            result['creators'] = [str(addr).strip() for addr in data['creators']]

        # Add involved if present
        if 'involved' in data:
            result['involved'] = [str(addr).strip() for addr in data['involved']]

        # add early transfers if present, save ALL data:
        if 'early_transfers' in data:
            result['early_transfers'] = data['early_transfers']

        return result

    @classmethod
    def format_holders_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Format holders dataframe to standard structure."""
        return pd.DataFrame({
            'owner_address': df['owner_address'].astype(str).str.strip(),
            'balance': df['balance'].apply(cls.to_decimal)
        })

    @classmethod
    def format_historical_holders_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Format holders dataframe to standard structure."""
        return pd.DataFrame({
            'timestamp': df['timestamp'].apply(cls.to_timestamp),
            'owner_address': df['owner_address'].astype(str).str.strip(),
            'balance': df['balance'].apply(cls.to_decimal)
        })

    @classmethod
    def format_prices_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Format prices dataframe to standard structure."""
        formatted_df = pd.DataFrame({
            'timestamp': df['timestamp'].apply(cls.to_timestamp),
            'average_price': df['average_price'].apply(cls.to_float),
            'close_price': df['close_price'].apply(cls.to_float),
            'volume': df['volume'].apply(cls.to_decimal)
        })
        # Ensure timestamp column has no timezone info
        formatted_df['timestamp'] = formatted_df['timestamp'].dt.tz_localize(None)
        return formatted_df

    @classmethod
    def format_transfers_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Format transfers dataframe to standard structure."""
        formatted_df = pd.DataFrame({
            'timestamp': df['timestamp'].apply(cls.to_timestamp),
            'from_address': df['from_address'].astype(str).str.strip(),
            'to_address': df['to_address'].astype(str).str.strip(),
            'quantity': df['quantity'].apply(cls.to_decimal),
            'transaction_hash': df['transaction_hash'].astype(str).str.strip()
        })
        # Ensure timestamp column has no timezone info
        formatted_df['timestamp'] = formatted_df['timestamp'].dt.tz_localize(None)
        return formatted_df

    def format_social_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format social metrics DataFrame to standard structure."""
        try:
            metrics = {
                # Social Volume Metrics
                'social_volume': df.get('social_volume', 0),
                'social_volume_change': df.get('social_volume_24h', 0),
                'social_dominance': df.get('social_dominance', 0),

                # Sentiment Metrics
                'sentiment_score': df.get('sentiment', 0),
                'bullish_ratio': df.get('sentiment_relative_bullish', 0),
                'sentiment_change': df.get('sentiment_change_24h', 0),

                # Engagement Metrics
                'contributors_active': df.get('contributors_active', 0),
                'posts_active': df.get('posts_active', 0),
                'interactions': df.get('interactions', 0),
                'spam_ratio': df.get('spam', 0),

                # News/Influencer Metrics
                'posts_created': df.get('posts_created', 0),
                'contributors_created': df.get('contributors_created', 0),

                # Additional Context
                'galaxy_score': df.get('galaxy_score', 0),
                'volume_24h': df.get('volume_24h', 0),
                'alt_rank': df.get('alt_rank', 0),
                'volatility': df.get('volatility', 0),
                'market_dominance': df.get('market_dominance', 0)
            }

            # Convert all metrics to numeric, handling any conversion errors
            formatted_df = pd.DataFrame({
                metric: pd.to_numeric(values, errors='coerce')
                for metric, values in metrics.items()
            })

            # Ensure timestamp index with no timezone
            formatted_df.index = pd.to_datetime(df.index)
            formatted_df.index = formatted_df.index.tz_localize(None)
            formatted_df.fillna(0, inplace=True)  # Fill any NaN values with 0

            return formatted_df

        except Exception as e:
            print(e)
            return pd.DataFrame()  # Return empty DataFrame on error
