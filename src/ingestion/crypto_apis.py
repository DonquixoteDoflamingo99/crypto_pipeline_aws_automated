"""
Cryptocurrency API client implementations.

This module provides client classes for various cryptocurrency exchanges
and data providers.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import requests


logger = logging.getLogger(__name__)


class CryptoAPIClient(ABC):
    """Abstract base class for cryptocurrency API clients."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CryptoPipeline/1.0'
        })
    
    @abstractmethod
    def get_ticker_data(self, symbols: List[str]) -> List[Dict]:
        """Get ticker data for specified symbols."""
        pass
    
    @abstractmethod
    def get_market_data(self, symbols: List[str]) -> Dict:
        """Get additional market data."""
        pass


class BinanceClient(CryptoAPIClient):
    """Binance API client for cryptocurrency data."""
    
    BASE_URL = "https://api.binance.com/api/v3"
    
    def get_ticker_data(self, symbols: List[str]) -> List[Dict]:
        """Get 24hr ticker statistics for specified symbols."""
        try:
            url = f"{self.BASE_URL}/ticker/24hr"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            all_tickers = response.json()
            
            # Filter for our symbols
            filtered_data = []
            for ticker in all_tickers:
                if ticker['symbol'] in symbols:
                    filtered_data.append({
                        'symbol': ticker['symbol'],
                        'price': float(ticker['lastPrice']),
                        'volume': float(ticker['volume']),
                        'high_24h': float(ticker['highPrice']),
                        'low_24h': float(ticker['lowPrice']),
                        'price_change_24h': float(ticker['priceChange']),
                        'price_change_percent_24h': float(ticker['priceChangePercent']),
                        'trade_count': int(ticker['count']),
                        'timestamp': int(time.time()),
                        'source': 'binance'
                    })
            
            logger.info(f"Fetched data for {len(filtered_data)} symbols from Binance")
            return filtered_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Binance data: {e}")
            raise
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing Binance response: {e}")
            raise


class CoinGeckoClient(CryptoAPIClient):
    """CoinGecko API client for additional market data."""
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    # Mapping from Binance symbols to CoinGecko IDs
    SYMBOL_MAPPING = {
        'BTCUSDT': 'bitcoin',
        'ETHUSDT': 'ethereum',
        'ADAUSDT': 'cardano',
        'DOTUSDT': 'polkadot',
        'LINKUSDT': 'chainlink',
        'BNBUSDT': 'binancecoin',
        'SOLUSDT': 'solana',
        'MATICUSDT': 'matic-network',
        'AVAXUSDT': 'avalanche-2',
        'UNIUSDT': 'uniswap'
    }
    
    def get_ticker_data(self, symbols: List[str]) -> List[Dict]:
        """CoinGecko doesn't provide ticker data in the same format."""
        return []
    
    def get_market_data(self, symbols: List[str]) -> Dict:
        """Get market data from CoinGecko."""
        try:
            # Convert symbols to CoinGecko IDs
            coingecko_ids = []
            for symbol in symbols:
                if symbol in self.SYMBOL_MAPPING:
                    coingecko_ids.append(self.SYMBOL_MAPPING[symbol])
            
            if not coingecko_ids:
                return {}
            
            url = f"{self.BASE_URL}/simple/price"
            params = {
                'ids': ','.join(coingecko_ids),
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true'
            }
            
            headers = {}
            if self.api_key:
                headers['X-CG-Demo-API-Key'] = self.api_key
            
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Fetched market data for {len(data)} coins from CoinGecko")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching CoinGecko data: {e}")
            return {}
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing CoinGecko response: {e}")
            return {}


class CryptoDataAggregator:
    """Aggregates data from multiple cryptocurrency API sources."""
    
    def __init__(self, binance_client: BinanceClient, coingecko_client: CoinGeckoClient):
        self.binance_client = binance_client
        self.coingecko_client = coingecko_client
    
    def fetch_enriched_data(self, symbols: List[str]) -> List[Dict]:
        """Fetch and enrich cryptocurrency data from multiple sources."""
        # Get ticker data from Binance
        ticker_data = self.binance_client.get_ticker_data(symbols)
        
        # Get additional market data from CoinGecko
        market_data = self.coingecko_client.get_market_data(symbols)
        
        # Enrich ticker data with market data
        enriched_data = []
        for ticker in ticker_data:
            symbol = ticker['symbol']
            coingecko_id = self.coingecko_client.SYMBOL_MAPPING.get(symbol)
            
            if coingecko_id and coingecko_id in market_data:
                cg_data = market_data[coingecko_id]
                ticker.update({
                    'market_cap': cg_data.get('usd_market_cap'),
                    'volume_24h_usd': cg_data.get('usd_24h_vol'),
                    'price_change_24h_cg': cg_data.get('usd_24h_change')
                })
            
            enriched_data.append(ticker)
        
        return enriched_data