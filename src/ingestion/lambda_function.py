# src/ingestion/lambda_function.py
import json
import boto3
import requests
import time
import os
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """
    Lambda function to ingest cryptocurrency data from multiple sources
    """
    kinesis = boto3.client('kinesis')
    dynamodb = boto3.resource('dynamodb')
    
    stream_name = os.environ['KINESIS_STREAM_NAME']
    table_name = os.environ['DYNAMODB_TABLE_NAME']
    table = dynamodb.Table(table_name)
    
    # Crypto symbols to track
    symbols = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT', 'LINKUSDT']
    
    try:
        # Fetch data from Binance API
        binance_data = fetch_binance_data(symbols)
        
        # Fetch data from CoinGecko API
        coingecko_data = fetch_coingecko_data()
        
        # Process and send to Kinesis
        for symbol_data in binance_data:
            # Enrich with additional data
            enriched_data = enrich_price_data(symbol_data, coingecko_data)
            
            # Send to Kinesis
            send_to_kinesis(kinesis, stream_name, enriched_data)
            
            # Store current price in DynamoDB for real-time queries
            store_current_price(table, enriched_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {len(binance_data)} symbols')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def fetch_binance_data(symbols):
    """Fetch current prices from Binance API"""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    
    try:
        response = requests.get(url, timeout=30)
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
        
        return filtered_data
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Binance data: {e}")
        raise

def fetch_coingecko_data():
    """Fetch additional market data from CoinGecko"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'bitcoin,ethereum,cardano,polkadot,chainlink',
        'vs_currencies': 'usd',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_24hr_change': 'true'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching CoinGecko data: {e}")
        return {}

def enrich_price_data(binance_data, coingecko_data):
    """Enrich Binance data with CoinGecko market data"""
    # Map Binance symbols to CoinGecko IDs
    symbol_mapping = {
        'BTCUSDT': 'bitcoin',
        'ETHUSDT': 'ethereum',
        'ADAUSDT': 'cardano',
        'DOTUSDT': 'polkadot',
        'LINKUSDT': 'chainlink'
    }
    
    symbol = binance_data['symbol']
    coingecko_id = symbol_mapping.get(symbol)
    
    if coingecko_id and coingecko_id in coingecko_data:
        cg_data = coingecko_data[coingecko_id]
        binance_data.update({
            'market_cap': cg_data.get('usd_market_cap'),
            'volume_24h_usd': cg_data.get('usd_24h_vol'),
            'price_change_24h_cg': cg_data.get('usd_24h_change')
        })
    
    # Add technical indicators placeholders (will be calculated in Spark)
    binance_data.update({
        'event_time': datetime.utcnow().isoformat(),
        'partition_date': datetime.utcnow().strftime('%Y-%m-%d'),
        'partition_hour': datetime.utcnow().strftime('%H')
    })
    
    return binance_data

def send_to_kinesis(kinesis, stream_name, data):
    """Send data to Kinesis stream"""
    try:
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=data['symbol']
        )
        print(f"Sent {data['symbol']} to Kinesis: {response['SequenceNumber']}")
        
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")
        raise

def store_current_price(table, data):
    """Store current price in DynamoDB for real-time access"""
    try:
        # Convert float values to Decimal for DynamoDB
        item = {
            'symbol': data['symbol'],
            'timestamp': data['timestamp'],
            'price': Decimal(str(data['price'])),
            'volume': Decimal(str(data['volume'])),
            'high_24h': Decimal(str(data['high_24h'])),
            'low_24h': Decimal(str(data['low_24h'])),
            'price_change_percent_24h': Decimal(str(data['price_change_percent_24h'])),
            'source': data['source'],
            'ttl': data['timestamp'] + 86400  # 24 hours TTL
        }
        
        # Add market cap if available
        if data.get('market_cap'):
            item['market_cap'] = Decimal(str(data['market_cap']))
        
        table.put_item(Item=item)
        print(f"Stored {data['symbol']} current price in DynamoDB")
        
    except Exception as e:
        print(f"Error storing in DynamoDB: {e}")
        # Don't raise here, as Kinesis data is more important