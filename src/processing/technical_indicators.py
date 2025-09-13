"""
Technical indicators for cryptocurrency analysis.

This module implements various technical indicators commonly used
in cryptocurrency trading and analysis.
"""

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, avg, stddev, when, abs as spark_abs
from pyspark.sql.window import Window


logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """Calculator for technical indicators used in crypto trading."""
    
    @staticmethod
    def calculate_sma(df: DataFrame, price_col: str = "price", window: int = 20) -> DataFrame:
        """Calculate Simple Moving Average (SMA)."""
        window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        return df.withColumn(f"sma_{window}", avg(price_col).over(window_spec))
    
    @staticmethod
    def calculate_ema(df: DataFrame, price_col: str = "price", window: int = 20, alpha: float = None) -> DataFrame:
        """Calculate Exponential Moving Average (EMA)."""
        if alpha is None:
            alpha = 2.0 / (window + 1)
        
        # This is a simplified EMA calculation - in production, you'd want a more accurate implementation
        window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        return df.withColumn(f"ema_{window}", avg(price_col).over(window_spec))
    
    @staticmethod
    def calculate_rsi(df: DataFrame, price_col: str = "price", window: int = 14) -> DataFrame:
        """Calculate Relative Strength Index (RSI)."""
        # Calculate price changes
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        df_with_changes = df.withColumn(
            "price_change", 
            col(price_col) - lag(price_col, 1).over(window_spec)
        )
        
        # Calculate gains and losses
        df_with_gains_losses = df_with_changes \
            .withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0)) \
            .withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        # Calculate average gains and losses
        avg_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        df_with_averages = df_with_gains_losses \
            .withColumn("avg_gain", avg("gain").over(avg_window)) \
            .withColumn("avg_loss", avg("loss").over(avg_window))
        
        # Calculate RSI
        return df_with_averages \
            .withColumn("rs", col("avg_gain") / col("avg_loss")) \
            .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
    
    @staticmethod
    def calculate_macd(df: DataFrame, price_col: str = "price", 
                      fast_window: int = 12, slow_window: int = 26, signal_window: int = 9) -> DataFrame:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        # Calculate EMAs for MACD
        df_with_emas = TechnicalIndicators.calculate_ema(df, price_col, fast_window)
        df_with_emas = TechnicalIndicators.calculate_ema(df_with_emas, price_col, slow_window)
        
        # Calculate MACD line
        df_with_macd = df_with_emas.withColumn(
            "macd_line", 
            col(f"ema_{fast_window}") - col(f"ema_{slow_window}")
        )
        
        # Calculate signal line (EMA of MACD line)
        signal_window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-signal_window + 1, 0)
        df_with_signal = df_with_macd.withColumn(
            "macd_signal", 
            avg("macd_line").over(signal_window_spec)
        )
        
        # Calculate histogram
        return df_with_signal.withColumn(
            "macd_histogram", 
            col("macd_line") - col("macd_signal")
        )
    
    @staticmethod
    def calculate_bollinger_bands(df: DataFrame, price_col: str = "price", 
                                window: int = 20, num_std: float = 2.0) -> DataFrame:
        """Calculate Bollinger Bands."""
        window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        
        df_with_stats = df \
            .withColumn("bb_middle", avg(price_col).over(window_spec)) \
            .withColumn("bb_std", stddev(price_col).over(window_spec))
        
        return df_with_stats \
            .withColumn("bb_upper", col("bb_middle") + (col("bb_std") * num_std)) \
            .withColumn("bb_lower", col("bb_middle") - (col("bb_std") * num_std))
    
    @staticmethod
    def detect_price_anomalies(df: DataFrame, price_col: str = "price", 
                             window: int = 20, threshold: float = 2.5) -> DataFrame:
        """Detect price anomalies using statistical methods."""
        window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        
        df_with_stats = df \
            .withColumn("price_mean", avg(price_col).over(window_spec)) \
            .withColumn("price_stddev", stddev(price_col).over(window_spec))
        
        df_with_zscore = df_with_stats.withColumn(
            "z_score", 
            (col(price_col) - col("price_mean")) / col("price_stddev")
        )
        
        return df_with_zscore \
            .withColumn("is_anomaly", spark_abs(col("z_score")) > threshold) \
            .withColumn(
                "anomaly_type",
                when(col("z_score") > threshold, "price_spike")
                .when(col("z_score") < -threshold, "price_drop")
                .otherwise("normal")
            )
    
    @staticmethod
    def calculate_volatility(df: DataFrame, price_col: str = "price", window: int = 20) -> DataFrame:
        """Calculate price volatility."""
        # Calculate returns
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        df_with_returns = df.withColumn(
            "return", 
            (col(price_col) / lag(price_col, 1).over(window_spec) - 1) * 100
        )
        
        # Calculate rolling volatility
        volatility_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-window + 1, 0)
        return df_with_returns.withColumn(
            "volatility", 
            stddev("return").over(volatility_window)
        )


class TradingSignals:
    """Generate trading signals based on technical indicators."""
    
    @staticmethod
    def generate_rsi_signals(df: DataFrame, oversold: float = 30, overbought: float = 70) -> DataFrame:
        """Generate trading signals based on RSI."""
        return df.withColumn(
            "rsi_signal",
            when(col("rsi") < oversold, "BUY")
            .when(col("rsi") > overbought, "SELL")
            .otherwise("HOLD")
        )
    
    @staticmethod
    def generate_macd_signals(df: DataFrame) -> DataFrame:
        """Generate trading signals based on MACD crossover."""
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        
        df_with_prev_macd = df \
            .withColumn("prev_macd_line", lag("macd_line", 1).over(window_spec)) \
            .withColumn("prev_macd_signal", lag("macd_signal", 1).over(window_spec))
        
        return df_with_prev_macd.withColumn(
            "macd_signal_trade",
            when(
                (col("macd_line") > col("macd_signal")) & 
                (col("prev_macd_line") <= col("prev_macd_signal")), 
                "BUY"
            )
            .when(
                (col("macd_line") < col("macd_signal")) & 
                (col("prev_macd_line") >= col("prev_macd_signal")), 
                "SELL"
            )
            .otherwise("HOLD")
        )
    
    @staticmethod
    def generate_combined_signals(df: DataFrame) -> DataFrame:
        """Generate combined trading signals from multiple indicators."""
        return df.withColumn(
            "combined_signal",
            when(
                (col("rsi_signal") == "BUY") & 
                (col("macd_signal_trade") == "BUY"), 
                "STRONG_BUY"
            )
            .when(
                (col("rsi_signal") == "SELL") & 
                (col("macd_signal_trade") == "SELL"), 
                "STRONG_SELL"
            )
            .when(
                (col("rsi_signal") == "BUY") | 
                (col("macd_signal_trade") == "BUY"), 
                "BUY"
            )
            .when(
                (col("rsi_signal") == "SELL") | 
                (col("macd_signal_trade") == "SELL"), 
                "SELL"
            )
            .otherwise("HOLD")
        )