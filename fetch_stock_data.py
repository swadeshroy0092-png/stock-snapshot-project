import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# --- Configuration ---

# 1. Your list of tickers
USER_TICKERS = [
    "HINDALCO", "ICICIBANK", "BHARTIARTL", "SHRIRAMFIN", "ONGC", "BEL", 
    "NESTLEIND", "ITC", "SUNPHARMA", "BAJAJ-AUTO", "COALINDIA", "DRREDDY", 
    "RELIANCE", "TATASTEEL", "JSWSTEEL", "HCLTECH", "TRENT", "ASIANPAINT", 
    "M&M", "INFY", "POWERGRID", "LT", "TCS", "INDIGO", "SBILIFE", "WIPRO", 
    "BAJFINANCE", "ETERNAL", "TMPV", "TATACONSUM", "EICHERMOT", "TECHM", 
    "GRASIM", "MARUTI", "BAJAJFINSV", "NTPC", "SBIN", "JIOFIN", "HDFCLIFE", 
    "HDFCBANK", "AXISBANK", "TITAN", "KOTAKBANK", "ADANIENT", "APOLLOHOSP", 
    "ADANIPORTS", "ULTRACEMCO", "MAXHEALTH", "HINDUNILVR", "CIPLA"
]

# 2. Add ".NS" suffix for NSE tickers
TICKERS_NS = [t + ".NS" for t in USER_TICKERS]

# 3. The data points you want (based on your SBIN example)
# We will use .get() to avoid errors if a key doesn't exist for a ticker
KEYS_TO_EXTRACT = [
    'longName', 'industry', 'sector', 'fullExchangeName', 'website', 
    'city', 'marketCap', 'regularMarketTime', 'currentPrice', 'open', 
    'dayHigh', 'dayLow', 'volume', 'previousClose', 'regularMarketChange', 
    'regularMarketChangePercent', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 
    'fiftyDayAverage', 'twoHundredDayAverage', 'averageVolume', 
    'sharesOutstanding', 'heldPercentInsiders', 'heldPercentInstitutions',
    'totalCash', 'totalDebt', 'totalRevenue', 'returnOnEquity', 'profitMargins',
    'bookValue', 'priceToBook', 'trailingPE', 'forwardPE', 'trailingEps', 
    'dividendYield', 'payoutRatio', 'lastSplitFactor', 'lastSplitDate', 'beta'
]

OUTPUT_CSV = "stock_snapshot_data.csv"
# ---------------------

def fetch_snapshot_data():
    all_stock_data = []
    # Get a single timestamp for this entire batch of data
    fetch_timestamp = datetime.now().isoformat()
    
    print(f"Starting data fetch for {len(TICKERS_NS)} tickers...")

    for ticker_symbol in TICKERS_NS:
        try:
            print(f"Fetching: {ticker_symbol}")
            ticker_obj = yf.Ticker(ticker_symbol)
            info = ticker_obj.info

            # Build a dictionary for this one ticker
            stock_data = {key: info.get(key, None) for key in KEYS_TO_EXTRACT}
            
            # Add our own custom fields
            stock_data['ticker'] = ticker_symbol # Add the ticker symbol itself
            stock_data['fetchTimestamp'] = fetch_timestamp # Add our timestamp

            all_stock_data.append(stock_data)

        except Exception as e:
            # This will catch errors for invalid tickers (like ETERNAL.NS, TMPV.NS)
            # The script will print the error and continue with the next ticker
            print(f"--- FAILED to fetch data for {ticker_symbol}. Error: {e}")
            continue
    
    if not all_stock_data:
        print("No data was fetched. Exiting.")
        return

    print("Data fetch complete. Converting to DataFrame...")
    
    # Convert our list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(all_stock_data)
    
    # Re-order columns to put 'ticker' and 'fetchTimestamp' first
    cols = ['fetchTimestamp', 'ticker'] + [col for col in df.columns if col not in ['fetchTimestamp', 'ticker']]
    df = df[cols]

    # Check if the file already exists
    file_exists = os.path.exists(OUTPUT_CSV)
    
    if file_exists:
        print(f"Appending data to existing {OUTPUT_CSV}")
        # Append to the CSV without writing the header
        df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
    else:
        print(f"Creating new file: {OUTPUT_CSV}")
        # Create the file and write the header
        df.to_csv(OUTPUT_CSV, mode='w', header=True, index=False)
        
    print(f"Script finished. Added {len(df)} rows to {OUTPUT_CSV}.")

if __name__ == "__main__":
    fetch_snapshot_data()