import os
import pandas as pd
import requests
import logging
import time
from datetime import datetime

# Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'analysis.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleWalletAnalyzer:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        # Use RPC endpoint instead of REST API
        self.rpc_url = "https://mainnet.helius-rpc.com/?api-key=68ef0900-ddc2-4300-b079-df0db172e839"

    def get_wallet_info(self, wallet_address):
        """Get basic wallet information using JSON-RPC"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getAccountInfo",
            "params": [wallet_address]
        }
        
        try:
            response = self.session.post(self.rpc_url, json=payload)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Error fetching wallet {wallet_address}: {str(e)}")
            return None

    def analyze_wallet(self, wallet_data, pnl):
        """Simple wallet analysis"""
        analysis = {
            'status': 'Success',
            'total_pnl': pnl,
            'category': self._categorize_wallet(pnl),
            'activity_level': self._get_activity_level(wallet_data)
        }
        return analysis

    def _categorize_wallet(self, pnl):
        """Categorize wallet based on PNL"""
        if pnl > 10000000:  # 10M+
            return "Whale"
        elif pnl > 5000000:  # 5M+
            return "Large Trader"
        else:
            return "Medium Trader"

    def _get_activity_level(self, wallet_data):
        """Determine activity level from wallet data"""
        try:
            if wallet_data and wallet_data.get('result') and wallet_data['result'].get('value'):
                return 'Active'
            return 'Unknown'
        except:
            return 'Unknown'

def main():
    try:
        # Initialize analyzer
        analyzer = SimpleWalletAnalyzer()
        
        # Load wallet data
        input_file = os.path.join(DATA_DIR, 'profitable_wallets_over_1M.csv')
        wallet_df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(wallet_df)} wallets for analysis")
        
        analyses = []
        for idx, row in wallet_df.iterrows():
            wallet = row['wallet']
            pnl = row['total_pnl']
            
            logger.info(f"Analyzing wallet {idx+1}/{len(wallet_df)}: {wallet}")
            
            # Get and analyze wallet data
            wallet_data = analyzer.get_wallet_info(wallet)
            analysis = analyzer.analyze_wallet(wallet_data, pnl)
            analysis['wallet'] = wallet
            analyses.append(analysis)
            
            # Save progress every 10 wallets
            if (idx + 1) % 10 == 0:
                progress_df = pd.DataFrame(analyses)
                progress_df.to_csv(os.path.join(DATA_DIR, 'analysis_progress.csv'), index=False)
                logger.info(f"Progress saved: {idx+1}/{len(wallet_df)} wallets analyzed")
            
            time.sleep(0.1)  # Gentle rate limiting
        
        # Save final results
        final_df = pd.DataFrame(analyses)
        final_df.to_csv(os.path.join(DATA_DIR, 'wallet_analysis_final.csv'), index=False)
        
        # Print summary
        print("\nAnalysis Summary:")
        print(f"Total wallets analyzed: {len(final_df)}")
        print("\nWallet Categories:")
        print(final_df['category'].value_counts())
        print("\nActivity Levels:")
        print(final_df['activity_level'].value_counts())
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()