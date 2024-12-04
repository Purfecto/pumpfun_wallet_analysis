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

class WalletAnalyzer:
   def __init__(self):
       self.session = requests.Session()
       self.session.headers.update({
           'Content-Type': 'application/json'
       })
       # Using public Solana RPC endpoint
       self.rpc_url = "https://api.mainnet-beta.solana.com"

   def get_wallet_info(self, wallet_address):
       """Get basic wallet information"""
       payload = {
           "jsonrpc": "2.0",
           "id": "1",
           "method": "getAccountInfo",
           "params": [
               wallet_address,
               {"encoding": "jsonParsed"}
           ]
       }
       
       try:
           response = self.session.post(self.rpc_url, json=payload)
           return response.json() if response.status_code == 200 else None
       except Exception as e:
           logger.error(f"Error fetching wallet info: {str(e)}")
           return None

   def get_token_accounts(self, wallet_address):
       """Get token accounts owned by wallet"""
       payload = {
           "jsonrpc": "2.0",
           "id": "1",
           "method": "getTokenAccountsByOwner",
           "params": [
               wallet_address,
               {
                   "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
               },
               {"encoding": "jsonParsed"}
           ]
       }
       
       try:
           response = self.session.post(self.rpc_url, json=payload)
           return response.json() if response.status_code == 200 else None
       except Exception as e:
           logger.error(f"Error fetching token accounts: {str(e)}")
           return None

   def analyze_wallet_activity(self, wallet_address, pnl):
       """Comprehensive wallet analysis"""
       wallet_info = self.get_wallet_info(wallet_address)
       token_data = self.get_token_accounts(wallet_address)

       analysis = {
           'wallet': wallet_address,
           'total_pnl': pnl,
           'category': self._categorize_wallet(pnl),
           'token_count': 0,
           'wallet_type': 'Unknown',
           'balance_status': 'Unknown',
           'token_holdings': []
       }

       # Analyze token holdings
       if token_data and 'result' in token_data:
           token_accounts = token_data['result'].get('value', [])
           analysis['token_count'] = len(token_accounts)
           
           # Extract token information
           for account in token_accounts:
               if 'parsed' in account.get('account', {}).get('data', {}):
                   token_info = account['account']['data']['parsed']['info']
                   if float(token_info.get('tokenAmount', {}).get('uiAmount', 0)) > 0:
                       analysis['token_holdings'].append({
                           'mint': token_info.get('mint'),
                           'amount': token_info.get('tokenAmount', {}).get('uiAmount', 0)
                       })

       # Categorize wallet behavior
       analysis['wallet_type'] = self._determine_wallet_type(analysis)
       analysis['balance_status'] = self._check_balance_status(wallet_info)

       return analysis

   def _categorize_wallet(self, pnl):
       if pnl > 10000000:
           return "Whale"
       elif pnl > 5000000:
           return "Large Trader"
       return "Medium Trader"

   def _determine_wallet_type(self, analysis):
       token_count = analysis['token_count']
       if token_count > 20:
           return 'Diversified'
       elif token_count > 5:
           return 'Multi-Token Trader'
       elif token_count > 0:
           return 'Focused Trader'
       return 'Unknown'

   def _check_balance_status(self, wallet_info):
       if not wallet_info or 'result' not in wallet_info:
           return 'Unknown'
       return 'Active' if wallet_info['result'] else 'Inactive'

def main():
   try:
       analyzer = WalletAnalyzer()
       
       # Load wallet data
       input_file = os.path.join(DATA_DIR, 'profitable_wallets_over_1M.csv')
       wallet_df = pd.read_csv(input_file)
       logger.info(f"Loaded {len(wallet_df)} wallets for analysis")
       
       analyses = []
       failed_wallets = []
       
       for idx, row in wallet_df.iterrows():
           wallet = row['wallet']
           pnl = row['total_pnl']
           
           logger.info(f"Analyzing wallet {idx+1}/{len(wallet_df)}: {wallet}")
           
           try:
               analysis = analyzer.analyze_wallet_activity(wallet, pnl)
               analyses.append(analysis)
           except Exception as e:
               logger.error(f"Failed to analyze wallet {wallet}: {str(e)}")
               failed_wallets.append(wallet)
           
           # Save progress every 10 wallets
           if (idx + 1) % 10 == 0:
               progress_df = pd.DataFrame(analyses)
               progress_df.to_csv(os.path.join(DATA_DIR, 'analysis_progress.csv'), index=False)
               logger.info(f"Progress saved: {idx+1}/{len(wallet_df)} wallets analyzed")
           
           # Rate limiting
           time.sleep(0.1)
       
       # Save final results
       final_df = pd.DataFrame(analyses)
       final_df.to_csv(os.path.join(DATA_DIR, 'wallet_analysis_final.csv'), index=False)
       
       if failed_wallets:
           with open(os.path.join(DATA_DIR, 'failed_wallets.txt'), 'w') as f:
               f.write('\n'.join(failed_wallets))
       
       # Print summary
       print("\nAnalysis Summary:")
       print(f"Total wallets analyzed: {len(final_df)}")
       print(f"Failed analyses: {len(failed_wallets)}")
       
       print("\nWallet Categories:")
       print(final_df['category'].value_counts())
       
       print("\nWallet Types:")
       print(final_df['wallet_type'].value_counts())
       
       print("\nBalance Status:")
       print(final_df['balance_status'].value_counts())
       
       print("\nToken Statistics:")
       print(f"Average tokens per wallet: {final_df['token_count'].mean():.2f}")
       print(f"Max tokens in a wallet: {final_df['token_count'].max()}")
       
   except Exception as e:
       logger.error(f"Analysis failed: {str(e)}")
       raise

if __name__ == "__main__":
   main()