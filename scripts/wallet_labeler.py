import os
import pandas as pd
import requests
import logging
from datetime import datetime

# Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'wallet_labeling.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WalletLabeler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        self.rpc_url = "https://api.mainnet-beta.solana.com"

    def analyze_token_holdings(self, token_data):
        """Analyze token holdings for patterns"""
        if not token_data or 'token_holdings' not in token_data:
            return []

        patterns = []
        tokens = token_data['token_holdings']
        
        # Look for specific patterns in token names
        pump_tokens = [t for t in tokens if 'pump' in str(t['mint']).lower()]
        meme_tokens = [t for t in tokens if any(x in str(t['mint']).lower() for x in ['pepe', 'doge', 'shib', 'wojak', 'chad'])]
        
        if pump_tokens:
            patterns.append(f"Pump Trader ({len(pump_tokens)} tokens)")
        if meme_tokens:
            patterns.append(f"Meme Trader ({len(meme_tokens)} tokens)")
        if len(tokens) > 100:
            patterns.append("Heavy Diversification")
        elif len(tokens) > 50:
            patterns.append("Moderate Diversification")
        
        return patterns

    def get_detailed_labels(self, wallet_data):
        """Generate detailed labels for a wallet"""
        labels = []
        
        # Category based on PNL
        pnl = float(wallet_data['total_pnl'])
        if pnl > 10000000:
            labels.append("Mega Whale")
        elif pnl > 5000000:
            labels.append("Whale")
        else:
            labels.append("Large Trader")

        # Token count based categorization
        token_count = len(wallet_data.get('token_holdings', []))
        if token_count > 0:
            if token_count > 100:
                labels.append("Portfolio Manager")
            elif token_count > 50:
                labels.append("Active Trader")
            else:
                labels.append("Focused Trader")

        # Check for specific trading patterns
        if 'token_holdings' in wallet_data:
            pattern_labels = self.analyze_token_holdings(wallet_data)
            labels.extend(pattern_labels)

        return labels

def main():
    try:
        # Load original data
        input_file = os.path.join(DATA_DIR, 'profitable_wallets_over_1M.csv')
        wallet_df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(wallet_df)} wallets for analysis")

        # Create new labeling structure
        labeled_wallets = []
        labeler = WalletLabeler()

        for idx, row in wallet_df.iterrows():
            wallet_address = row['wallet']
            logger.info(f"Analyzing wallet {idx+1}/{len(wallet_df)}: {wallet_address}")

            wallet_data = {
                'wallet': wallet_address,
                'total_pnl': row['total_pnl'],
                'token_holdings': eval(row.get('token_holdings', '[]')) if 'token_holdings' in row else []
            }

            # Get detailed labels
            labels = labeler.get_detailed_labels(wallet_data)

            labeled_wallet = {
                'wallet_address': wallet_address,
                'total_pnl': wallet_data['total_pnl'],
                'token_count': len(wallet_data.get('token_holdings', [])),
                'primary_category': labels[0] if labels else 'Unknown',
                'trading_style': labels[1] if len(labels) > 1 else 'Unknown',
                'patterns': ', '.join(labels[2:]) if len(labels) > 2 else 'None Detected',
                'detailed_labels': ' | '.join(labels)
            }
            labeled_wallets.append(labeled_wallet)

        # Create and save new labeled dataset
        labeled_df = pd.DataFrame(labeled_wallets)
        output_file = os.path.join(DATA_DIR, 'labeled_wallets_detailed.csv')
        labeled_df.to_csv(output_file, index=False)

        # Print summary statistics
        print("\nLabeling Summary:")
        print(f"Total wallets labeled: {len(labeled_df)}")
        print("\nPrimary Categories:")
        print(labeled_df['primary_category'].value_counts())
        print("\nTrading Styles:")
        print(labeled_df['trading_style'].value_counts())
        print("\nMost Common Patterns:")
        pattern_series = labeled_df['patterns'].str.split(', ').explode()
        print(pattern_series.value_counts().head())

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()