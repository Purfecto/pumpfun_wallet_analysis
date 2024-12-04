import os
import pandas as pd
import logging
from datetime import datetime

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
TRADERS_DIR = os.path.join(BASE_DIR, 'traders')
os.makedirs(TRADERS_DIR, exist_ok=True)

logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s',
   handlers=[
       logging.FileHandler(os.path.join(TRADERS_DIR, 'special_wallets.log')),
       logging.StreamHandler()
   ]
)
logger = logging.getLogger(__name__)

def extract_special_wallets():
   # Load patterns CSV
   patterns_file = os.path.join(DATA_DIR, 'patterns.csv')
   patterns_df = pd.read_csv(patterns_file)
   
   # Different categories of special wallets
   special_wallets = {
       'super_diversified': patterns_df[patterns_df['trading_style'] == 'Super Diversified'],
       'high_volume_traders': patterns_df[patterns_df['token_count'] > 100],
       'mega_whales': patterns_df[patterns_df['category'] == 'Mega Whale'],
       'pump_specialists': patterns_df[patterns_df['patterns'].str.contains('Pump', na=False)],
       'whale_positions': patterns_df[patterns_df['patterns'].str.contains('Whale Positions', na=False)]
   }
   
   # Create detailed analysis for each category
   for category, wallets in special_wallets.items():
       if len(wallets) > 0:
           output_file = os.path.join(TRADERS_DIR, f'{category}_analysis.csv')
           wallets.to_csv(output_file, index=False)
           
           print(f"\n{category.replace('_', ' ').title()} Analysis:")
           print(f"Number of wallets: {len(wallets)}")
           print(f"Average PNL: ${wallets['total_pnl'].mean():,.2f}")
           print(f"Average token count: {wallets['token_count'].mean():.1f}")
           print("\nTop 5 by PNL:")
           top_5 = wallets.nlargest(5, 'total_pnl')[['wallet_address', 'total_pnl', 'trading_style', 'patterns']]
           print(top_5.to_string())
           
   # Find overlap between categories
   print("\nWallet Category Overlap Analysis:")
   mega_whale_addresses = set(special_wallets['mega_whales']['wallet_address'])
   pump_specialist_addresses = set(special_wallets['pump_specialists']['wallet_address'])
   whale_position_addresses = set(special_wallets['whale_positions']['wallet_address'])
   
   # Calculate overlaps
   mega_whale_and_pump = mega_whale_addresses.intersection(pump_specialist_addresses)
   mega_whale_and_whale_pos = mega_whale_addresses.intersection(whale_position_addresses)
   pump_and_whale_pos = pump_specialist_addresses.intersection(whale_position_addresses)
   
   print(f"Mega Whales who are Pump Specialists: {len(mega_whale_and_pump)}")
   print(f"Mega Whales with Whale Positions: {len(mega_whale_and_whale_pos)}")
   print(f"Pump Specialists with Whale Positions: {len(pump_and_whale_pos)}")
   
   # Save overlap analysis
   overlap_data = {
       'category': ['Mega Whale & Pump', 'Mega Whale & Whale Pos', 'Pump & Whale Pos'],
       'count': [len(mega_whale_and_pump), len(mega_whale_and_whale_pos), len(pump_and_whale_pos)],
       'addresses': [list(mega_whale_and_pump), list(mega_whale_and_whale_pos), list(pump_and_whale_pos)]
   }
   overlap_df = pd.DataFrame(overlap_data)
   overlap_df.to_csv(os.path.join(TRADERS_DIR, 'category_overlaps.csv'), index=False)
   
   # Create master list of special wallets
   all_special_wallets = pd.concat(special_wallets.values())
   all_special_wallets = all_special_wallets.drop_duplicates(subset=['wallet_address'])
   master_file = os.path.join(TRADERS_DIR, 'special_wallets_master.csv')
   all_special_wallets.to_csv(master_file, index=False)
   
   return all_special_wallets

def analyze_token_patterns(df):
   """Analyze common patterns among special wallets"""
   # Load the original analysis data to get token holdings
   analysis_file = os.path.join(DATA_DIR, 'analysis_progress.csv')
   full_data = pd.read_csv(analysis_file)
   
   # Merge with special wallets
   special_wallets_with_data = df.merge(full_data, left_on='wallet_address', right_on='wallet', how='left')
   
   patterns = {
       'token_counts': [],
       'holding_sizes': [],
       'common_tokens': set(),
       'trade_frequencies': []
   }
   
   for _, row in special_wallets_with_data.iterrows():
       if 'token_holdings' in row and row['token_holdings'] != '[]':
           holdings = eval(row['token_holdings'])
           patterns['token_counts'].append(len(holdings))
           
           for holding in holdings:
               if float(holding.get('amount', 0)) > 1000000:
                   patterns['holding_sizes'].append(float(holding['amount']))
               patterns['common_tokens'].add(holding['mint'])
   
   # Save detailed token analysis
   token_analysis = {
       'token': list(patterns['common_tokens']),
       'frequency': [patterns['token_counts'].count(token) for token in patterns['common_tokens']]
   }
   token_df = pd.DataFrame(token_analysis)
   token_df = token_df.sort_values('frequency', ascending=False)
   token_df.to_csv(os.path.join(TRADERS_DIR, 'token_analysis.csv'), index=False)
   
   return patterns

def main():
   try:
       logger.info("Starting special wallet analysis...")
       
       # Extract and analyze special wallets
       special_wallets = extract_special_wallets()
       
       # Analyze token patterns
       patterns = analyze_token_patterns(special_wallets)
       
       print("\nDetailed Token Analysis:")
       print(f"Total unique tokens held: {len(patterns['common_tokens'])}")
       if patterns['token_counts']:
           print(f"Average tokens per wallet: {sum(patterns['token_counts'])/len(patterns['token_counts']):.1f}")
       if patterns['holding_sizes']:
           print(f"Average large position size: {sum(patterns['holding_sizes'])/len(patterns['holding_sizes']):,.0f}")
       
       logger.info("Analysis complete. Results saved to traders directory.")
       
   except Exception as e:
       logger.error(f"Analysis failed: {str(e)}")
       raise

if __name__ == "__main__":
   main()