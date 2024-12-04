import os
import pandas as pd
import logging
from datetime import datetime

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s',
   handlers=[
       logging.FileHandler(os.path.join(DATA_DIR, 'pattern_analysis.log')),
       logging.StreamHandler()
   ]
)
logger = logging.getLogger(__name__)

class WalletPatternAnalyzer:
   def analyze_token_holdings(self, holdings_str):
       try:
           if not holdings_str or holdings_str == '[]':
               return []

           holdings = eval(holdings_str)
           patterns = []
           
           # Token type analysis
           pump_tokens = [t for t in holdings if 'pump' in str(t['mint']).lower()]
           meme_tokens = [t for t in holdings if any(x in str(t['mint']).lower() for x in ['pepe', 'doge', 'shib', 'wojak', 'chad', 'elon'])]
           high_value_tokens = [t for t in holdings if float(t.get('amount', 0)) > 1000000]
           
           # Identify trading patterns
           if pump_tokens:
               pump_volume = sum(float(t.get('amount', 0)) for t in pump_tokens)
               patterns.append(f"Pump Specialist ({len(pump_tokens)} tokens, {pump_volume:,.0f} volume)")
           
           if meme_tokens:
               patterns.append(f"Meme Trader ({len(meme_tokens)} tokens)")
           
           if high_value_tokens:
               patterns.append(f"Whale Positions ({len(high_value_tokens)} large holdings)")
           
           # Portfolio diversity
           if len(holdings) > 100:
               patterns.append("Super Diversified")
           elif len(holdings) > 50:
               patterns.append("Highly Diversified")
           elif len(holdings) > 20:
               patterns.append("Moderately Diversified")
           elif len(holdings) > 5:
               patterns.append("Focused Trader")
           else:
               patterns.append("Concentrated Positions")
               
           return patterns
       except Exception as e:
           logger.error(f"Error analyzing holdings: {str(e)}")
           return []

   def get_wallet_profile(self, row):
       """Generate comprehensive wallet profile"""
       pnl = float(row['total_pnl'])
       
       # Base categorization
       if pnl > 10000000:
           category = "Mega Whale"
       elif pnl > 5000000:
           category = "Whale"
       else:
           category = "Large Trader"

       # Get trading patterns
       patterns = self.analyze_token_holdings(row.get('token_holdings', '[]'))
       
       # Determine primary trading style
       if 'Super Diversified' in patterns:
           style = 'Super Diversified'
       elif 'Highly Diversified' in patterns:
           style = 'Highly Diversified'
       elif 'Moderately Diversified' in patterns:
           style = 'Moderately Diversified'
       elif 'Focused Trader' in patterns:
           style = 'Focused Trader'
       elif 'Concentrated Positions' in patterns:
           style = 'Concentrated Trader'
       else:
           style = 'Unknown'

       # Remove style from patterns to avoid duplication
       patterns = [p for p in patterns if p not in 
                  ['Super Diversified', 'Highly Diversified', 
                   'Moderately Diversified', 'Focused Trader', 
                   'Concentrated Positions']]

       return {
           'wallet_address': row['wallet'],
           'total_pnl': pnl,
           'category': category,
           'trading_style': style,
           'token_count': len(eval(row.get('token_holdings', '[]'))) if 'token_holdings' in row else 0,
           'patterns': ' | '.join(patterns) if patterns else 'None Detected',
           'last_analyzed': datetime.now().strftime('%Y-%m-%d')
       }

def main():
   try:
       logger.info("Starting pattern analysis...")
       
       # Load previous analysis data
       input_file = os.path.join(DATA_DIR, 'analysis_progress.csv')
       df = pd.read_csv(input_file)
       logger.info(f"Loaded {len(df)} wallets for analysis")
       
       # Analyze patterns
       analyzer = WalletPatternAnalyzer()
       wallet_profiles = []
       
       for idx, row in df.iterrows():
           logger.info(f"Analyzing patterns for wallet {idx+1}/{len(df)}")
           profile = analyzer.get_wallet_profile(row)
           wallet_profiles.append(profile)
           
           # Save progress every 50 wallets
           if (idx + 1) % 50 == 0:
               progress_df = pd.DataFrame(wallet_profiles)
               progress_df.to_csv(os.path.join(DATA_DIR, 'patterns_progress.csv'), index=False)
       
       # Create final dataframe and save
       results_df = pd.DataFrame(wallet_profiles)
       results_df.to_csv(os.path.join(DATA_DIR, 'patterns.csv'), index=False)
       
       # Generate summary statistics
       print("\nPattern Analysis Summary:")
       print(f"Total wallets analyzed: {len(results_df)}")
       
       print("\nCategories:")
       print(results_df['category'].value_counts())
       
       print("\nTrading Styles:")
       print(results_df['trading_style'].value_counts())
       
       print("\nToken Count Statistics:")
       print(f"Average tokens per wallet: {results_df['token_count'].mean():.1f}")
       print(f"Max tokens in wallet: {results_df['token_count'].max()}")
       
       # Pattern frequency analysis
       patterns = results_df[results_df['patterns'] != 'None Detected']['patterns'].str.split(' | ').explode()
       if not patterns.empty:
           print("\nMost Common Patterns:")
           print(patterns.value_counts().head(10))
       
   except Exception as e:
       logger.error(f"Analysis failed: {str(e)}")
       raise

if __name__ == "__main__":
   main()