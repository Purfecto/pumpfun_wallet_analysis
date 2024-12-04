import os
import pandas as pd
from dune_client.client import DuneClient
from datetime import datetime
import logging
import time
import shutil
import json

# Set up directory structure for organized data storage
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRACKING_DIR = os.path.join(BASE_DIR, 'tracking')
BACKUP_DIR = os.path.join(TRACKING_DIR, 'backups')
os.makedirs(TRACKING_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# Configure logging to track program execution and errors
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s',
   handlers=[
       logging.FileHandler(os.path.join(TRACKING_DIR, 'millionaire_tracker.log')),
       logging.StreamHandler()
   ]
)
logger = logging.getLogger(__name__)

class MillionaireTracker:
   def __init__(self):
       """Initialize tracker with API credentials and file paths"""
       self.dune = DuneClient("KMnMS9585gw3DuUAGJsKufBk1eC1xQSs")
       self.query_id = 4364994
       self.history_file = os.path.join(TRACKING_DIR, 'millionaire_history.csv')
       self.current_file = os.path.join(TRACKING_DIR, 'current_millionaires.csv')
       self.stats_file = os.path.join(TRACKING_DIR, 'tracker_statistics.json')

   def fetch_current_data(self):
       """Fetch and validate current data from Dune Analytics with retry logic"""
       max_retries = 3
       retry_delay = 5
       
       for attempt in range(max_retries):
           try:
               query_result = self.dune.get_latest_result(self.query_id)
               df = pd.DataFrame(query_result.result.rows)
               if df.empty:
                   raise ValueError("Received empty dataset from Dune")
               return df.sort_values('total_pnl', ascending=False)
           except Exception as e:
               if attempt == max_retries - 1:
                   logger.error(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
                   return None
               logger.warning(f"Attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
               time.sleep(retry_delay)

   def load_history(self):
       """Load historical tracking data with validation"""
       if os.path.exists(self.history_file):
           df = pd.read_csv(self.history_file)
           required_columns = ['wallet', 'total_pnl', 'first_seen', 'last_seen']
           if all(col in df.columns for col in required_columns):
               return df
           else:
               logger.warning("History file missing required columns, creating new history")
       return pd.DataFrame(columns=['wallet', 'total_pnl', 'first_seen', 'last_seen'])

   def backup_data(self):
       """Create timestamped backups of tracking data"""
       timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       for file in [self.history_file, self.current_file, self.stats_file]:
           if os.path.exists(file):
               backup_path = os.path.join(BACKUP_DIR, f'{timestamp}_{os.path.basename(file)}')
               shutil.copy2(file, backup_path)
               logger.info(f"Backup created: {backup_path}")

   def generate_statistics(self, current_data, history_data):
       """Generate comprehensive statistics about millionaire wallets"""
       today = datetime.now().strftime('%Y-%m-%d')
       
       stats = {
           "date": today,
           "total_current_millionaires": len(current_data),
           "historical_total": len(history_data),
           "new_today": len(current_data[~current_data['wallet'].isin(history_data['wallet'])]),
           "pnl_stats": {
               "highest": float(current_data['total_pnl'].max()),
               "average": float(current_data['total_pnl'].mean()),
               "median": float(current_data['total_pnl'].median()),
               "total_combined": float(current_data['total_pnl'].sum())
           },
           "categories": {
               "1M-5M": int(len(current_data[current_data['total_pnl'].between(1000000, 5000000)])),
               "5M-10M": int(len(current_data[current_data['total_pnl'].between(5000000, 10000000)])),
               "10M+": int(len(current_data[current_data['total_pnl'] > 10000000]))
           }
       }
       
       return stats

   def update_tracking(self):
       """Update tracker with latest data and generate reports"""
       try:
           # Fetch and validate current data
           current_data = self.fetch_current_data()
           if current_data is None:
               return
           
           # Filter for millionaires
           millionaires = current_data[current_data['total_pnl'] > 1000000].copy()
           today = datetime.now().strftime('%Y-%m-%d')
           
           # Load and update history
           history = self.load_history()
           
           # Backup existing data before updates
           self.backup_data()
           
           # Update historical records
           for _, row in millionaires.iterrows():
               wallet = row['wallet']
               if wallet in history['wallet'].values:
                   # Update existing wallet
                   history.loc[history['wallet'] == wallet, ['total_pnl', 'last_seen']] = [
                       row['total_pnl'], 
                       today
                   ]
               else:
                   # Add new wallet
                   new_row = pd.DataFrame({
                       'wallet': [wallet],
                       'total_pnl': [row['total_pnl']],
                       'first_seen': [today],
                       'last_seen': [today]
                   })
                   history = pd.concat([history, new_row], ignore_index=True)
           
           # Generate and save statistics
           stats = self.generate_statistics(millionaires, history)
           
           # Save all updated data
           history.to_csv(self.history_file, index=False)
           millionaires.to_csv(self.current_file, index=False)
           with open(self.stats_file, 'w') as f:
               json.dump(stats, f, indent=4)
           
           # Print summary
           print("\nPumpFun Millionaire Tracker Summary")
           print("=" * 40)
           print(f"Total Current Millionaires: {stats['total_current_millionaires']}")
           print(f"Historical Total: {stats['historical_total']}")
           print(f"New Today: {stats['new_today']}")
           print("\nPNL Statistics:")
           print(f"Highest PNL: ${stats['pnl_stats']['highest']:,.2f}")
           print(f"Average PNL: ${stats['pnl_stats']['average']:,.2f}")
           print(f"Median PNL: ${stats['pnl_stats']['median']:,.2f}")
           print(f"Total Combined PNL: ${stats['pnl_stats']['total_combined']:,.2f}")
           
       except Exception as e:
           logger.error(f"Error updating tracker: {str(e)}")
           raise

def main():
   try:
       logger.info("Starting millionaire tracker update...")
       tracker = MillionaireTracker()
       tracker.update_tracking()
       logger.info("Update complete")
       
   except Exception as e:
       logger.error(f"Tracker failed: {str(e)}")
       raise

if __name__ == "__main__":
   main()