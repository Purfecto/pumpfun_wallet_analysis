PumpFun Wallet Analysis
Tracking and analyzing Solana's millionaire trading wallets - December 2024

Latest Statistics
Our analysis currently tracks 726 millionaire wallets with a combined value of $2.65B in profits. The most successful wallet has achieved $64.9M in gains, with the average wallet maintaining $3.65M in profitable trades.

Why This Project Matters
The PumpFun Wallet Analysis project offers deep insights into successful trading strategies on the Solana blockchain. By examining wallets that have achieved over $1M in profits, we can understand:

How successful traders select and time their trades
The balance between bot and human trading strategies
Portfolio management techniques that lead to consistent profits
Market patterns that indicate potential opportunities

Core Features
Our analysis toolkit provides comprehensive insights through:
Data Collection & Processing

Automated Dune Analytics data retrieval
Real-time wallet performance tracking
Historical trend analysis and storage

Pattern Recognition

Bot vs human trading pattern detection
Token preference analysis
Portfolio diversity measurements
Whale activity monitoring

Risk Management

Success rate calculations
Drawdown analysis
Position sizing patterns
Market exposure metrics

Getting Started
Prerequisites
Before beginning, ensure you have:
bashCopy✓ Python 3.12.6 or higher
✓ Git (optional for version control)
✓ Dune Analytics API key
✓ Stable internet connection
Installation Process

Clone the repository:

bashCopygit clone https://github.com/yourusername/pumpfun_wallet_analysis.git
cd pumpfun_wallet_analysis

Set up your environment:

bashCopypython -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

Configure your settings:
Create a .env file with your API key:

envCopyDUNE_API_KEY=your_dune_api_key
Usage Guide
Basic Analysis
bashCopy# Generate current millionaire list
python scripts/get_millionaires.py

# Analyze trading patterns
python scripts/wallet_analysis.py
Viewing Results
Analysis results are stored in organized directories:
Copypumpfun_wallet_analysis/
├── data/           # Raw data storage
├── traders/        # Categorized analysis
├── tracking/       # Daily monitoring
└── scripts/        # Analysis tools
Support & Troubleshooting
Common questions and solutions:
Q: Why does my IDE show an import warning for dune-client?
A: This is a known IDE issue that doesn't affect functionality.
Q: How often should I run the analysis?
A: Daily runs are recommended for optimal tracking.
Q: Where can I find historical data?
A: Check the tracking/ directory for daily snapshots.
Contributing
We welcome contributions that improve analysis accuracy or add new features. To contribute:

Fork the repository
Create your feature branch
Add your improvements
Submit a detailed pull request

License & Usage
This project is open source for educational purposes only. Not intended for financial advice.

<div align="center">
Connect & Contribute
Report Issues •
Submit PRs •
Read Docs
</div>
Would you like me to adjust any section or add more details to specific areas?