# Phase 1: Smart Token Data Collector for Free Tier RPC
# Monitors promising tokens efficiently within rate limits

import asyncio
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional
from dataclasses import dataclass
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
import aiohttp
import sqlite3  # Using SQLite for simplicity, upgrade to PostgreSQL later

# Configuration
MAX_MONITORED_TOKENS = 30  # Maximum tokens to monitor simultaneously
PRICE_CHECK_INTERVAL = 300  # 5 minutes
INITIAL_CHECK_INTERVAL = 60  # 1 minute for first hour
TOKEN_LIFETIME_DAYS = 7  # Monitor tokens for 7 days
MIN_INITIAL_LIQUIDITY_SOL = 3  # Minimum liquidity to start monitoring

# Jupiter Price API (free)
JUPITER_PRICE_API = "https://price.jup.ag/v4/price"

@dataclass
class Token:
    """Token being monitored"""
    mint: str
    name: str
    symbol: str
    created_at: datetime
    platform: str  # 'pump_fun' or 'raydium'
    creation_tx: str
    monitoring_tier: int = 1  # 1=high priority, 2=medium, 3=low
    last_price_check: Optional[datetime] = None
    initial_metrics: Optional[Dict] = None


class SmartTokenCollector:
    def __init__(self, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        self.rpc_url = rpc_url
        self.client = None
        self.monitored_tokens: Dict[str, Token] = {}
        self.db_conn = None
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database"""
        self.db_conn = sqlite3.connect('memecoin_data.db')
        cursor = self.db_conn.cursor()
        
        # Create tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint TEXT UNIQUE NOT NULL,
            name TEXT,
            symbol TEXT,
            created_at TIMESTAMP,
            platform TEXT,
            creation_tx TEXT,
            initial_liquidity_sol REAL,
            initial_holders INTEGER,
            status TEXT DEFAULT 'active'
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            token_id INTEGER,
            timestamp TIMESTAMP,
            price_sol REAL,
            price_usd REAL,
            market_cap_usd REAL,
            volume_24h REAL,
            liquidity_sol REAL,
            holder_count INTEGER,
            tx_count_5min INTEGER,
            buy_count_5min INTEGER,
            sell_count_5min INTEGER,
            largest_buy_5min REAL,
            monitoring_tier INTEGER,
            FOREIGN KEY (token_id) REFERENCES tokens (id),
            PRIMARY KEY (token_id, timestamp)
        )
        ''')
        
        self.db_conn.commit()
    
    async def should_monitor_token(self, token_data: Dict) -> bool:
        """Decide if a token is worth monitoring based on initial metrics"""
        # This is called ~1 hour after token creation
        # Check key metrics to decide if worth monitoring
        
        initial_checks = {
            'min_liquidity': token_data.get('liquidity_sol', 0) >= MIN_INITIAL_LIQUIDITY_SOL,
            'min_holders': token_data.get('holder_count', 0) >= 20,
            'min_transactions': token_data.get('tx_count', 0) >= 50,
            'not_rugged': token_data.get('liquidity_change', 0) > -50,  # Liquidity not removed
            'active_trading': token_data.get('volume_1h', 0) > 10,  # 10 SOL volume
        }
        
        # Need at least 3 out of 5 criteria
        passed_checks = sum(initial_checks.values())
        
        if passed_checks >= 3:
            print(f"✅ Token passes {passed_checks}/5 checks: {initial_checks}")
            return True
        else:
            print(f"❌ Token fails with {passed_checks}/5 checks: {initial_checks}")
            return False
    
    def assign_monitoring_tier(self, token_data: Dict) -> int:
        """Assign monitoring priority tier based on performance"""
        score = 0
        
        # Scoring criteria
        if token_data.get('liquidity_sol', 0) > 20: score += 3
        elif token_data.get('liquidity_sol', 0) > 10: score += 2
        elif token_data.get('liquidity_sol', 0) > 5: score += 1
        
        if token_data.get('holder_count', 0) > 100: score += 3
        elif token_data.get('holder_count', 0) > 50: score += 2
        elif token_data.get('holder_count', 0) > 25: score += 1
        
        if token_data.get('volume_1h', 0) > 100: score += 3
        elif token_data.get('volume_1h', 0) > 50: score += 2
        elif token_data.get('volume_1h', 0) > 20: score += 1
        
        # Assign tier
        if score >= 7: return 1  # Check every 5 minutes
        elif score >= 4: return 2  # Check every 15 minutes
        else: return 3  # Check every 30 minutes
    
    async def add_token_to_monitor(self, token: Token, initial_data: Dict):
        """Add a token to monitoring if we have capacity"""
        # Check if we're at capacity
        if len(self.monitored_tokens) >= MAX_MONITORED_TOKENS:
            # Remove oldest tier 3 token to make room
            tier_3_tokens = [t for t in self.monitored_tokens.values() if t.monitoring_tier == 3]
            if tier_3_tokens:
                oldest = min(tier_3_tokens, key=lambda t: t.created_at)
                del self.monitored_tokens[oldest.mint]
                print(f"📤 Removed {oldest.symbol} to make room")
        
        # Add to monitoring
        self.monitored_tokens[token.mint] = token
        
        # Save to database
        cursor = self.db_conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO tokens (mint, name, symbol, created_at, platform, creation_tx, initial_liquidity_sol, initial_holders)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (token.mint, token.name, token.symbol, token.created_at, token.platform, 
              token.creation_tx, initial_data.get('liquidity_sol', 0), initial_data.get('holder_count', 0)))
        self.db_conn.commit()
        
        print(f"📊 Now monitoring {token.symbol} (Tier {token.monitoring_tier}) - Total: {len(self.monitored_tokens)}")
    
    async def get_token_price_data(self, mint: str) -> Optional[Dict]:
        """Get current price and metrics for a token"""
        try:
            # Use Jupiter Price API (free and reliable)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{JUPITER_PRICE_API}?ids={mint}") as response:
                    if response.status == 200:
                        data = await response.json()
                        if 'data' in data and mint in data['data']:
                            price_data = data['data'][mint]
                            return {
                                'price_usd': price_data.get('price', 0),
                                'price_sol': price_data.get('price', 0) / 50,  # Rough SOL conversion
                                'timestamp': datetime.now()
                            }
            
            # If Jupiter doesn't have it, try direct from blockchain
            # This is where you'd calculate from pool reserves
            return None
            
        except Exception as e:
            print(f"Error getting price for {mint}: {e}")
            return None
    
    async def collect_token_metrics(self, token: Token) -> Dict:
        """Collect comprehensive metrics for a token"""
        metrics = {
            'timestamp': datetime.now(),
            'mint': token.mint,
            'monitoring_tier': token.monitoring_tier
        }
        
        # Get price data
        price_data = await self.get_token_price_data(token.mint)
        if price_data:
            metrics.update(price_data)
        
        # Get additional metrics (simplified for free tier)
        # In production, you'd get these from blockchain
        metrics.update({
            'holder_count': 0,  # Would query token accounts
            'liquidity_sol': 0,  # Would check pool reserves
            'volume_24h': 0,  # Would sum recent transactions
            'tx_count_5min': 0,  # Would count recent transactions
        })
        
        return metrics
    
    async def save_metrics(self, token_id: int, metrics: Dict):
        """Save metrics to database"""
        cursor = self.db_conn.cursor()
        cursor.execute('''
        INSERT INTO price_history 
        (token_id, timestamp, price_sol, price_usd, market_cap_usd, volume_24h, 
         liquidity_sol, holder_count, tx_count_5min, monitoring_tier)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            token_id, metrics['timestamp'], 
            metrics.get('price_sol', 0), metrics.get('price_usd', 0),
            metrics.get('market_cap_usd', 0), metrics.get('volume_24h', 0),
            metrics.get('liquidity_sol', 0), metrics.get('holder_count', 0),
            metrics.get('tx_count_5min', 0), metrics.get('monitoring_tier', 3)
        ))
        self.db_conn.commit()
    
    async def monitoring_loop(self):
        """Main monitoring loop"""
        print("🔄 Starting monitoring loop...")
        
        while True:
            current_time = datetime.now()
            
            # Check each token based on its tier
            for mint, token in list(self.monitored_tokens.items()):
                # Skip if token is too old
                if (current_time - token.created_at).days > TOKEN_LIFETIME_DAYS:
                    del self.monitored_tokens[mint]
                    print(f"⏰ {token.symbol} aged out (7 days)")
                    continue
                
                # Check if it's time to collect data based on tier
                check_interval = {
                    1: 300,   # 5 minutes
                    2: 900,   # 15 minutes
                    3: 1800   # 30 minutes
                }.get(token.monitoring_tier, 1800)
                
                if token.last_price_check is None or \
                   (current_time - token.last_price_check).seconds >= check_interval:
                    
                    # Collect metrics
                    metrics = await self.collect_token_metrics(token)
                    
                    # Get token ID from database
                    cursor = self.db_conn.cursor()
                    cursor.execute("SELECT id FROM tokens WHERE mint = ?", (mint,))
                    result = cursor.fetchone()
                    if result:
                        await self.save_metrics(result[0], metrics)
                        token.last_price_check = current_time
                        print(f"📈 Updated {token.symbol} - Price: ${metrics.get('price_usd', 0):.6f}")
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.5)
            
            # Wait before next loop
            await asyncio.sleep(30)  # Check every 30 seconds
    
    async def run(self):
        """Main execution"""
        self.client = AsyncClient(self.rpc_url)
        
        try:
            # In a real implementation, you'd have:
            # 1. Token discovery feeding into this
            # 2. Initial evaluation after 1 hour
            # 3. Dynamic tier adjustment
            
            await self.monitoring_loop()
            
        finally:
            await self.client.close()
            self.db_conn.close()


# Example of how token discovery would feed into this system
async def on_new_token_found(token_data: Dict, collector: SmartTokenCollector):
    """Called when pump_fun_scanner finds a new token"""
    token = Token(
        mint=token_data['mint'],
        name=token_data.get('name', 'Unknown'),
        symbol=token_data.get('symbol', 'Unknown'),
        created_at=datetime.now(),
        platform='pump_fun',
        creation_tx=token_data['signature']
    )
    
    # Wait 1 hour to see initial performance
    await asyncio.sleep(3600)
    
    # Get initial metrics
    initial_metrics = await collector.collect_token_metrics(token)
    
    # Decide if worth monitoring
    if await collector.should_monitor_token(initial_metrics):
        token.monitoring_tier = collector.assign_monitoring_tier(initial_metrics)
        await collector.add_token_to_monitor(token, initial_metrics)


if __name__ == "__main__":
    collector = SmartTokenCollector()
    asyncio.run(collector.run())