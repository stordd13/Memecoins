# Pump.fun Memecoin Scanner
# Monitors new token launches on Pump.fun platform

import asyncio
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from solders.signature import Signature
import base58
import json
from datetime import datetime
import struct

# Pump.fun Program IDs
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  # Main Pump.fun program
PUMP_FUN_TOKEN_MINT_AUTHORITY = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"  # Token mint authority

# System programs
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
RENT_PROGRAM = "SysvarRent111111111111111111111111111111111"
EVENT_AUTHORITY = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
SOL_MINT = "So11111111111111111111111111111111111111112"

# Instruction discriminators for Pump.fun
# These are the first 8 bytes of different instruction types
CREATE_DISCRIMINATOR = bytes([0x18, 0x1e, 0xc8, 0x50, 0x51, 0xa5, 0x86, 0x26])  # Create new token
BUY_DISCRIMINATOR = bytes([0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea])     # Buy tokens
SELL_DISCRIMINATOR = bytes([0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad])    # Sell tokens


async def decode_pump_fun_metadata(data: bytes) -> dict:
    """Decode Pump.fun token metadata from instruction data"""
    try:
        # Skip discriminator (8 bytes)
        offset = 8
        
        # Read name (string)
        name_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        name = data[offset:offset+name_len].decode('utf-8', errors='ignore')
        offset += name_len
        
        # Read symbol (string)
        symbol_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        symbol = data[offset:offset+symbol_len].decode('utf-8', errors='ignore')
        offset += symbol_len
        
        # Read URI (metadata URI)
        uri_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        uri = data[offset:offset+uri_len].decode('utf-8', errors='ignore')
        
        return {
            'name': name.strip(),
            'symbol': symbol.strip(),
            'uri': uri.strip()
        }
    except Exception as e:
        print(f"Error decoding metadata: {e}")
        return None


async def parse_pump_fun_transaction(signature_str, client):
    """Parse a Pump.fun transaction to identify token creations"""
    try:
        sig = Signature.from_string(signature_str)
        
        # Get transaction
        tx_response = await client.get_transaction(
            sig,
            encoding="json",
            max_supported_transaction_version=0
        )
        
        if not tx_response.value:
            return None
            
        tx = tx_response.value
        
        # Get the transaction message
        if hasattr(tx, 'transaction') and hasattr(tx.transaction, 'message'):
            message = tx.transaction.message
            
            # Look through instructions
            if hasattr(message, 'instructions'):
                for inst in message.instructions:
                    # Get program ID
                    program_idx = inst.program_id_index
                    if program_idx < len(message.account_keys):
                        program_id = str(message.account_keys[program_idx])
                        
                        # Check if it's a Pump.fun instruction
                        if program_id == PUMP_FUN_PROGRAM:
                            # Decode instruction data
                            if hasattr(inst, 'data'):
                                try:
                                    inst_data = base58.b58decode(inst.data)
                                    discriminator = inst_data[:8]
                                    
                                    # Check if it's a CREATE instruction
                                    if discriminator == CREATE_DISCRIMINATOR:
                                        print(f"\n🎯 Found token creation on Pump.fun!")
                                        
                                        # Decode metadata
                                        metadata = await decode_pump_fun_metadata(inst_data)
                                        
                                        # Get token mint address (usually first account)
                                        if inst.accounts and len(inst.accounts) > 0:
                                            mint_idx = inst.accounts[0]
                                            if mint_idx < len(message.account_keys):
                                                mint_address = str(message.account_keys[mint_idx])
                                                
                                                result = {
                                                    'mint': mint_address,
                                                    'signature': signature_str,
                                                    'slot': tx.slot,
                                                    'timestamp': datetime.now().isoformat()
                                                }
                                                
                                                if metadata:
                                                    result.update(metadata)
                                                
                                                return result
                                    
                                    elif discriminator == BUY_DISCRIMINATOR:
                                        # This is a buy transaction
                                        pass
                                    elif discriminator == SELL_DISCRIMINATOR:
                                        # This is a sell transaction
                                        pass
                                    
                                except Exception as e:
                                    pass
        
        return None
        
    except Exception as e:
        return None


async def scan_pump_fun_launches(duration_seconds=60):
    """Monitor Pump.fun for new token launches"""
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    
    print("🚀 Pump.fun Token Scanner")
    print("=" * 60)
    print(f"Monitoring for {duration_seconds} seconds...")
    print(f"Started at: {datetime.now()}\n")
    
    found_tokens = []
    seen_signatures = set()
    start_time = datetime.now()
    check_count = 0
    
    try:
        while (datetime.now() - start_time).seconds < duration_seconds:
            check_count += 1
            
            # Get recent Pump.fun transactions
            pump_pubkey = Pubkey.from_string(PUMP_FUN_PROGRAM)
            sigs_response = await client.get_signatures_for_address(
                pump_pubkey,
                limit=20  # Check last 20 transactions
            )
            
            if sigs_response.value:
                new_count = 0
                
                for sig_info in sigs_response.value:
                    sig_str = str(sig_info.signature)
                    
                    # Skip if we've seen this signature
                    if sig_str in seen_signatures:
                        continue
                    
                    seen_signatures.add(sig_str)
                    
                    # Parse transaction
                    result = await parse_pump_fun_transaction(sig_str, client)
                    
                    if result:
                        new_count += 1
                        found_tokens.append(result)
                        
                        print(f"\n🎉 NEW PUMP.FUN TOKEN DETECTED!")
                        print(f"Name: {result.get('name', 'Unknown')}")
                        print(f"Symbol: {result.get('symbol', 'Unknown')}")
                        print(f"Mint: {result['mint']}")
                        print(f"URI: {result.get('uri', 'No metadata')[:50]}...")
                        print(f"Transaction: {result['signature']}")
                        print(f"Time: {result['timestamp']}")
                        print("=" * 60)
                        
                        # Get additional token info
                        await analyze_pump_fun_token(result['mint'], client)
                    
                    # Small delay to avoid rate limits
                    await asyncio.sleep(0.1)
                
                if new_count == 0:
                    print(f"\rCheck #{check_count}: No new tokens... (checked {len(sigs_response.value)} txs)", end="", flush=True)
            
            # Wait before next check
            await asyncio.sleep(5)
    
    except KeyboardInterrupt:
        print("\n\nStopping monitor...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        await client.close()
    
    # Summary
    print(f"\n\n📊 Scan Complete!")
    print(f"Duration: {(datetime.now() - start_time).seconds} seconds")
    print(f"Tokens found: {len(found_tokens)}")
    
    if found_tokens:
        print("\n🏆 Tokens discovered:")
        for token in found_tokens:
            print(f"\n- {token.get('name', 'Unknown')} ({token.get('symbol', 'Unknown')})")
            print(f"  Mint: {token['mint']}")
            print(f"  Time: {token['timestamp']}")
    
    return found_tokens


async def analyze_pump_fun_token(mint_address: str, client):
    """Get additional information about a Pump.fun token"""
    try:
        # Get token supply
        mint_pubkey = Pubkey.from_string(mint_address)
        supply_response = await client.get_token_supply(mint_pubkey)
        
        if supply_response.value:
            supply = supply_response.value.ui_amount
            decimals = supply_response.value.decimals
            
            print(f"\n📈 Token Details:")
            print(f"   Supply: {supply:,.0f}")
            print(f"   Decimals: {decimals}")
            
            # Check initial liquidity/bonding curve
            # Pump.fun uses a bonding curve mechanism
            print(f"   Platform: Pump.fun (Bonding Curve)")
            
    except Exception as e:
        print(f"   Could not fetch token details: {e}")


async def get_pump_fun_trading_activity(mint_address: str, client, duration_seconds=60):
    """Monitor trading activity for a specific Pump.fun token"""
    print(f"\n📊 Monitoring trades for {mint_address[:8]}...")
    
    start_time = datetime.now()
    trades = []
    
    # Get the token's associated Pump.fun account
    # This would contain the bonding curve state
    
    while (datetime.now() - start_time).seconds < duration_seconds:
        # Monitor for buy/sell transactions
        # This is simplified - you'd want to track the specific token's trades
        
        await asyncio.sleep(5)
    
    return trades


async def main():
    """Main execution"""
    print("Pump.fun Memecoin Scanner")
    print("=" * 60)
    print("\nOptions:")
    print("1. Monitor for new launches (recommended)")
    print("2. Analyze recent launches")
    print("3. Track specific token")
    
    # For this example, we'll monitor for new launches
    await scan_pump_fun_launches(duration_seconds=300)  # Monitor for 5 minutes


# Additional utility functions
def calculate_pump_fun_price(virtual_sol_reserves: float, virtual_token_reserves: float) -> float:
    """Calculate current price from Pump.fun bonding curve"""
    if virtual_token_reserves == 0:
        return 0
    return virtual_sol_reserves / virtual_token_reserves


def estimate_market_cap(price_per_token: float, total_supply: float) -> float:
    """Estimate market cap in SOL"""
    return price_per_token * total_supply


if __name__ == "__main__":
    asyncio.run(main())