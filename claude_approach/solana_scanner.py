# Working Solana Transaction Parser
# This version properly handles the transaction structure

import asyncio
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from solders.signature import Signature
import base58
import json
from datetime import datetime

# Important addresses
RAYDIUM_AMM_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Raydium pool initialization discriminator (first 8 bytes of instruction)
INIT_POOL_DISCRIMINATOR = bytes([175, 175, 109, 31, 13, 152, 155, 237])

async def parse_raydium_transaction(signature_str):
    """Parse a Raydium transaction to find pool creations"""
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    
    try:
        sig = Signature.from_string(signature_str)
        
        # Get transaction with base64 encoding (more reliable)
        tx_response = await client.get_transaction(
            sig,
            encoding="base64",
            max_supported_transaction_version=0
        )
        
        if not tx_response.value:
            return None
            
        tx = tx_response.value
        
        print(f"\n📋 Analyzing transaction: {signature_str[:32]}...")
        print(f"   Slot: {tx.slot}")
        
        # Get the raw transaction data
        if hasattr(tx, 'transaction'):
            # The transaction is base64 encoded
            if isinstance(tx.transaction, str):
                # It's a base64 string
                import base64
                tx_bytes = base64.b64decode(tx.transaction)
                print(f"   Transaction size: {len(tx_bytes)} bytes")
            
            # Now get the parsed version for easier analysis
            tx_parsed = await client.get_transaction(
                sig,
                encoding="json",  # Use json encoding to get account lists
                max_supported_transaction_version=0
            )
            
            if tx_parsed.value and hasattr(tx_parsed.value, 'transaction'):
                transaction_data = tx_parsed.value.transaction
                
                # Access the message
                if hasattr(transaction_data, 'message'):
                    message = transaction_data.message
                    
                    # Get instructions
                    if hasattr(message, 'instructions'):
                        instructions = message.instructions
                        print(f"   Number of instructions: {len(instructions)}")
                        
                        raydium_found = False
                        
                        # Look for Raydium instructions
                        for i, inst in enumerate(instructions):
                            # Get program index and check if it's Raydium
                            program_idx = inst.program_id_index
                            
                            # Get the actual program ID from account keys
                            if hasattr(message, 'account_keys') and program_idx < len(message.account_keys):
                                program_id = str(message.account_keys[program_idx])
                                
                                if program_id == RAYDIUM_AMM_PROGRAM:
                                    raydium_found = True
                                    print(f"\n   ✅ Found Raydium instruction #{i+1}")
                                    
                                    # Check number of accounts (pool init has many)
                                    if hasattr(inst, 'accounts'):
                                        num_accounts = len(inst.accounts)
                                        print(f"   Accounts involved: {num_accounts}")
                                        
                                        # Pool initialization typically has 17+ accounts
                                        if num_accounts >= 17:
                                            print(f"   🎯 Possible pool initialization!")
                                            
                                            # Try to decode instruction data
                                            if hasattr(inst, 'data'):
                                                try:
                                                    inst_data = base58.b58decode(inst.data)
                                                    discriminator = inst_data[:8]
                                                    
                                                    if discriminator == INIT_POOL_DISCRIMINATOR:
                                                        print(f"   ✨ CONFIRMED: Pool initialization!")
                                                        
                                                        # Extract token addresses
                                                        # In Raydium init, tokens are usually at indices 8 and 9
                                                        if len(inst.accounts) > 9:
                                                            token_a_idx = inst.accounts[8]
                                                            token_b_idx = inst.accounts[9]
                                                            
                                                            if token_a_idx < len(message.account_keys) and token_b_idx < len(message.account_keys):
                                                                token_a = str(message.account_keys[token_a_idx])
                                                                token_b = str(message.account_keys[token_b_idx])
                                                                
                                                                print(f"\n   💎 NEW POOL FOUND!")
                                                                print(f"   Token A: {token_a}")
                                                                print(f"   Token B: {token_b}")
                                                                
                                                                # Identify which is the new token
                                                                if token_a == SOL_MINT:
                                                                    print(f"   New token: {token_b}")
                                                                    return {'new_token': token_b, 'pool_tx': signature_str}
                                                                elif token_b == SOL_MINT:
                                                                    print(f"   New token: {token_a}")
                                                                    return {'new_token': token_a, 'pool_tx': signature_str}
                                                    else:
                                                        print(f"   Different instruction type: {inst_data[:8].hex()}")
                                                        print(f"   (Likely swap, add/remove liquidity, etc.)")
                                                except Exception as e:
                                                    print(f"   Could not decode instruction: {e}")
                                        else:
                                            print(f"   Not a pool init (only {num_accounts} accounts, need 17+)")
                        
                        if not raydium_found:
                            print("   No Raydium instructions in this transaction")
        
        return None
        
    except Exception as e:
        print(f"Error parsing transaction: {e}")
        return None
    finally:
        await client.close()


async def find_new_tokens(num_transactions=50, show_all=True):
    """Scan recent Raydium transactions for new token launches"""
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    new_tokens = []
    
    try:
        print(f"🔍 Scanning {num_transactions} recent Raydium transactions...")
        
        # Get recent signatures
        raydium = Pubkey.from_string(RAYDIUM_AMM_PROGRAM)
        sigs_response = await client.get_signatures_for_address(
            raydium,
            limit=num_transactions
        )
        
        if not sigs_response.value:
            print("No transactions found")
            return []
        
        print(f"Found {len(sigs_response.value)} transactions to analyze\n")
        
        # Track statistics
        stats = {
            'total': len(sigs_response.value),
            'errors': 0,
            'swaps': 0,
            'other': 0,
            'pool_inits': 0
        }
        
        # Analyze each transaction
        for i, sig_info in enumerate(sigs_response.value):
            sig_str = str(sig_info.signature)
            
            if show_all:
                print(f"\n[{i+1}/{len(sigs_response.value)}] Checking transaction...")
            
            result = await parse_raydium_transaction(sig_str)
            
            if result:
                new_tokens.append(result)
                stats['pool_inits'] += 1
                print(f"\n🚀 Found new token launch!")
                
                # Get token info
                await get_token_info(result['new_token'])
            
            # Rate limit to avoid hitting RPC limits
            await asyncio.sleep(0.5)
        
        print(f"\n\n📊 Summary:")
        print(f"   Total transactions scanned: {stats['total']}")
        print(f"   Pool initializations found: {stats['pool_inits']}")
        print(f"   Success rate: {(stats['pool_inits'] / stats['total'] * 100):.1f}% are new pools")
        
        return new_tokens
        
    except Exception as e:
        print(f"Error scanning transactions: {e}")
        return []
    finally:
        await client.close()


async def get_token_info(token_mint_str):
    """Get basic token information"""
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    
    try:
        token_pubkey = Pubkey.from_string(token_mint_str)
        
        # Get token supply
        supply_response = await client.get_token_supply(token_pubkey)
        
        if supply_response.value:
            supply = supply_response.value.ui_amount
            decimals = supply_response.value.decimals
            
            print(f"\n   📈 Token Info:")
            print(f"   Address: {token_mint_str}")
            print(f"   Supply: {supply:,.0f}")
            print(f"   Decimals: {decimals}")
            
            return {
                'mint': token_mint_str,
                'supply': supply,
                'decimals': decimals
            }
    except Exception as e:
        print(f"   Could not get token info: {e}")
    finally:
        await client.close()


async def monitor_live(duration_seconds=300):
    """Monitor for new tokens in real-time"""
    print(f"🚀 Starting live monitor for {duration_seconds} seconds...")
    print(f"Time: {datetime.now()}")
    print("=" * 60)
    
    seen_tokens = set()
    start_time = datetime.now()
    check_count = 0
    
    while (datetime.now() - start_time).seconds < duration_seconds:
        check_count += 1
        print(f"\n--- Check #{check_count} at {datetime.now().strftime('%H:%M:%S')} ---")
        
        # Check last 10 transactions (don't show all details in monitor mode)
        new_tokens = await find_new_tokens(num_transactions=10, show_all=False)
        
        # Filter out tokens we've already seen
        for token_info in new_tokens:
            if token_info['new_token'] not in seen_tokens:
                seen_tokens.add(token_info['new_token'])
                print(f"\n🎉 NEW MEMECOIN ALERT!")
                print(f"Token: {token_info['new_token']}")
                print(f"Transaction: {token_info['pool_tx']}")
                print(f"Found at: {datetime.now()}")
                print("=" * 60)
        
        # Show we're still running
        if not new_tokens:
            print("No new pools found in this batch. Still monitoring...")
        
        # Wait before next check
        await asyncio.sleep(30)  # Check every 30 seconds
    
    print(f"\n\nMonitoring complete. Found {len(seen_tokens)} unique tokens.")
    if seen_tokens:
        print("\nTokens found:")
        for token in seen_tokens:
            print(f"  - {token}")


# Main execution
async def main():
    print("Solana Memecoin Scanner")
    print("=" * 60)
    
    # Option 1: Scan recent transactions
    # await find_new_tokens(num_transactions=100)
    
    # Option 2: Monitor live
    await monitor_live(duration_seconds=60)


if __name__ == "__main__":
    asyncio.run(main())