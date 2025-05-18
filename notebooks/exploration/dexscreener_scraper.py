"""
DexScreener Scraping Script
--------------------------
This script demonstrates various approaches to scrape data from DexScreener,
which has bot detection mechanisms in place.
"""

import time
import json
import os
from dotenv import load_dotenv
import pandas as pd
import requests
from playwright.sync_api import sync_playwright
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# Target URL
TARGET_URL = "https://dexscreener.com/new-pairs/6h?rankBy=trendingScoreH6&order=desc&minLiq=1000&maxAge=12"

def save_data(data, filename="dexscreener_data.csv"):
    """Save scraped data to a CSV file"""
    df = pd.DataFrame(data)
    output_path = os.path.join("../../data/raw", filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")
    return df

# =====================================================================
# APPROACH 1: Using the DexScreener API (Preferred if available)
# =====================================================================

def scrape_dexscreener_api():
    """
    Attempt to use DexScreener's API if available.
    This is the most reliable method if the API is accessible.
    """
    print("Attempting to use DexScreener API...")
    
    # DexScreener might have an API endpoint like this (hypothetical)
    api_url = "https://api.dexscreener.com/latest/dex/pairs/new"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://dexscreener.com/",
    }
    
    params = {
        "rankBy": "trendingScoreH6",
        "order": "desc",
        "minLiq": "1000",
        "maxAge": "12",
        "timeframe": "6h"
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            print("Successfully retrieved data from API")
            return data
        else:
            print(f"API request failed with status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error accessing API: {e}")
        return None

# =====================================================================
# APPROACH 2: Using Playwright (Most Advanced)
# =====================================================================

def scrape_with_playwright():
    """
    Use Playwright to scrape DexScreener.
    Playwright is excellent for bypassing bot detection as it emulates a real browser.
    """
    print("Scraping with Playwright...")
    data = []
    
    with sync_playwright() as p:
        # Use a real browser with stealth mode
        browser = p.chromium.launch(headless=False)  # Set headless=False to see the browser
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Add cookies if needed
        # context.add_cookies([...])
        
        page = context.new_page()
        
        # Emulate human-like behavior
        page.goto("https://dexscreener.com/")
        time.sleep(2)  # Wait a bit before navigating to the target page
        
        # Navigate to the target page
        page.goto(TARGET_URL)
        
        # Wait for the content to load
        page.wait_for_selector("table", timeout=60000)
        time.sleep(5)  # Give extra time for dynamic content
        
        # Extract data from the table
        rows = page.query_selector_all("table tbody tr")
        
        for row in rows:
            try:
                # Extract data from each row (adjust selectors as needed)
                pair_name = row.query_selector("td:nth-child(1)").inner_text()
                price = row.query_selector("td:nth-child(2)").inner_text()
                price_change = row.query_selector("td:nth-child(3)").inner_text()
                liquidity = row.query_selector("td:nth-child(4)").inner_text()
                volume = row.query_selector("td:nth-child(5)").inner_text()
                
                data.append({
                    "pair_name": pair_name,
                    "price": price,
                    "price_change": price_change,
                    "liquidity": liquidity,
                    "volume": volume
                })
            except Exception as e:
                print(f"Error extracting row data: {e}")
        
        # Take a screenshot for debugging
        page.screenshot(path="dexscreener_screenshot.png")
        
        # Close the browser
        browser.close()
    
    return data

# =====================================================================
# APPROACH 3: Using Selenium with Undetected ChromeDriver
# =====================================================================

def scrape_with_selenium():
    """
    Use Selenium with undetected_chromedriver to scrape DexScreener.
    This approach can bypass some bot detection mechanisms.
    """
    print("Scraping with Selenium...")
    data = []
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # Uncomment to run headless (may be detected more easily)
    # chrome_options.add_argument("--headless")
    
    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Set the window size
        driver.set_window_size(1920, 1080)
        
        # Execute CDP commands to bypass detection
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        
        # First visit the main site
        driver.get("https://dexscreener.com/")
        time.sleep(3)
        
        # Then navigate to the target URL
        driver.get(TARGET_URL)
        
        # Wait for the table to load
        wait = WebDriverWait(driver, 30)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        # Give extra time for dynamic content to load
        time.sleep(5)
        
        # Extract data from the table
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        
        for row in rows:
            try:
                # Extract data from each row (adjust selectors as needed)
                pair_name = row.find_element(By.CSS_SELECTOR, "td:nth-child(1)").text
                price = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)").text
                price_change = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text
                liquidity = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text
                volume = row.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text
                
                data.append({
                    "pair_name": pair_name,
                    "price": price,
                    "price_change": price_change,
                    "liquidity": liquidity,
                    "volume": volume
                })
            except Exception as e:
                print(f"Error extracting row data: {e}")
        
        # Take a screenshot for debugging
        driver.save_screenshot("dexscreener_selenium_screenshot.png")
        
    except Exception as e:
        print(f"Error during Selenium scraping: {e}")
    finally:
        driver.quit()
    
    return data

# =====================================================================
# APPROACH 4: Using DexScreener's GraphQL API (if available)
# =====================================================================

def scrape_dexscreener_graphql():
    """
    Attempt to use DexScreener's GraphQL API if available.
    Many modern websites use GraphQL for data fetching.
    """
    print("Attempting to use DexScreener GraphQL API...")
    
    # Hypothetical GraphQL endpoint
    graphql_url = "https://api.dexscreener.com/graphql"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Referer": "https://dexscreener.com/",
    }
    
    # GraphQL query (this is hypothetical and would need to be adjusted)
    query = """
    query GetNewPairs($timeframe: String!, $rankBy: String!, $order: String!, $minLiq: Float!, $maxAge: Int!) {
        newPairs(timeframe: $timeframe, rankBy: $rankBy, order: $order, minLiq: $minLiq, maxAge: $maxAge) {
            pairName
            price
            priceChange
            liquidity
            volume
            chain
            address
        }
    }
    """
    
    variables = {
        "timeframe": "6h",
        "rankBy": "trendingScoreH6",
        "order": "desc",
        "minLiq": 1000,
        "maxAge": 12
    }
    
    try:
        response = requests.post(
            graphql_url,
            headers=headers,
            json={"query": query, "variables": variables}
        )
        
        if response.status_code == 200:
            data = response.json()
            print("Successfully retrieved data from GraphQL API")
            return data
        else:
            print(f"GraphQL request failed with status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error accessing GraphQL API: {e}")
        return None

# =====================================================================
# Main execution
# =====================================================================

def main():
    """Execute the scraping process using multiple approaches"""
    
    print("Starting DexScreener scraping process...")
    
    # Try the API approach first (most reliable if available)
    api_data = scrape_dexscreener_api()
    if api_data and len(api_data) > 0:
        print("Successfully scraped data using the API")
        return save_data(api_data, "dexscreener_api_data.csv")
    
    # Try the GraphQL approach
    graphql_data = scrape_dexscreener_graphql()
    if graphql_data and len(graphql_data) > 0:
        print("Successfully scraped data using the GraphQL API")
        return save_data(graphql_data, "dexscreener_graphql_data.csv")
    
    # If API approaches fail, try Playwright (most advanced browser automation)
    playwright_data = scrape_with_playwright()
    if playwright_data and len(playwright_data) > 0:
        print("Successfully scraped data using Playwright")
        return save_data(playwright_data, "dexscreener_playwright_data.csv")
    
    # If Playwright fails, try Selenium
    selenium_data = scrape_with_selenium()
    if selenium_data and len(selenium_data) > 0:
        print("Successfully scraped data using Selenium")
        return save_data(selenium_data, "dexscreener_selenium_data.csv")
    
    print("All scraping approaches failed")
    return None

if __name__ == "__main__":
    main()
