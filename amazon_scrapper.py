# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 17:00:59 2024

@author: Rutik Retwade
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
import time
from datetime import datetime

# Initialize the Service with the correct path to chromedriver
s = Service("D:/Project Proitbridge/chromedriver-win64/chromedriver.exe")
options = Options()
options.add_argument("--start-maximized")  # Start browser in maximized mode
driver = webdriver.Chrome(service=s, options=options)

# PART 1: Open the Amazon website and search for a product
driver.get("https://www.amazon.in/")
time.sleep(2)

# Find the search bar element
search_bar = driver.find_element(By.XPATH, "//input[@id='twotabsearchtextbox']")

# Enter the search term (e.g., "HP laptop AMD") and press Enter
search_term = "HP laptop AMD"
search_bar.send_keys(search_term)
search_bar.send_keys(Keys.RETURN)

# Wait for the search results page to load
time.sleep(3)

# Define the product name you're looking for
desired_product_name = "HP Laptop 15s, AMD Ryzen 5 5500U, 15.6-inch (39.6 cm), FHD, 8GB DDR4, 512GB SSD, AMD Radeon Graphics, Thin & Light, Dual Speakers (Win 11, MSO 2019, Silver, 1.69 kg), eq2144AU"

# Wait for products to be visible on the page
WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='a-size-medium a-color-base a-text-normal']")))

# Find all product elements with the specific class that contains product names
products = driver.find_elements(By.XPATH, "//span[@class='a-size-medium a-color-base a-text-normal']")

# Iterate through the products to find the one that matches your criteria
for product in products:
    product_text = product.text
    print(f"Found product: {product_text}")  # Optional: Print product names for debugging
    if desired_product_name in product_text:
        # Scroll to the product to ensure it is in view
        actions = ActionChains(driver)
        actions.move_to_element(product).perform()

        # Click on the product
        product.click()
        print(f"Clicked on product: {product_text}")
        break  # Exit the loop once the correct product is found and clicked

# Wait for the product page to load
time.sleep(3)

# Define the XPath for the "New (2) from" link in the offers section
offer_link_xpath = "//a[contains(@href, '/gp/offer-listing') and contains(@class, 'a-link-normal')]"

# Find the offer link and click it to open in a new tab
try:
    offer_link_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, offer_link_xpath))
    )
    offer_page_link = offer_link_element.get_attribute('href')

    # Open the link in a new tab
    driver.execute_script("window.open(arguments[0]);", offer_page_link)
    print(f"Opened offer page link in a new tab: {offer_page_link}")

    # Switch to the newly opened tab
    driver.switch_to.window(driver.window_handles[-1])

    # Get the live URL of the new tab
    live_link = driver.current_url
    print(f"Live link in the new tab: {live_link}")

except Exception as e:
    print(f"An error occurred while opening the link: {e}")

# PART 2: Scraping seller names, prices, and live links
time.sleep(3)

# Initialize lists to hold seller names, prices, live links
seller_names = []
prices = []
live_links = []

# Use BeautifulSoup to parse the page's content
soup = BeautifulSoup(driver.page_source, 'html.parser')

# Find all seller names
seller_links = soup.select('.a-fixed-left-grid-col.a-col-right a.a-size-small.a-link-normal')

# Find all price whole spans
price_whole_links = soup.select('.a-price-whole')

# Function to take a screenshot of the entire page with a timestamp, and zoom out to 50% before capturing it
def take_full_page_screenshot(index):
    # Zoom out to 50% using JavaScript
    driver.execute_script("document.body.style.zoom='50%'")
    
    # Give the browser a moment to adjust the zoom
    time.sleep(1)

    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # Format: YYYYMMDD_HHMMSS
    
    # Create a file path with the timestamp in the file name
    screenshot_path = f'D:/Project Proitbridge/amazon_screenshot_product_laptop_{timestamp}_{index}.png'
    
    try:
        # Save the screenshot
        driver.save_screenshot(screenshot_path)
        print(f"Screenshot for seller {index + 1} saved successfully at {screenshot_path}.")
    except Exception as e:
        print(f"Error taking screenshot for seller {index + 1}: {e}")
    
    # Reset the zoom back to 100% after taking the screenshot
    driver.execute_script("document.body.style.zoom='100%'")
    time.sleep(1)  # Pause for a moment to allow the browser to reset the zoom

    return screenshot_path

# Loop through seller names and extract prices and live links
for index, (seller, price_whole) in enumerate(zip(seller_links, price_whole_links)):
    seller_name = seller.get_text(strip=True)
    price_value = price_whole.get_text(strip=True)

    # Get the live link
    live_link = "https://www.amazon.in" + seller.get('href')
    
    if seller_name and price_value:  # Ensure they're not empty
        price_decimal = price_whole.find_next_sibling('.a-price-decimal').get_text(strip=True) if price_whole.find_next_sibling('.a-price-decimal') else '00'
        full_price = f"{price_value}{price_decimal}"
        seller_names.append(seller_name)
        prices.append(full_price)
        live_links.append(live_link)

        # Take a screenshot for each seller
        screenshot_path = take_full_page_screenshot(index)

# Create a DataFrame from the seller names, prices, and live links
df = pd.DataFrame({
    'Seller Name': seller_names,
    'Price': prices,
    'Live Link': live_links,
    'SKU': ['laptop'] * len(seller_names),  # Default SKU value
    'Region': ['India'] * len(seller_names),  # Default region value
    'Marketplace': ['Amazon'] * len(seller_names)  # Marketplace for Amazon
})

# Display the DataFrame
print(df)

# PART 3: Insert into the database
# Define the connection parameters
server = 'DESKTOP-3N0NHE2\\SQLEXPRESS'
target_database = 'MAP_Compliance_Db'

# Create a connection string for Windows Authentication
def get_connection_string(database):
    return (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'Trusted_Connection=yes;'
    )

# Create the SQL connection
connection = pyodbc.connect(get_connection_string(target_database))
cursor = connection.cursor()

# Insert data into the database
insert_query = """
    INSERT INTO Seller_Table (Seller_name, SKU, Region, Advertised_price, Marketplace, Live_link)
    VALUES (?, ?, ?, ?, ?, ?)
"""

# Loop through the DataFrame and insert each row into the database
for i, row in df.iterrows():
    cursor.execute(insert_query, row['Seller Name'], row['SKU'], row['Region'], row['Price'], row['Marketplace'], row['Live Link'])

# Commit the transaction
connection.commit()

# Close the connection
cursor.close()
connection.close()

# Close the browser
driver.quit()
