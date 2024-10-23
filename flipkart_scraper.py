from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pyodbc
from PIL import Image
import io

# Initialize the Service with the correct path to chromedriver
s = Service("D:/Project Proitbridge/chromedriver-win64/chromedriver.exe")
options = Options()
options.add_argument("--start-maximized")  # Start browser in maximized mode
driver = webdriver.Chrome(service=s, options=options)

# Open the Flipkart website
driver.get("https://www.flipkart.com/")

time.sleep(2)

# Close the login pop-up if it appears
try:
    close_button = driver.find_element(By.XPATH, "//button[contains(text(), '✕')]")
    close_button.click()
except Exception as e:
    print("Login pop-up did not appear.")

# Find the search bar element
search_bar = driver.find_element(By.XPATH, "//input[@name='q']")


# Enter the search term and press Enter
search_term = input("Enter the search term: ")
search_bar.send_keys(search_term)
search_bar.send_keys(Keys.RETURN)

# Wait for the search results page to load
time.sleep(3)

# Find all product elements that match the search term
products = driver.find_elements(By.XPATH, "//div[@class='KzDlHZ']")

product_name = input("Enter the product name to match: ")

# Iterate through the products to find the one that matches your criteria
for product in products:
    product_text = product.text
    if product_name in product_text:
        # Scroll to the product to ensure it is in view
        actions = ActionChains(driver)
        actions.move_to_element(product).perform()

        # Click on the product
        product.click()
        break  # Exit the loop once the correct product is found and clicked

# Wait for the product page to load
time.sleep(3)

# Get a list of all window handles
window_handles = driver.window_handles

# Switch to the new tab (the last one in the list)
driver.switch_to.window(window_handles[-1])

# Now get the current URL of the new tab
current_url = driver.current_url

# Print the current URL of the new tab
print(f"Current URL of the new tab: {current_url}")
# Open the Flipkart product page
driver.get(current_url)

time.sleep(2)

# Find and click the first product
product = driver.find_element(By.XPATH, "/html/body/div[1]/div/div[3]/div[1]/div[2]/div[8]/div[1]/div/div[2]/li/a")
product.click()

# Wait for the new page to load
time.sleep(3)

# Get the page source and parse with BeautifulSoup
page_source = driver.page_source
soup = BeautifulSoup(page_source, 'html.parser')

# Extract all seller elements
seller_elements = soup.find_all('div', {'class': 'UQFoop'})

# Lists to store the scraped data
seller_names = []
advertised_prices = []
screenshots = []  # Initialize the screenshots list
live_links = []

# Initialize a counter for screenshot numbering
screenshot_counter = 1

for seller_element in seller_elements:
    # Extract the seller name
    seller_name = seller_element.find('span').text.strip()
    seller_names.append(seller_name)
    
    # Extract the advertised price
    advertised_price_elements = seller_element.find_all('div', {'class': 'Nx9bqj'})
    
    if advertised_price_elements:
        for price_element in advertised_price_elements:
            price_text = price_element.text.strip()
            # Clean the price text
            advertised_price_cleaned = price_text.replace('₹', '').replace(',', '').strip()
            try:
                advertised_price = float(advertised_price_cleaned)
                advertised_prices.append(advertised_price)
            except ValueError:
                advertised_prices.append(None)  # Handle any conversion errors gracefully
    else:
        advertised_prices.append(None)
        
        # Capture the live link (current URL)
    live_link = driver.current_url
    live_links.append(live_link)

    
# Define the base XPath pattern for product containers
base_xpath = '/html/body/div[1]/div/div[3]/div/div/div/div[2]/div[2]/div['

# Initialize index and capture screenshots
index = 1
while True:
    # Construct the XPath for the current product container
    xpath = f"{base_xpath}{index}]"
    
    try:
        # Find the product container
        product_container = driver.find_element(By.XPATH, xpath)
        
        # Take a screenshot of the current product container
        screenshot_path = f'D:/Project Proitbridge/screenshot_product_laptop{index}.png'
        product_container.screenshot(screenshot_path)
        screenshots.append(screenshot_path)
        print(f"Screenshot taken for product {index}: {screenshot_path}")

        # Increment index for the next product
        index += 1
        
        # Optionally, scroll to the next product if necessary
        driver.execute_script("arguments[0].scrollIntoView();", product_container)
        time.sleep(1)  # Short delay to ensure the product container is in view

    except Exception as e:
        # If no more products are found, break the loop
        print(f"No more products found or error occurred: {e}")
        break
        
    

# Print the collected data for verification
print("Seller Names:", seller_names)
print("Advertised Prices:", advertised_prices)
print("Screenshots:", screenshots)
print("Live Links:", live_links)

# Insert the collected data into the database
# Define the connection parameters
server = 'DESKTOP-3N0NHE2\\SQLEXPRESS'
target_database = 'MAP_Compliance_Db'

# Create a connection string for Windows Authentication
def get_connection_string(database):
    return (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        'Trusted_Connection=yes;'
        f'DATABASE={database}'
    )

# Connect to the target database
conn = pyodbc.connect(get_connection_string(target_database))
cursor = conn.cursor()

# Define the insert query
insert_query = """
INSERT INTO Seller_Table (Seller_name, SKU, Region, Advertised_price, Marketplace, Screenshot, Live_link)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# Insert the data into the database
for i in range(len(seller_names)):
    try:
        cursor.execute(
            insert_query,
            seller_names[i],
            'laptop',  # Default SKU value
            'India',  # Default region value
            advertised_prices[i] if i < len(advertised_prices) else None,
            'Flipkart',
            screenshots[i] if i < len(screenshots) else None,
            live_links[i] if i < len(live_links) else None
        )
        conn.commit()
        print(f"Data for {seller_names[i]} inserted successfully.")
    except pyodbc.Error as e:
        print(f"Error inserting data for {seller_names[i]}: {e}")

# Close the database connection
cursor.close()
conn.close()

# Close the browser
driver.quit()
