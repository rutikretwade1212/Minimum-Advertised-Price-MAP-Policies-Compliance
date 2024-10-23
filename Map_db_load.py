import pyodbc
import pandas as pd
import xml.etree.ElementTree as ET
import json
import yaml
from datetime import datetime
from decimal import Decimal

# Define the connection parameters
server = 'DESKTOP-3N0NHE2\\SQLEXPRESS'  # Server name 
master_database = 'master'
target_database = 'MAP_Compliance_Db'

# Create a connection string for Windows Authentication
def get_connection_string(database):
    return (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        'Trusted_Connection=yes;'
        f'DATABASE={database}'
    )

# Create and initialize the database
def initialize_database():
    conn = pyodbc.connect(get_connection_string(master_database), autocommit=True)
    cursor = conn.cursor()
    
    # Drop the database if it exists and create a new one
    drop_db_if_exists_command = f"""
    IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{target_database}')
    BEGIN
        DROP DATABASE {target_database}
    END
    """
    cursor.execute(drop_db_if_exists_command)
    print("Checked for existing database and dropped it if it existed.")
    
    create_db_command = f"CREATE DATABASE {target_database}"
    cursor.execute(create_db_command)
    print(f"Database '{target_database}' created successfully.")
    
    cursor.close()
    conn.close()

# Create tables in the target database
def create_tables():
    conn = pyodbc.connect(get_connection_string(target_database), autocommit=True)
    cursor = conn.cursor()
    
    table_creation_commands = [
        """
        CREATE TABLE SKU_Table (
            PN VARCHAR(50),
            SKU VARCHAR(50)
        );
        """,
        """
        CREATE TABLE PL_Table (
            PL VARCHAR(50),
            SKU VARCHAR(50),
            SUB_CATEGORY VARCHAR(50)
        );
        """,
        """
        CREATE TABLE Price_List_Table (
            PL VARCHAR(50),
            SKU VARCHAR(50),
            MAP DECIMAL(10, 2),
            LPP DECIMAL(10, 2)
        );
        """,
        """
        CREATE TABLE Seller_Mapping_Table (
            Seller_Name VARCHAR(50),
            Homologated_Name VARCHAR(50)
        );
        """,
        """
        CREATE TABLE Category_Mapping_Table (
            Category VARCHAR(50),
            Sub_Category VARCHAR(50),
            PL VARCHAR(50),
            SKU VARCHAR(50)
        );
        """,
        """
        CREATE TABLE Promotion_Table (
            PL VARCHAR(50),
            SKU VARCHAR(50),
            Season VARCHAR(50),
            Promotion DECIMAL(10, 2)
        );
        """,
        """
        CREATE TABLE Price_Monitoring_Table (
            SKU VARCHAR(50),
            Violation_date DATE,
            PL VARCHAR(50),
            Category VARCHAR(50),
            Sub_category VARCHAR(50),
            Region VARCHAR(50),
            Marketplace VARCHAR(50),
            Seller_name VARCHAR(50),
            Homologated_name VARCHAR(50),
            MAP_Price DECIMAL(10, 2),
            LPP DECIMAL(10, 2),
            Advertised_price DECIMAL(10, 2),
            Season VARCHAR(50),
            Promotional_price DECIMAL(10, 2)
        );
        """,
        """
        CREATE TABLE Seller_Table (
            Seller_name VARCHAR(50),
            SKU VARCHAR(50),
            Region VARCHAR(50),
            Advertised_price DECIMAL(10, 2),
            Marketplace VARCHAR(50),
            Screenshot VARCHAR(255),
            Live_link VARCHAR(255),
            Last_time_collected TIMESTAMP
        );
        """
    ]
    
    for command in table_creation_commands:
        cursor.execute(command)
        print("Table created successfully.")
    
    cursor.close()
    conn.close()

# Load data from XML into SKU_Table
def load_xml_to_sku_table():
    conn = pyodbc.connect(get_connection_string(target_database), autocommit=True)
    cursor = conn.cursor()
    
    tree = ET.parse('SKU Table.xml')
    root = tree.getroot()
    
    for item in root.findall('item'):
        pn = item.find('PN').text
        sku_code = item.find('Sku').text
        
        insert_command = """
        INSERT INTO SKU_Table (PN, SKU)
        VALUES (?, ?)
        """
        cursor.execute(insert_command, pn, sku_code)
    
    print("Data loaded successfully from XML to SKU_Table.")
    cursor.close()
    conn.close()

# Load data from JSON into PL_Table
def determine_sub_category(pl):
    """Determine the sub-category based on the PL value."""
    if 'Laptop' in pl or 'PC_Laptop' in pl:
        return 'Laptop'
    elif 'Monitor' in pl or 'PC_Monitor' in pl:
        return 'Monitor'
    elif 'TON' in pl:
        return 'Toner'
    elif 'LJ' in pl or 'Laserjet' in pl:
        return 'Laserjet'
    elif 'IJ' in pl or 'Inkjet' in pl:
        return 'Inkjet'
    elif 'Desktop' in pl or 'PC_Desktop' in pl:
        return 'Desktop'
    elif 'INK' in pl or 'SUP_INK' in pl:
        return 'Ink'
    else:
        return 'Unknown'  # Default case

def load_json_to_pl_table():
    try:
        conn = pyodbc.connect(get_connection_string(target_database), autocommit=True)
        cursor = conn.cursor()
        
        with open('PL Table.json', 'r') as file:
            data = file.readlines()
        
        for line in data:
            record = json.loads(line)
            sku = record['sku']
            pl = record['PL']
            sub_category = determine_sub_category(pl)
            
            insert_command = """
            INSERT INTO PL_Table (PL, SKU, SUB_CATEGORY)
            VALUES (?, ?, ?)
            """
            cursor.execute(insert_command, (pl, sku, sub_category))
            print(f"Inserted SKU: {sku}, PL: {pl}, SUB_CATEGORY: {sub_category} into PL_Table.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

# Load data from YAML into Price_List_Table
sub_category_discounts = {
    'Laptop': {'Hp': 0.034, 'Dell': 0.078},
    'Monitor': {'Hp': 0.012, 'Dell': 0.056},
    'Toner': {'Hp': 0.091, 'Dell': 0.023},
    'Laserjet': {'Hp': 0.065, 'Dell': 0.035},
    'Inkjet': {'Hp': 0.012, 'Dell': 0.056},
    'Desktop': {'Hp': 0.091, 'Dell': 0.023},
    'Ink': {'Hp': 0.065, 'Dell': 0.035},
}

def determine_sub_category_and_brand(pl):
    """Determine the sub-category and brand based on the PL value."""
    sub_category = determine_sub_category(pl)
    
    if 'DELL' in pl:
        brand = 'Dell'
    elif 'HP' in pl:
        brand = 'Hp'
    else:
        brand = 'Unknown'
    
    return sub_category, brand

def determine_lpp(sub_category, map_price, brand):
    """Calculate LPP based on the sub_category and brand."""
    if sub_category not in sub_category_discounts:
        return map_price  # Return the original price if the sub_category is unknown

    discount = sub_category_discounts[sub_category].get(brand, 0)
    lpp = map_price * (1 - discount)
    return lpp

def load_yaml_to_price_list_table():
    try:
        conn = pyodbc.connect(get_connection_string(target_database), autocommit=True)
        cursor = conn.cursor()
        
        with open('Price List Table.yaml', 'r') as file:
            data = yaml.safe_load(file)
        
        for record in data:
            pl = record['PL']
            sku = record['sku']
            map_price = record['MAP']
            sub_category, brand = determine_sub_category_and_brand(pl)
            lpp_price = determine_lpp(sub_category, map_price, brand)
            
            insert_command = """
            INSERT INTO Price_List_Table (PL, SKU, MAP, LPP)
            VALUES (?, ?, ?, ?)
            """
            cursor.execute(insert_command, (pl, sku, map_price, lpp_price))
            print(f"Inserted PL: {pl}, SKU: {sku}, MAP: {map_price}, LPP: {lpp_price} into Price_List_Table.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

# Load data from Excel into Seller_Mapping_Table
def load_xlsx_to_seller_mapping_table():
    try:
        conn = pyodbc.connect(get_connection_string(target_database), autocommit=True)
        cursor = conn.cursor()
        
        df = pd.read_excel('Seller Mapping Table.xlsx')
        
        for index, row in df.iterrows():
            seller_name = row['Sellers_name']
            homologated_name = row['homologated_sellers']
            
            insert_command = """
            INSERT INTO Seller_Mapping_Table (Seller_Name, Homologated_Name)
            VALUES (?, ?)
            """
            cursor.execute(insert_command, (seller_name, homologated_name))
            print(f"Inserted Seller_Name: {seller_name}, Homologated_Name: {homologated_name} into Seller_Mapping_Table.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

# Load the category for SKU
def insert_category_mapping(connection_string):
    try:
        conn = pyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        
        cursor.execute("SELECT PL, SKU, SUB_CATEGORY FROM PL_Table")
        pl_table_data = cursor.fetchall()
        
        for row in pl_table_data:
            pl = row[0]
            sku = row[1]
            sub_category = row[2]
            
            # Determine the category based on the sub-category
            if sub_category in ['Laptop', 'Desktop', 'Monitor']:
                category = 'PC'
            elif sub_category in ['Laserjet', 'Inkjet']:
                category = 'Print Hardware'
            elif sub_category in ['Toner', 'Ink']:
                category = 'Supply'
            else:
                category = 'Unknown'
            
            insert_command = """
            INSERT INTO Category_Mapping_Table (Category, Sub_Category, PL, SKU)
            VALUES (?, ?, ?, ?)
            """
            cursor.execute(insert_command, (category, sub_category, pl, sku))
            print(f"Inserted Category: {category}, Sub_Category: {sub_category}, PL: {pl}, SKU: {sku} into Category_Mapping_Table.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

# Load promotion data into Promotion_Table
# Define the discounts for each category and quarter
category_discounts_calculate_promotion = {
    'Supply': {'Q1': 0.10, 'Q2': 0.0, 'Q3': 0.05, 'Q4': 0.20},  # Applying percentage discounts
    'Print Hardware': {'Q1': 0.15, 'Q2': 0.0, 'Q3': 0.02, 'Q4': 10},  # 10 dollars off
    'PC': {'Q1': 5, 'Q2': 0.0, 'Q3': 0.0, 'Q4': 0.15},  # 5 pounds off or percentage off
}

# Define mapping for sub-categories
sub_category_mappings_calculate_promotion = {
    'Laptop': 'PC',
    'Monitor': 'Print Hardware',
    'Toner': 'Supply',
    'Laserjet': 'Print Hardware',
    'Inkjet': 'Print Hardware',
    'Desktop': 'PC',
    'Ink': 'Supply',
}

def determine_sub_category_calculate_promotion(pl):
    """Determine the sub-category based on the PL value."""
    if 'Laptop' in pl:
        return 'Laptop'
    elif 'Monitor' in pl:
        return 'Monitor'
    elif 'TON' in pl:
        return 'Toner'
    elif 'Laserjet' in pl:
        return 'Laserjet'
    elif 'Inkjet' in pl:
        return 'Inkjet'
    elif 'Desktop' in pl:
        return 'Desktop'
    elif 'INK' in pl:
        return 'Ink'
    else:
        return 'Unknown'  # Default case

def determine_category_calculate_promotion(sub_category):
    """Map sub-category to main category."""
    return sub_category_mappings_calculate_promotion.get(sub_category, 'Unknown')

def get_current_quarter_calculate_promotion():
    """Determine the current quarter based on the current date."""
    now = datetime.now()
    month = now.month
    if month in [1, 2, 3]:
        return 'Q1'
    elif month in [4, 5, 6]:
        return 'Q2'
    elif month in [7, 8, 9]:
        return 'Q3'
    elif month in [10, 11, 12]:
        return 'Q4'
    else:
        return 'Unknown'

def determine_promotion_price_calculate_promotion(map_price, category, quarter):
    """Calculate the promotion price based on the category and quarter."""
    if category not in category_discounts_calculate_promotion:
        return map_price  # Return the original price if the category is unknown

    discount = category_discounts_calculate_promotion[category].get(quarter, 0)
    
    if category == 'Print Hardware' and quarter == 'Q4':
        # For Print Hardware in Q4, we have a fixed $10 off
        promotion_price = max(map_price - 10, 0)
    elif category == 'PC' and quarter == 'Q1':
        # For PC in Q1, the discount is £5 off
        # Assuming MAP is in dollars, you would need to convert this if necessary
        promotion_price = max(map_price - 5, 0)
    elif discount:
        # For percentage based discounts
        promotion_price = map_price * (1 - discount)
    else:
        promotion_price = map_price
    
    return promotion_price

def load_promotion_table_calculate_promotion():
    try:
        # Establish a connection to the SQL Server
        conn = pyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        
        # Read the YAML file
        with open('Price List Table.yaml', 'r') as file:
            data = yaml.safe_load(file)
        
        # Define the SKUs you want to process
        skus = [
            'G0L15B', '7Y4T6QB', '4K0V8PA', '385Z7PA', 'T6M05AA', 'C4Q32B', '03V96QB', '509O6B',
            '952X8QB', 'R3723BG', '6X4B9PA', '3YM22AA', '3ED50A', '780A5PA', '935M7QB', '8L164PA',
            'Z7Y83A', 'CF501A', '9D3N5PA', 'U9X27B', 'CF502A', 'W2090A', 'W1370A', '805X2PA', '492W7B',
            'P2V64A', '3F3Z5B7', '381U0A', '3I1O2BB', '9G9A4QB', 'F9J66A', 'DG513Y', 'Z7Y82A', '0R043QB',
            '9Y2V1QB', 'DF422B', '8L4A4QB', '7O5F9BB', '81B19PA', 'CN046AA', 'M1T17BB', '8L145PA', 'Z7Y78A',
            '3A725B', '7L0K3QB', 'CF256X', 'G0K77B', '9M9Q6QB', '4S6X5PA', 'CE320A', 'DA218BB', '3WX00A',
            'DG388B', 'CN054AA', '7O5E7BB', 'DG512B', 'CN636A', '9T680PA', '4XY49B', 'C9380A', 'DF091B',
            '4FE88B', 'J3M70A', 'C2P24AA', 'P2V27A', 'CB543A', 'M1T68BB', 'CN635A', '8Q815QB', 'U9X26B',
            '3YP94AA', 'DG474B', '891B6QB', '9X3L6PA', '7K667PA', '8U6Y8PA', 'DG343B', '2H0N1AA', 'L2763A',
            '6ZD61AA', '515M8D', 'P2V83A', '8L146PA', '4SB24A', '7Y5C6QB', '8L107PA', '9G7E4QB', 'W2092A',
            'X2446Y', 'Y5F89BB', '90N54PA', 'CN047AA', '87G01PA', 'L0S60AA', 'CH645A', '7KW56A', 'X2214B',
            '8F8Z3PA', '77S54PA', 'B3P21A'
        ]
        
        # Get the current quarter
        current_quarter = get_current_quarter_calculate_promotion()
        
        # Parse each YAML record and insert into the Promotion Table
        for record in data:
            pl = record['PL']
            sku = record['sku']
            map_price = record['MAP']
            
            if sku in skus:
                sub_category = determine_sub_category_calculate_promotion(pl)
                category = determine_category_calculate_promotion(sub_category)
                promotion_price = determine_promotion_price_calculate_promotion(map_price, category, current_quarter)
                
                insert_command = """
                INSERT INTO Promotion_Table (PL, SKU, Season, Promotion)
                VALUES (?, ?, ?, ?)
                """
                cursor.execute(insert_command, (pl, sku, current_quarter, promotion_price))
                print(f"Inserted PL: {pl}, SKU: {sku}, Season: {current_quarter}, Promotion_Price: {promotion_price} into Promotion_Table.")
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

# Main workflow
initialize_database()
create_tables()
load_xml_to_sku_table()
load_json_to_pl_table()
load_yaml_to_price_list_table()
load_xlsx_to_seller_mapping_table()

connection_string = get_connection_string(target_database)
insert_category_mapping(connection_string)

load_promotion_table_calculate_promotion()
