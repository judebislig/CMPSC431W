#!/usr/bin/env python
import psycopg2 as db_connect
import csv
from io import StringIO

host_name="localhost"
db_user="postgres"
db_password="Arcijulu04!"
db_name="postgres"

NUM_COLUMNS = 17

connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
cursor = connection.cursor()

# HELPER FUNCTIONS ------------------------------------------------------------------------------------

# Given table and data, inserts the data into the table
def table_insert(table, data):
    formatted_values = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO {table} VALUES ({formatted_values})"
    cursor.execute(query, data)

# Delete from tables given the dolvehicleid
def table_delete(table, id):
    query = f"DELETE FROM {table} WHERE dolvehicleid = '{id}';"
    cursor.execute(query)

# Update data in tables given the dolvehicleid
def table_update(table, id, newVal):
    query = f"UPDATE {table} SET electricrange = '{newVal}' WHERE dolvehicleid = '{id}';"
    cursor.execute(query) 

    
# HELPER FUNCTIONS END --------------------------------------------------------------------------------

# Insert new car entry into the database
def insert_car_entry():
    dolVehicleId = input("Enter the Dept of Licensing (DOL) Vehicle ID: ")
    attributes = input("Enter the following attributes separated by a comma and space: VIN, County, City, State, Postal Code, Model Year, Make, Model, Electric Vehicle Type, CAFV Eligibility, Electric Range, Base MSRP, Legislative District, Vehicle Location (coordiinates), Electric Utility, and 2020 Census Tract: ")
    attributeList = attributes.split(", ")
    if (len(attributeList) != NUM_COLUMNS - 1):
        print("Invalid amount of entries!")
    else:
        vehicle_data = (attributeList[0], attributeList[5], attributeList[9], attributeList[10], dolVehicleId, attributeList[13])
        location_data = (attributeList[4], attributeList[1], attributeList[2], attributeList[3], attributeList[12], attributeList[14], attributeList[15], dolVehicleId)
        physical_attributes_data = (attributeList[6], attributeList[7], attributeList[8], attributeList[11], dolVehicleId)
        
        try:
            table_insert('vehicle', vehicle_data)
            table_insert('location', location_data)
            table_insert('physicalattributes', physical_attributes_data)
            print("Car entry successfully added.")
            
        except Exception as e:
            print(f"Error inserting data: {e}")

        connection.commit()

# Delete existing car entry into the database
def delete_car_entry():
    dolVehicleId = input("Enter the Dept of Licensing (DOL) Vehicle ID: ")
    try:
        table_delete('location', dolVehicleId)
        table_delete('physicalattributes', dolVehicleId)
        table_delete('vehicle', dolVehicleId)
        print("Car entry successfully deleted.")
        
    except Exception as e:
        print(f"Error deleting data: {e}")

    connection.commit()

# Update existing car entry in the database
def update_car_entry():
    dolVehicleId = input("Enter the Dept of Licensing (DOL) Vehicle ID: ")
    electricRange = input("Enter the new Electric Range: ")
    try:
        table_update('vehicle', dolVehicleId, electricRange)
        print("Car entry successfully updated.")
        
    except Exception as e:
        print(f"Error updating data: {e}")

    connection.commit()

# Search existing car entry in the database
def search_car_entry():
    vin = input("Enter the VIN to search: ")
    try:
        query = f"SELECT * from vehicle where vin = '{vin}';"
        cursor.execute(query) 
        results = cursor.fetchall()
        for row in results:
            print(row)
        
    except Exception as e:
        print(f"Error selecting data: {e}")

# Search existing car entry in the database
def count_car_entry():
    evt = input("Enter the Electric Vehicle Type to count (either 'Battery Electric Vehicle (BEV)' or 'Plug-in Hybrid Electric Vehicle (PHEV)'): ")
    try:
        query = f"SELECT COUNT(electricvehicletype) from physicalattributes where electricvehicletype = '{evt}';"
        cursor.execute(query) 
        results = cursor.fetchall()
        print(f"Count of {evt}: {results[0][0]}")
        
    except Exception as e:
        print(f"Error selecting data: {e}")

# Sort car entries in the database
def sort_car_entry():
    try:
        query = f"SELECT * FROM vehicle ORDER BY dolvehicleid ASC LIMIT 1000;"
        cursor.execute(query) 
        results = cursor.fetchall()
        print(f"First 1000 entries sorted by DOL Vehicle ID:")
        for row in results:
            print(row)
        
    except Exception as e:
        print(f"Error selecting data: {e}")

# Join car entries in the database
def join_car_entry():
    try:
        query = f"""SELECT vin, make, model, electricutility
        FROM vehicle v 
        INNER JOIN location l
        ON v.dolvehicleid = l.dolvehicleid
        INNER JOIN physicalattributes p
        ON p.dolvehicleid = v.dolvehicleid LIMIT 1000; """
        cursor.execute(query) 
        results = cursor.fetchall()
        print(f"First 1000 entries joined by DOL Vehicle ID:")
        for row in results:
            print(row)
        
    except Exception as e:
        print(f"Error selecting data: {e}")


# Join car entries in the database
def group_car_entry():
    try:
        query = f"SELECT modelyear, COUNT(vin) from vehicle GROUP BY modelyear"
        cursor.execute(query) 
        results = cursor.fetchall()
        for row in results:
            print(f"Model Year: {row[0]} | Amount of Cars: {row[1]}")
        
    except Exception as e:
        print(f"Error selecting data: {e}")

# Join car entries in the database
def subquery_car_entry():
    make = input("Enter the Make of the vehicle: ")
    try:
        print(f"Selecting vehicles where DOL Vehicle ID matches PhysicalAttributes entries with Make {make}")
        query = f"""
        SELECT v.* FROM vehicle v
        WHERE v.dolvehicleid IN (
            SELECT pa.dolvehicleid FROM physicalattributes pa
            WHERE pa.make = '{make}'
        ) LIMIT 1000;
        """
        cursor.execute(query) 
        results = cursor.fetchall()
        for row in results:
            print(row)
        
    except Exception as e:
        print(f"Error selecting data: {e}")

def transaction_delete():
    dolVehicleId = input("Enter the Dept of Licensing (DOL) Vehicle ID: ")
    try:
        # Disable autocommit to manually manage the transaction
        connection.autocommit = False
        print("Transaction started.")

        # Perform delete operations
        table_delete('location', dolVehicleId)
        table_delete('physicalattributes', dolVehicleId)
        table_delete('vehicle', dolVehicleId)

        connection.commit()
        print("Transaction committed.")

    except Exception as e:
        # Rollback transaction if any delete fails
        connection.rollback()
        print(f"Error deleting data: {e}")

    finally:
        connection.autocommit = True


# Attempt to port data from the csv
def state_populate_data():
    try:
        with open('Electric_Vehicle_Population_Data.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            counter = 0
            newRows = 0
            for row in reader:
                if counter >= 10000:
                    break

                # Data for each of the tables
                vehicle_data = (row['VIN (1-10)'], row['Model Year'], row['Clean Alternative Fuel Vehicle (CAFV) Eligibility'], row['Electric Range'], row['DOL Vehicle ID'], row['Vehicle Location'])
                location_data = (row['Postal Code'], row['County'], row['City'], row['State'], row['Legislative District'], row['Electric Utility'], row['2020 Census Tract'], row['DOL Vehicle ID'])
                physical_attributes_data = (row['Make'], row['Model'], row['Electric Vehicle Type'], row['Base MSRP'], row['DOL Vehicle ID'])
                
                # Insert if fresh/zero rows in the tables
                try:
                    table_insert('vehicle', vehicle_data)
                    table_insert('location', location_data)
                    table_insert('physicalattributes', physical_attributes_data)
                    newRows += 1
                    
                except:
                    print("Current row is already populated!")

                counter += 1
                
            print(f"Insertion attempt complete with {newRows} rows out of {counter}.")
            connection.commit() 

    # Error handling using 
    except FileNotFoundError:
        print("The specified file was not found.")
    except KeyError as e:
        print(f"Missing column in the CSV: {e}")

    


def stateStart():
    userInput = 0
    print("----------------------------------------------------------")
    print("Welcome to the Washington State Electric Vehicle Interface!")
    print("----------------------------------------------------------\n")

    print("Please select an option:\n")
    print("1. Insert New Car Entry\n2. Delete Car Entry\n3. Update Car Entry\n4. Search Car Entry\n5. Aggregate Functions\n6. Sorting\n7. Joins\n8. Grouping\n9. Subqueries\n10. Transactions\n11. Populate Data\n12. Exit\n\n")
    userInput = input("Enter your choice (1-12):")
    
    try:
        userInput = int(userInput)

    except:
        print("Invalid input. Please try again!")


    if userInput == 1:
        insert_car_entry()
    elif userInput == 2:
        delete_car_entry()
    elif userInput == 3:
        update_car_entry()
    elif userInput == 4:
        search_car_entry()
    elif userInput == 5:
        count_car_entry()
    elif userInput == 6:
        sort_car_entry()
    elif userInput == 7:
        join_car_entry()
    elif userInput == 8:
        group_car_entry()
    elif userInput == 9:
        subquery_car_entry()
    elif userInput == 10:
        transaction_delete()
    elif userInput == 11:
        state_populate_data()
    elif userInput == 12:
        print("Exiting Interface...")
        

if __name__ == "__main__":
    stateStart()

connection.close()




