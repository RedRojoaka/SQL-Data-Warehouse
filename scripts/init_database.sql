/*

=============================
Create Database and Schemas
=============================
Purpose:
	Script creates new database named "DataWarehouse" after checking if it already exists.
	If database exists, it is dropped and recreated. Additionally, the script also creates and set up three schemas
	labeled as "bronze", "silver", and "gold".

Warning:
	Running script will drop entire "DataWarehouse" database if it exists.
	All data in the database will permanatly be deleted. Proceed with caution
	and ensure you have made proper backups before running the script.

*/

-- Create DataBase  "DataWarehouse"

USE master;
GO

-- Recreationg and dropping of the "DataWarehouse" database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	Alter DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Creating database "DataWarehouse"
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating Schemas
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
