/*
=============================================================
Datawarehouse Database and Schema Creation Script
=============================================================
Author: Jesco Wurm aka. RaptileBytez
Date: 06-04-2025

Script purpose:
    This script creates a new SQL Server database named 'Datawarehouse' after checking if it already exists 
    and sets up three schemas within it: 'Bronze', 'Silver', and 'Gold'.

WARNING:
    This script will drop the existing 'Datawarehouse' database if it exists,
    All data in the database will be permanently lost.
    Proceed with caution and ensure you have backups if necessary.
=============================================================
*/

USE master;

GO
-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END;

GO
-- Create the 'Datawarehouse' database
CREATE DATABASE Datawarehouse;

GO
-- Create the 'Bronze', 'Silver', and 'Gold' schemas in the 'Datawarehouse' database
USE Datawarehouse;
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze')
END

GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver')
END

GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold')
END
