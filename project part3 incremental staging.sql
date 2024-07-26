Part 2: ETL and SCD Implementation
CREATE TABLE Insignia_staging_copy AS
SELECT * FROM Insignia_staging;
-- Handle SCD Type 1 for Customer Dimension
-- Assume Customer_Dim has no historical changes, just updates.

INSERT INTO Customer_Dim (CustomerID, CustomerName, Email, Address, City, State, ZipCode, Lineage_Id)
SELECT DISTINCT
    CustomerID, CustomerName, Email, Address, City, State, ZipCode, Load_Id
FROM Insignia_staging_copy
WHERE CustomerID NOT IN (SELECT CustomerID FROM Customer_Dim);
-- Handle SCD Type 2 for Employee Dimension
-- Assuming EndDate is NULL for current records

MERGE Employee_Dim AS target
USING (SELECT * FROM Insignia_staging_copy) AS source
ON (target.EmployeeID = source.EmployeeID)
WHEN MATCHED AND (target.Current_Flag = 1) THEN
    UPDATE SET
        target.EndDate = GETDATE(),
        target.Current_Flag = 0
WHEN NOT MATCHED THEN
    INSERT (EmployeeID, EmployeeName, Department, Position, StartDate, EndDate, Lineage_Id, Current_Flag)
    VALUES (source.EmployeeID, source.EmployeeName, source.Department, source.Position, source.StartDate, NULL, source.Lineage_Id, 1);
    -- Handle SCD Type 3 for Geography Dimension
-- Update Population and keep history in Population_History

MERGE Geography_Dim AS target
USING (SELECT * FROM Insignia_staging_copy) AS source
ON (target.GeographyID = source.GeographyID)
WHEN MATCHED THEN
    UPDATE SET
        target.Population_History = CONCAT(target.Population_History, ';', source.Population),
        target.Population = source.Population,
        target.Lineage_Id = source.Lineage_Id
WHEN NOT MATCHED THEN
    INSERT (GeographyID, Country, State, City, Population, Lineage_Id, Population_History)
    VALUES (source.GeographyID, source.Country, source.State, source.City, source.Population, source.Lineage_Id, NULL);
    -- Load Date Dimension with historical data if needed

INSERT INTO Date_Dim (DateKey, Date, Day_Number, Month_Name, Short_Month, Calendar_Month_Number, Calendar_Year, Fiscal_Month_Number, Fiscal_Year, Week_Number)
SELECT DISTINCT
    CONVERT(INT, FORMAT(Date, 'yyyyMMdd')) AS DateKey,
    Date,
    DAY(Date) AS Day_Number,
    DATENAME(MONTH, Date) AS Month_Name,
    FORMAT(Date, 'MMM') AS Short_Month,
    MONTH(Date) AS Calendar_Month_Number,
    YEAR(Date) AS Calendar_Year,
    CASE
        WHEN MONTH(Date) >= 7 THEN MONTH(Date) - 6
        ELSE MONTH(Date) + 6
    END AS Fiscal_Month_Number,
    CASE
        WHEN MONTH(Date) >= 7 THEN YEAR(Date)
        ELSE YEAR(Date) - 1
    END AS Fiscal_Year,
    DATEPART(WEEK, Date) AS Week_Number
FROM Insignia_staging_copy
INSERT INTO Sales_Fact (DateKey, CustomerKey, EmployeeKey, GeographyKey, SalesAmount, Quantity, Lineage_Id)
SELECT
    d.DateKey,
    c.CustomerKey,
    e.EmployeeKey,
    g.GeographyKey,
    s.SalesAmount,
    s.Quantity,
    s.Lineage_Id
FROM Insignia_staging_copy s
JOIN Date_Dim d ON s.Date = d.Date
JOIN Customer_Dim c ON s.CustomerID = c.CustomerID
JOIN Employee_Dim e ON s.EmployeeID = e.EmployeeID
JOIN Geography_Dim g ON s.GeographyID = g.GeographyID;
-- Truncate Insignia_staging_copy before loading incremental data

TRUNCATE TABLE Insignia_staging_copy;

-- Load incremental data into Insignia_staging_copy

INSERT INTO Insignia_staging_copy
SELECT * FROM Insignia_incremental;