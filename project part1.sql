CREATE TABLE Customer_Dim (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(50) UNIQUE,
    CustomerName VARCHAR(255),
    Email VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(100),
    State VARCHAR(100),
    ZipCode VARCHAR(20),
    Lineage_Id BIGINT
);
CREATE TABLE Employee_Dim (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID VARCHAR(50) UNIQUE,
    EmployeeName VARCHAR(255),
    Department VARCHAR(100),
    Position VARCHAR(100),
    Lineage_Id BIGINT,
    StartDate DATE,
    EndDate DATE,
    Current_Flag BIT
);