CREATE TABLE Geography_Dim (
    GeographyKey INT IDENTITY(1,1) PRIMARY KEY,
    GeographyID VARCHAR(50) UNIQUE,
    Country VARCHAR(100),
    State VARCHAR(100),
    City VARCHAR(100),
    Population INT,
    Lineage_Id BIGINT,
    Population_History VARCHAR(MAX) -- SCD Type 3
);
CREATE TABLE Date_Dim (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day_Number INT,
    Month_Name VARCHAR(50),
    Short_Month CHAR(3),
    Calendar_Month_Number INT,
    Calendar_Year INT,
    Fiscal_Month_Number INT,
    Fiscal_Year INT,
    Week_Number INT
);
CREATE TABLE Sales_Fact (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    EmployeeKey INT,
    GeographyKey INT,
    SalesAmount DECIMAL(18, 2),
    Quantity INT,
    Lineage_Id BIGINT,
    FOREIGN KEY (DateKey) REFERENCES Date_Dim(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES Customer_Dim(CustomerKey),
    FOREIGN KEY (EmployeeKey) REFERENCES Employee_Dim(EmployeeKey),
    FOREIGN KEY (GeographyKey) REFERENCES Geography_Dim(GeographyKey)
);
CREATE TABLE Lineage (
    Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_System VARCHAR(100),
    Load_Stat_Datetime DATETIME,
    Load_EndDatetime DATETIME,
    Rows_at_Source INT,
    Rows_at_destination_Fact INT,
    Load_Status BIT
);