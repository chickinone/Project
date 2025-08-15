CREATE DATABASE laptop_final;

-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE DIM_DATE (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    DayOfMonth SMALLINT,
    DayName VARCHAR(10),
    IsWeekday BOOLEAN,
    MonthNumber SMALLINT,
    MonthName VARCHAR(15),
    QuarterNumber SMALLINT,
    YearNumber SMALLINT,
    WeekOfYear SMALLINT,
    IsHoliday BOOLEAN,
    IsMonthEnd BOOLEAN,
    IsMonthStart BOOLEAN,
    IsQuarterStart BOOLEAN,
    IsQuarterEnd BOOLEAN,
    IsYearEnd BOOLEAN,
    IsYearStart BOOLEAN
);

CREATE TABLE DIM_CUSTOMER (
    CustomerKey INT PRIMARY KEY,
    CustomerID VARCHAR(50),
    FullName VARCHAR(100),
    Gender VARCHAR(10),
    DateOfBirth DATE,
    Age INT,
    PhoneNumber VARCHAR(40),
    Email VARCHAR(100),
    Address VARCHAR(255),
    City VARCHAR(50),
    Province VARCHAR(50),
    Country VARCHAR(100),
    CustomerType VARCHAR(20),
    IsLoyalCustomer BOOLEAN,
    JoinDate DATE,
    IsActive BOOLEAN,
    CreatedAt DATE
);

CREATE TABLE DIM_CUSTOMER_2 (
    CustomerSurrogateKey SERIAL PRIMARY KEY,
    CustomerKey INT,
    CustomerID VARCHAR(20),
    FullName VARCHAR(100),
    Gender VARCHAR(10),
    DateOfBirth DATE,
    PhoneNumber VARCHAR(30),
    Email VARCHAR(100),
    Address VARCHAR(255),
    City VARCHAR(100),
    Province VARCHAR(100),
    Country VARCHAR(100),
    CustomerType VARCHAR(50),
    IsLoyalCustomer BOOLEAN,
    JoinDate DATE,
    IsActive BOOLEAN,
    Version INT,
    EffectiveFrom DATE,
    EffectiveTo DATE,
    IsCurrent BOOLEAN,
    Age INT
);

CREATE TABLE DIM_BRAND (
    BrandKey INT PRIMARY KEY,
    BrandID VARCHAR(50),
    BrandName VARCHAR(100),
    Country VARCHAR(50),
    FoundedYear SMALLINT,
    Website VARCHAR(100)
);

CREATE TABLE DIM_CATEGORY (
    CategoryKey INT PRIMARY KEY,
    CategoryID VARCHAR(50),
    CategoryName VARCHAR(100),
    Description TEXT
);

CREATE TABLE DIM_STORE (
    StoreKey INT PRIMARY KEY,
    StoreID VARCHAR(50),
    StoreName VARCHAR(100),
    City VARCHAR(50),
    Province VARCHAR(50),
    Country VARCHAR(50),
    ManagerName VARCHAR(100),
    OpenedDate DATE
);

CREATE TABLE DIM_SUPPLIER (
    SupplierKey INT PRIMARY KEY,
    SupplierID VARCHAR(50),
    SupplierName VARCHAR(100),
    ContactName VARCHAR(100),
    PhoneNumber VARCHAR(40),
    Email VARCHAR(100),
    Country VARCHAR(50)
);

CREATE TABLE DIM_PAYMENT_METHOD (
    PaymentMethodKey INT PRIMARY KEY,
    PaymentMethodID VARCHAR(50),
    PaymentMethodName VARCHAR(100),
    Provider VARCHAR(100),
    IsOnline BOOLEAN,
    IsActive BOOLEAN,
    CreatedAt DATE
);

CREATE TABLE DIM_PRODUCT (
    ProductKey SERIAL PRIMARY KEY,
    Name VARCHAR(355),
    Model VARCHAR(100),
    BrandKey INT,
    CategoryKey INT,
    CPU VARCHAR(200),
    Cores INT,
    Threads INT,
    CpuSpeedGHz DECIMAL(10,2),
    MaxSpeed VARCHAR(200),
    GPU VARCHAR(300),
    RAMValue INT,
    RAMUnit VARCHAR(100),
    RAMMaxValue INT,
    RAMMaxUnit VARCHAR(100),
    RAMSpeed VARCHAR(700),
    RAMType VARCHAR(50),
    StorageValue INT,
    StorageUnit VARCHAR(100),
    ScreenSize VARCHAR(200),
    ScreenResolution VARCHAR(500),
    ScreenRefreshRate INT,
    ScreenRefreshRateUnit VARCHAR(100),
    ScreenTech VARCHAR(700),
    ScreenDescription TEXT,
    ColorCoverage VARCHAR(700),
    SoundTech VARCHAR(700),
    HasKeyboardBacklight BOOLEAN,
    HasWebcam BOOLEAN,
    HasWireless BOOLEAN,
    Ports TEXT,
    OperatingSystem VARCHAR(700),
    BatterySpec TEXT,
    Material VARCHAR(700),
    Features TEXT,
    ProductDescription TEXT,
    ReleaseDate DATE,
    IsActive BOOLEAN,
    Price DECIMAL(18,2),
    CurrentPrice DECIMAL(18,2),
    DiscountRate DECIMAL(19,3),
    ProductLink TEXT,
    ProductImage TEXT,
    FOREIGN KEY (BrandKey) REFERENCES DIM_BRAND(BrandKey),
    FOREIGN KEY (CategoryKey) REFERENCES DIM_CATEGORY(CategoryKey)
);

CREATE TABLE DIM_EMPLOYEE (
    EmployeeKey INT PRIMARY KEY,
    EmployeeID VARCHAR(50),
    FullName VARCHAR(100),
    Gender VARCHAR(10),
    Position VARCHAR(50),
    StoreKey INT,
    HireDate DATE,
    IsActive BOOLEAN,
    FOREIGN KEY (StoreKey) REFERENCES DIM_STORE(StoreKey)
);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE FACT_ORDER (
    OrderID INT PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    EmployeeKey INT,
    StoreKey INT,
    PaymentMethodKey INT,
    TotalAmount DECIMAL(18,2),
    DiscountTotal DECIMAL(18,2),
    FOREIGN KEY (DateKey) REFERENCES DIM_DATE(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DIM_CUSTOMER(CustomerKey),
    FOREIGN KEY (EmployeeKey) REFERENCES DIM_EMPLOYEE(EmployeeKey),
    FOREIGN KEY (StoreKey) REFERENCES DIM_STORE(StoreKey),
    FOREIGN KEY (PaymentMethodKey) REFERENCES DIM_PAYMENT_METHOD(PaymentMethodKey)
);

CREATE TABLE FACT_ORDER_DETAIL (
    OrderDetailID INT PRIMARY KEY,
    OrderID INT,
    ProductKey INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TotalPrice DECIMAL(18,2),
    OrderDiscount DECIMAL(18,2),
    FinalPrice DECIMAL(18,2),
    FOREIGN KEY (OrderID) REFERENCES FACT_ORDER(OrderID),
    FOREIGN KEY (ProductKey) REFERENCES DIM_PRODUCT(ProductKey)
);

CREATE TABLE FACT_SALES (
    SalesID INT PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    ProductKey INT,
    EmployeeKey INT,
    StoreKey INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    SalesDiscount DECIMAL(18,2),
    FinalAmount DECIMAL(18,2),
    FOREIGN KEY (DateKey) REFERENCES DIM_DATE(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DIM_CUSTOMER(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DIM_PRODUCT(ProductKey),
    FOREIGN KEY (EmployeeKey) REFERENCES DIM_EMPLOYEE(EmployeeKey),
    FOREIGN KEY (StoreKey) REFERENCES DIM_STORE(StoreKey)
);

CREATE TABLE FACT_WEB_TRAFFIC (
    TrafficID INT PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    ProductKey INT,
    ActionType VARCHAR(50),
    DeviceType VARCHAR(50),
    Browser VARCHAR(50),
    SessionID VARCHAR(100),
    TimeSpentSeconds INT,
    Referrer VARCHAR(100),
    IP VARCHAR(45),
    FOREIGN KEY (DateKey) REFERENCES DIM_DATE(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DIM_CUSTOMER(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DIM_PRODUCT(ProductKey)
);

CREATE TABLE FACT_INVENTORY (
    InventoryID INT PRIMARY KEY,
    DateKey INT,
    ProductKey INT,
    StoreKey INT,
    QuantityInStock INT,
    ReorderLevel INT,
    FOREIGN KEY (DateKey) REFERENCES DIM_DATE(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DIM_PRODUCT(ProductKey),
    FOREIGN KEY (StoreKey) REFERENCES DIM_STORE(StoreKey)
);

CREATE TABLE FACT_RETURN (
    ReturnID INT PRIMARY KEY,
    DateKey INT,
    CustomerKey INT,
    ProductKey INT,
    Quantity INT,
    ReturnReason VARCHAR(255),
    RefundAmount DECIMAL(18,2),
    FOREIGN KEY (DateKey) REFERENCES DIM_DATE(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DIM_CUSTOMER(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DIM_PRODUCT(ProductKey)
);