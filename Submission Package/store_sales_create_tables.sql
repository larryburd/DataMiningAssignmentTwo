CREATE TABLE Dim_Products (
    id SERIAL NOT NULL PRIMARY KEY,
    prodName VARCHAR(75),
    category VARCHAR(75)
);

CREATE TABLE Dim_Customers (
    id VARCHAR(10) NOT NULL PRIMARY KEY	
);

CREATE TABLE Dim_Dates (
    transDate DATE NOT NULL PRIMARY KEY,
    isWeekend BOOLEAN,
    transMonth VARCHAR(15),
    quarter INT,
    fiscalYear INT
);

CREATE TABLE Dim_Locations (
    id SERIAL NOT NULL PRIMARY KEY,
    name VARCHAR(25)
);

CREATE TABLE Fact_Sales (
    id SERIAL NOT NULL PRIMARY KEY,
    custID VARCHAR(10),
    transDate DATE,
    prodID INT,
    locID INT,
    quantity INT,
    pricePerUnit DECIMAL,
    totalSpent DECIMAL,
    CONSTRAINT fk_customer FOREIGN KEY (custID)
    REFERENCES Dim_Customers(id),
    CONSTRAINT fk_date FOREIGN KEY (transDate)
    REFERENCES Dim_Dates(transDate),
    CONSTRAINT fk_product FOREIGN KEY (prodID)
    REFERENCES Dim_Products(id),
    CONSTRAINT fk_location FOREIGN KEY (locID)
    REFERENCES Dim_Locations(id)
);