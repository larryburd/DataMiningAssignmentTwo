-- SQL stored procedures to insert and select
-- data from the store sales database

-- Insert a new sales record
-- The logic whether to build new records in the dimension tables is done in the build_db.py file

CREATE OR REPLACE PROCEDURE insert_fact_sale(cust_id VARCHAR, trans_date DATE, prod_id INT, loc_id INT, num_items INT, unit_price DECIMAL, total_spent DECIMAL)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO Fact_Sales(custID, transDate, prodID, locID, quantity, pricePerUnit, totalSpent)
    VALUES (cust_id, trans_date, prod_id, loc_id, num_items, unit_price, total_spent)
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_customer(cust_id VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Customers
    VALUES(cust_id)
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_Date(trans_date DATE, is_weekend BOOLEAN, month_name VARCHAR, sales_quarter INT, fy INT)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Dates
    VALUES(trans_date, is_weekend, month_name, sales_quarter, fy)
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_products(cat VARCHAR, prod_name VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Products(category, name)
    VALUES(cat, prod_name)
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_locations(loc_name VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Locations(name)
    VALUES(loc_name)
END;
$$;