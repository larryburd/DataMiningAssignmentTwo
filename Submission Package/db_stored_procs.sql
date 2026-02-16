-- SQL stored procedures to insert and select
-- data from the store sales database

-- Insert Stored Procs
-- The logic whether to build new records in the dimension tables is done in the clean_and_warehouse_retail_data.py file

CREATE OR REPLACE PROCEDURE insert_fact_sale(cust_id VARCHAR, trans_date DATE, prod_id INT, loc_id INT, num_items INT, unit_price DECIMAL, total_spent DECIMAL)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO Fact_Sales(custID, transDate, prodID, locID, quantity, pricePerUnit, totalSpent)
    VALUES (cust_id, trans_date, prod_id, loc_id, num_items, unit_price, total_spent);
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_customer(cust_id VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Customers
    VALUES(cust_id);
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_date(trans_date DATE, is_weekend BOOLEAN, month_name VARCHAR, sales_quarter INT, fy INT)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Dates
    VALUES(trans_date, is_weekend, month_name, sales_quarter, fy);
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_products(cat VARCHAR, prod_name VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Products(category, prodname)
    VALUES(cat, prod_name);
END;
$$;

CREATE OR REPLACE PROCEDURE insert_dim_locations(loc_name VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN 
    INSERT INTO Dim_Locations(name)
    VALUES(loc_name);
END;
$$;

-- Retrieval Functions
CREATE OR REPLACE FUNCTION transactions_by_cust()
RETURNS TABLE(
    cust_id VARCHAR,
    trans_date DATE,
    month_name VARCHAR,
    qtr INT,
    fy INT,
    is_weekend BOOLEAN,
    loc_name VARCHAR,
	items VARCHAR[],
	item_cats VARCHAR[],
    total_quantity BIGINT,
    total_sales NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        fs.custid::VARCHAR,
        fs.transdate::DATE,
        dd.transmonth::VARCHAR,
        dd.quarter::INT,
        dd.fiscalyear::INT,
        dd.isweekend::BOOLEAN,
        dl.name::VARCHAR,
		ARRAY_AGG(dp.prodname)::VARCHAR[],
		ARRAY_AGG(dp.category)::VARCHAR[],
        SUM(fs.quantity)::BIGINT,
        SUM(fs.totalspent)::NUMERIC
    FROM fact_sales fs
    LEFT JOIN dim_locations dl ON fs.locid = dl.id
    LEFT JOIN dim_dates dd ON fs.transdate = dd.transdate
	LEFT JOIN dim_products dp ON fs.prodid = dp.id
    GROUP BY
        fs.custid, fs.transdate, dl.name, dd.transmonth, dd.quarter, dd.fiscalyear, dd.isweekend
    ORDER BY 
        fs.custid, fs.transdate, dl.name;
END;
$$;

CREATE OR REPLACE FUNCTION products_bought_by_date()
RETURNS TABLE(
    trans_date DATE,
	month_name VARCHAR,
    fy INT,
    qtr INT,
    is_weekend BOOLEAN,
    items VARCHAR[],
    item_cats VARCHAR[]
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        fs.transdate::DATE,
		dd.transmonth::VARCHAR,
        dd.fiscalyear::INT,
        dd.quarter::INT,
		dd.isweekend::BOOLEAN,
        ARRAY_AGG(dp.prodname)::VARCHAR[],
        ARRAY_AGG(dp.category)::VARCHAR[]
    FROM fact_sales fs
    LEFT JOIN dim_dates dd ON fs.transdate = dd.transdate
    LEFT JOIN dim_products dp ON fs.prodid = dp.id
    GROUP BY
        fs.transdate, dd.transmonth, dd.fiscalyear, dd.quarter, dd.isweekend
    ORDER BY
        fs.transdate, dd.transmonth, dd.fiscalyear, dd.quarter;
END;
$$;