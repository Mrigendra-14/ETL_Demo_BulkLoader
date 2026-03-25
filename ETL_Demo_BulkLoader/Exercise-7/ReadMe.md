What is Dimension Load?

Dimension Load is the process of populating dimension tables 
with descriptive data from source systems.

In this exercise, data will be directly taken from AdventureWorks 
and loaded into dimension tables using MERGE (UPSERT logic).

It is the step where we store business-related descriptive information
(like customer name, email, region, etc.) into dimension tables.

Why is it required:

•	To store clean and structured business data
•	To support reporting and analytics
•	To avoid repeated joins from raw tables

Example:

Source:
Customer ID	Name	Email
100	John	john@gmail.com


After Dimension Load (DimCustomer)

CustomerKey	CustomerID	Name	Email
1	100	John	john@gmail.com



This loading process = Dimension Load

What is DimCustomer?


DimCustomer is a dimension table that stores customer-related descriptive information for reporting and analysis.

It will contain:
•	Customer name
•	Email Address
•	Territory / Region
•	CustomerID (Natural Key)
•	CustomerKey (Surrogate Key)

Example:

CustomerKey	CustomerID	Name	Email
1	100	John smith	john@gmail.com


Here:
CustomerKey → Surrogate Key
CustomerID → Natural Key (from source)

What is DimChannel?

DimChannel is a dimension table that stores information about the sales channel through which transactions occur.

Meaning of Channel:
Channel = Sales source (Online, Store, Partner, Reseller, etc.)


Example:

ChannelKey	ChannelName
1	Online
2	Store

What is DimBrand?


DimBrand is a dimension table that stores product brand information.
Source (AdventureWorks)

Brand can be derived from Product or ProductCategory tables

Example:


ProductName	SubCategory	Category(Brand)
HL Road Frame - Black, 58	Road Frames	Components
AWC Logo Cap	Caps	Clothing


Surrogate Key vs Natural Key


Surrogate Key: 

Surrogate Key is an artificial key created in the data warehouse.

Example:

CustomerKey (IDENTITY column)

Column	Value
CustomerKey	1



Characteristics of Surrogate Key:

•	Auto-generated (IDENTITY)
•	No business meaning
•	Stable and unique
•	Best for joins

Natural Key:  Natural Key is a business key that comes from the source system.

Example:


Column	Value
CustomerID	100


Characteristics of Natural Key:


It Comes from source, it has business meaning

CustomerKey	CustomerID	Name
1	100	John
2	101	Alen



Why we need Surrogate Key?


•	If source changes -> Data warehouse remains stable
•	Faster joins
•	Supports historical tracking (SCD)

Without Surrogate Key: CustomerID changes from 100 -> 200, and then all relationships may be broken. 

 
With Surrogate Key: CustomerKey remains the same -> No issue 
Natural Key → Comes from source (CustomerID)
Surrogate Key → Created in Data Warehouse (CustomerKey)

Differences:

Natural keys are owned by the source system, so the source can change them anytime. Surrogate keys are owned by the data warehouse itself, so the warehouse controls stability.

