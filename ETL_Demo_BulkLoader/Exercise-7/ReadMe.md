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

<img width="840" height="71" alt="image" src="https://github.com/user-attachments/assets/d604b07f-ca20-4e1e-8adf-f659aa354bf3" />



After Dimension Load (DimCustomer)

<img width="812" height="92" alt="image" src="https://github.com/user-attachments/assets/dca62a89-5fb9-4da1-a0fe-a04509d66dd8" />




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

<img width="835" height="100" alt="image" src="https://github.com/user-attachments/assets/557e7d1e-177d-4246-90a9-7c952c337329" />



Here:
CustomerKey → Surrogate Key
CustomerID → Natural Key (from source)

What is DimChannel?

DimChannel is a dimension table that stores information about the sales channel through which transactions occur.

Meaning of Channel:
Channel = Sales source (Online, Store, Partner, Reseller, etc.)


Example:

<img width="825" height="109" alt="image" src="https://github.com/user-attachments/assets/333ac3ba-3e9a-407f-a020-378e47b1bdb6" />


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

