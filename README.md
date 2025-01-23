# Data Quality and Analysis Project

## Overview

This project involves analyzing data quality issues, designing a structured relational data model, answering key business questions, and communicating insights to stakeholders. The entire project was implemented using **Snowflake**, leveraging its advanced SQL capabilities and data warehousing features.

----

## Project Objectives

1. **Data Modeling:** Transform unstructured JSON data into a structured relational model suitable for analytics.
2. **Data Quality Assessment:** Identify and resolve key data quality issues in the dataset.
3. **Business Analysis:** Answer specific business questions using SQL queries.
4. **Stakeholder Communication:** Document findings and communicate actionable insights to business stakeholders and analytics engineers.

---

## **Key Features**

- **Relational Data Model:**  
  A structured relational schema was designed, including the following tables:
  - `consumer_profiles` (Users)
  - `product_brands`
  - `transaction_receipts`
  - `transaction_items`

### 1. Data Ingestion
- Loaded raw JSON files into Snowflake staging tables using Snowflake's `COPY INTO` command.

### 2. Data Transformation
- Flattened the nested `receiptItems` field using **LATERAL FLATTEN** to extract item-level details for the `transaction_items` table.
- Created structured relational tables:
  - `consumer_profiles`
  - `product_brands`
  - `transaction_receipts`
  - `transaction_items`
  - 

- **Business Queries:**  
  - Identified top brands by transactions and spending trends.
  - Analyzed average spending and total items purchased for specific receipt statuses.
  - Addressed data quality issues, such as missing or inconsistent values.

- **Data Quality Analysis:**  
  SQL queries were used to identify:
  - Missing or invalid values across key tables.
  - Orphaned records and mismatched foreign keys.
  - Dominance of "Unknown Brands" due to incomplete data.

---

## **Technologies Used**

- **Snowflake**: Data warehousing and SQL for data transformation, analysis, and quality checks.
- **SQL**: Core language for querying and transforming data.
- **Git**: Version control for project files.
- **LucidChart**: For creating the relational schema diagram.


## **How to Run the Project**

### **1. Prerequisites**
- A **Snowflake** account with appropriate permissions.
- A Git client for version control.

## **Key Business Questions Addressed**

1. **Top 5 Brands by Receipts Scanned:**  
   Identified the most popular brands in the most recent and previous months.

2. **Comparison of Accepted vs. Rejected Receipts:**  
   Compared average spending and total items purchased between "Accepted" and "Rejected" receipts.

3. **Spending Analysis for Recent Users:**  
   Found the brand with the highest spending among users who joined within the last 6 months.

4. **Transaction Trends by Brand:**  
   Analyzed transaction volume and spending trends over time.

---

## **Data Quality Insights**

- **Missing Data:**  
  - 40% of rewards receipt items are missing.
  - 13% of product categories are incomplete.

- **Dominance of "Unknown Brand":**  
  - Represents 98.8% of total receipts due to missing or unmatched brand information.

- **Orphaned Records:**  
  - Found receipts without associated items and items without matching brands.

---

## **Challenges and Solutions**

### **Performance and Scaling:**
- **Challenge:** Snowflake query performance with large datasets.
- **Solution:** Applied indexing, optimized table structures, and used Snowflake's partitioning for efficient querying.

### **Data Consistency:**
- **Challenge:** Handling missing and inconsistent data.
- **Solution:** Added data validation checks during the ETL process to identify and resolve issues.

---

## **Next Steps**

1. **Address Missing Data:** Work with the upstream systems to reduce missing fields, especially for brand and category information.
2. **Automate Quality Checks:** Implement automated data validation checks in the ETL pipeline.
3. **Enhance Stakeholder Reporting:** Build dashboards for real-time insights into data quality and trends.

---

## **Acknowledgments**

Special thanks to the team for their support in completing this project. Let me know if there are any additional features or analyses you'd like to see!
