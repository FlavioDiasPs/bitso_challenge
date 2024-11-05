# Bitso Challenge

## Greetings
Hi! Thank you for giving me the opportunity to be challenged!  
I am Flavio Pegas, and I hope you like what I managed to develop in the limited time I had.

## Deliverables
- Image with the ERD of the data model
  - `data\erd`
- Python 3 source code with your solution or Github repository URL
  - `https://github.com/FlavioDiasPs/bitso_challenge`
- CSVs with the output tables
  - `data\csv`
- SQL or TXT file with the queries that will answer at least 4 cases
  - `data\queries.sql`
- Readme file explaining what modeling techniques did you use, why did you choose them and what would be potential 
downside of this approach

### Bonus
- ETL processes daily batches
- Main script orchestrating the pipeline or the use airflow
  - Dag created with networkx 
- Implement Unit testing

## Explaining

I chose to follow the **medallion architecture** in a way that works well with **Databricks**. The **Bronze**, **Silver**, and **Gold** layers are all Delta tables, which provide strong lineage, data quality, performance, and many other functionalities of Databricks.

Having Bronze tables as Delta tables allows analysts faster access to the data, especially when they are less concerned about data quality or specific scenarios that don't require pre-aggregations.

I didn't consider talking about partitioning because Databricks recomends only partitioning tables over 1 TB. Functionalities like [liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering) and [predictive optimization](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization) are making our lives much easier nowadays. 

### Transition from Data Warehousing

Regarding Data Warehousing, the industry is moving away from traditional Data Warehouses and fully embracing **Lakehouse architectures**. ClearSale is also adopting this approach, which is why I did not consider a traditional warehouse in this architecture.

However, warehouse table modeling will not become obsolete anytime soon. I opted for a **star schema**, aiming to keep it simple and avoid unnecessary complexity given the limited information available. I unified the withdrawals and deposits tables to facilitate queries, as fewer joins enhance performance.

I created the **dim_entity** table because I recognized that not all transactions are peer-to-peer. This dimension provides a broader understanding of the entities involved in transactions. Additionally, I created **dim_date**, which is a best practice. This dimension is beneficial for analytical tools such as **Power BI** and **Tableau**, as it simplifies many dashboard and reporting scenarios.

### Future Considerations

I considered creating dimensions for **dim_jurisdiction**, **dim_product**, and **dim_currency** because the company could grow. While this could be beneficial in the future, I currently lack sufficient information to implement these dimensions; such actions should be taken when necessary.

Regarding Slowly Changing Dimensions (SCD) Type 2, Databricks is addressing it by providing data versioning on Delta tables. Even though SCD 2 may not be needed anymore in the future, these features are still relatively new and are far from suitable for enterprise production environments. That is why dim_entity is a SCD 2 table.


### Downsides
#### Delay to get data ready
Because of bronze, silver, gold and all processes involved, getting data ready may not help scenarios that need data faster.

#### Rigid structure
Start Schema is naturally rigid, changes may take a long time to be adopted in the structure. Once multiple processes are committed with this structure, it is very hard to change.

If Dimensional tables grow too much, query performance will degrade.
#### Complexity
It is complex to keep up with data changes


## Issues during project development
I encountered a few issues during this project that I would like to explain:

### Github only accept files under 100mb
- I wanted to add all files there, so I compacted all raw, delta and sqlite data.
- If you want to test it or see it, you will have to umcompress it.

### I Am Not at Home
- Currently, I am not using my personal computer.
- This has led to multiple problems with permissions.
- I cannot download any `.exe` files.
- I cannot install software. (mysql, postgre, etc)

### I Cannot Install Airflow on This Computer
- We no longer use Airflow at the company.
- I attempted to install it, but it only runs on WSL2 or Linux environments, which is not the case here.
- I am not allowed to enable WSL2.
- The work to add Windows support is tracked here: `https://github.com/apache/airflow/issues/10388`

### I Cannot Use Spark Locally
- Spark requires lower versions (8, 9), but I currently have versions 10 and 11.

### I Do Not Have Docker Installed
- I had it but removed the license since I do not use it as a manager.

### Considerations on Databricks
I considered providing Databricks code, which would have been easier, but the exercise required Python 3, scheduling, and all that. Therefore, I tried not to use Databricks DLT or PySpark.
- Just to be clear, I would never create or allow Python pipelines like this in a production environment. However, I still think they are a good proof of technical expertise.
- Silver and gold analytical processes should certainly be conducted using Databricks PySpark.

## What Was Built?
### ERD
I used **Mermaid** to create the ERD:
- [Mermaid Chart](https://www.mermaidchart.com/)
- Both the ERD Mermaid code and image can be found here:
  - `data/erd/er_diagram.png`
  - `data/erd/erd_mermaid.mmd`

### File Formats
Nowadays, using one of the top three analytical file formats is essential (hudi, iceberg and delta). 
With that in mind, I used Delta Lake with DuckDB to create bronze, silver, and gold tables. I saved all of them in three formats:
- **Delta**: `data/delta`
- **CSV**: `data/csv` (as required by the exercise)
- **SQLite**: `data/bitso.db` (for querying locally)

Exercise queries can be found here: `data/queries.sql`.

### Dag
Since I cannot install Airflow, I decided to create my own DAG:
- I used the `networkx` library to assist with that.
- You can export the DAG, which will generate this image: `data/dag.png`.
- The configuration behind it is located in `src/config.py`.

### Challenges with SCD Type 2
Creating SCD Type 2 without proper environment is challenging, I managed to build it using DuckDB:
- Implementation: `src/dal/queries/populate_scd_dim_entity_duckdb.py`
- All queries used are located in `src/dal/queries`.
- Funny story: DuckDB released 1.1.3 while I was testing and broke multiple things with a generic error. It took me a while to figure that out... https://pypi.org/project/duckdb/#history

### Running the project
- Before running it, decompress the raw.7z
- I left the output folder structure for csv and delta there, so the program will generate all csv, delta and sqlite data automatically.

## Things That Did Not Work Out Due to Time Constraints
- **Incremental Batches**:
  - I lost too much time with permission issues and trying different libraries.
  - If you check `src/main.py` and `data/raw/incremental_sample`, files were splitted and ready, and an interval-based orchestrator was also prepared, but SCD on DuckDB is painful.

- **Data Quality with Great Expectations**:
  - Although I added some standardizations and dropped duplicates during silver processing, using Great Expectations would have been awesome to assert data quality issues. I even started creating something, but got stuck with pandas version incompatibility. I decided to move on due to short time: `bitso_challenge/tests/great_expectations/silver_tables.py`.

## Development Environment
I used **VSCode** during development. If you use PyCharm, there may be compatibility issues.

Have a great day!