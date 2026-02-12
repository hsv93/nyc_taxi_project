# NYC Taxi Data Engineering Project 🚕

## Description
This project demonstrates an end-to-end ETL pipeline for processing large-scale datasets using the "2023 Yellow Taxi Trip Data" (approx. 3 million records). 

The core of the project is a comparative analysis: I implemented data cleaning and transformation using two distinct methods:
1. **Python/Pandas**: Focus on vectorization and memory management.
2. **T-SQL**: Focus on set-based operations directly within the database.

After processing, I performed a data validation and performance comparison between both methods to determine the most efficient workflow for this scale of data.

## Tech Stack
* **Language:** Python / SQL
* **Libraries:** Pandas, NumPy, SQLAlchemy, PyODBC
* **Database:** Microsoft SQL Server (Docker container)
* **SO:** macOS (Apple Silicon)