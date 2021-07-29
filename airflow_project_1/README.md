This project fetches data from wikipedia's API every day
I fetched the following keywords: 
Google", "Amazon", "Apple", "Microsoft", "Facebook"
from the dumps and used airflow to automate the ETL pipeline.

the averagePageView sql script applies a row_number window function
on all the different tickers and sorts them in average pageview_number.
I inferred that the company name having the biggest average pageview_number
is the one getting the most traction. 
