# Data backend serving Dune downloads

## Data flow for the dune-bridge

There are 3 different data steams that need to be transferred between the backend and the dune queries:

- *appData hashes* that were sent with transactions on the ethereum chain: 
They are needed by the backend to look up the corresponding ipfs files, in order to index the meta-data of an order.
- *appData-referral mapping*: Once the backend parsed the appData and read the corresponding ipfs files, it is able to create a mapping from the appData to the referrals.
This information is needed to build the correct dune queries for the dune download. 
- *main-dune-query-data*: The main dune query calculates the trading volumes for the referrals, the usual trading volumes and some other key metrics. Since the calculation and download of the complete data for each user takes quite some time, the download is split for the current day and the rest of the entire history
    - entire history download: this file is only created once and it will reflect the complete history of trading data until a certain date. From that date, only daily downloads will be taken
    - daily downloads: Every 30 mins, a new daily download with the newest data for the day will be fetched from dune and the data in the backend is updated.

In the first version all the data is stored in simple json files. Later, we will consider building real databases. 
The data flows are driven by 2 different cronjobs. 
The first job updates and executes the queries for the appData and the main-dune-query for the daily download, each 30 mins. 
Then 15 mins later, a second job is starting the download of the query results. 
The backend-api will continously look for new downloads from dune in a maintaince loop and read the new data, serve it via an api and create new appData-referral mappings.