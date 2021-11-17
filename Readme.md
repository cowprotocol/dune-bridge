# Data Backend Serving Dune Downloads

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
The backend-api will continuously look for new downloads from dune in a maintenance loop and read the new data, serve it via an api and create new appData-referral mappings.



## Instructions for getting data from dune


### installation

Preparations:

```
cd dune_api_scripts
python3 -m venv env
source ./env/bin/activate
pip install -r requirements.txt
```
Setting some envs:
```
export DUNE_DATA_FOLDER=./data/dune_data/
export APP_DATA_REFERRAL_RELATION_FILE=./data/referral_data/app_data_referral_relationship.json        
export DUNE_USER=<User>
export DUNE_PASSWORD=<pwd>
```

### Download data:

Pulling new query results:

```
python -m dune_api_scripts.store_query_result_all_distinct_app_data.py
python -m dune_api_scripts.store_query_result_for_entire_history_trading_data.py
python -m dune_api_scripts.store_query_result_for_todays_trading_data.py
```


Update query:
```
python -m dune_api_scripts.modify_and_execute_dune_query_for_entire_history_trading_data.py
python -m dune_api_scripts.modify_and_execute_dune_query_for_todays_trading_volume.py
python -m dune_api_scripts.execute_dune_query_for_all_app_data
```

Alternatively, the scripts can also be run via docker:
```
docker build -t fetch_script -f ./docker/Dockerfile.binary .
docker run -e DUNE_PASSWORD=<pwd> -e DUNE_USER=alex@gnosis.pm -e REFERRAL_DATA_FOLDER=/usr/src/app/data/ -v ./data/:/usr/src/app/data -ti fetch_script /bin/sh
```


## Instructions for running the api

Running the api with the data form user_data.json:
```
cargo run
```


and then check the local endpoint like this:

```
http://127.0.0.1:8080/api/v1/profile/0xa4a6ef5c494091f6aeca7fa28a04a219dd0f31b5
or
http://127.0.0.1:8080/api/v1/profile/0xe7207afc5cd57625b88e2ddbc4fe9de794a76b0f
```

Alternatively, the code can also be run via docker:

1. Running api
```
docker build -t gpdata -f docker/Dockerfile.binary . 
docker run -ti  -e DUNE_DATA_FOLDER='/usr/src/app/data'  gpdata gpdata           
```

