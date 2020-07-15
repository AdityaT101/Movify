Project Overview
---

The project deals with creating a recommendation engine of movies to the users.

The project involves performing ETL on data based on Movie_Metadata , Movie_genre , Movie_Casting and the CPI index for the time the movies were released.


Using the information derived from the dataset, the system would be able to answer typical question like –

    a.	Which are the top movie genres based on the rating of the movie.
    b.	Does casting particular people help increase the popularity of the movie.
    c.	Finding out if releasing the film at a particular time - in a particular month might affect the revenue.
    d.	Which are the top 5 genres based on the CPI index.


## Data sources
---
The system uses data from 2 different sources : -
1.	The movies , genre and cast data is based on the following Kaggle dataset.

    - (https://www.kaggle.com/rounakbanik/the-movies-dataset)

    There are multiple files containing the data for movies , genre and cast data.
    These files contain metadata for all 45,000 movies listed in the Full MovieLens Dataset. The dataset consists of movies released on or before July 2017. Data       points include cast, crew, plot keywords, budget, revenue, posters, release dates, languages, production companies, countries, TMDB vote counts and vote           averages.
    This dataset also has files containing 26 million ratings from 270,000 users for all 45,000 movies. Ratings are on a scale of 1-5 and have been obtained from       the official GroupLens website.



2.	The CPI (consumer Price Index) Data is collected from the below source.

    - (https://fred.stlouisfed.org/series/CUSR0000SS62031)


    This dataset contains Consumer Price Index for All Urban Consumers: Admission to Movies, Theaters, and Concerts in U.S. City Average


## Architecture 
---
a.	The above mentioned APIs are used to load data into the local system. This data is in form of a bunch of CSV files. Amongst those, the following are staged on     the S3 bucket –
    * credits.csv
    * movies_metadata.csv
    * ratings.csv 
    * CPI.csv
            
b.	Data is then staged from these files into the Staging tables in redshift using the COPY command. Further this staged data is converted into Dimension and Fact     tables on redshift itself. This is done using the Airflow scheduler where in each task is executed in a particular order. The Architecture  is as shown below.

![Movify](images/Architecture.png)



## Schema
    The Schema for the facts and  dimensions is as shown below. 

![Movify](images/schema.png)



c.	For a staging the “Cast_staging “and the “Genre_staging” tables , some preprocessing is done on the data. This is cause the data is in form of a comma separated JSON structure. For eg, the genre column looks      like this – 
    [{"id": 10749, "name": "Romance"}, {"id": 18, "name": "Drama"}, {"id": 9648, "name": "Mystery"}]

    So to dump this into genre table,  ‘LoadGenreStagingOperator’ first ports in the Genre data from the S3 bucket.
    Then it filters on the Genre_id and Genre_name and stores it in a dataframe object. This is then converted into a csv object
    This csv object is further processed to enter the Genre_Data into the Staging table.

	The Cast data is processed by ‘LoadCastStagingOperator’  in similar way to form the Cast_Staging table. 



## Airflow Pipeline
---

![Movify](images/Data_pipeline.png)

    a.	The data-pipelining is done as follows. The data is ported from S3 into staging and then into Dimension/Fact tables. The tables are created earlier itself.
    b.	Further, we update the tables whenever new set of data is ported in based on the scheduler.
    c.	We also run the quality checks on fact and dimension tables after every cycle is complete.
    d.  Currently the DAG schedules the pipeline only once, but depending upon the requirement we can trigger it periodically everyday.


## Scalabiity and Related points
---
    a.<ins> What if data is increased by 100x?</ins>
       Currently the processing is done on local system. If we need faster processing, then we can employ much faster EC2 instances to handle the load.
       Also, we can use SPARK clusters if required to process this faster. Also, we can program the DAG to take only the subset of data at a time rather than in bulk.

    b. <ins>What if data pipeline needs to be run by 7am daily?</ins>
       We can trigeer the DAG to run at any time daily. Also, currently, the DAG runs of local system. If this was running on an EC2 instance, the DAG would itself run 
       at every regular interval without any disturbance.

    c. <ins>What if the database needs to be accessed by 100+ users?</ins>
       Redshift would be able to handle this load properly. Also, In order to optimize this further the tables can be redesigned / schema can be changed based on most common queries.
       Aggregated data tables can be provided beforehand to make queries more and more efficient.





## To set up and run the DAG
---
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

- Clone this repo
- Install the prerequisites
- Run the service
- Check http://localhost:8080
- Done! :tada:

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)
- Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

### Usage

Run the web service with docker

```
docker-compose up -d

# Build the image
# docker-compose up -d --build
```

Check http://localhost:8080/

- `docker-compose logs` - Displays log output
- `docker-compose ps` - List containers
- `docker-compose down` - Stop containers

## Other commands

If you want to run airflow sub-commands, you can do so like this:

- `docker-compose run --rm webserver airflow list_dags` - List dags
- `docker-compose run --rm webserver airflow test [DAG_ID] [TASK_ID] [EXECUTION_DATE]` - Test specific task

If you want to run/test python script, you can do so like this:
- `docker-compose run --rm webserver python /usr/local/airflow/dags/[PYTHON-FILE].py` - Test python script

## Connect to database

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values:
- Host : {redshift.xxxxxxxxx.us-west-2.redshift.amazonaws.com:5439}
- Schema : {database_name}
- Login : {User_Id}
- Password : {Password}


## Credits

- [Apache Airflow](https://github.com/apache/incubator-airflow)
- [docker-airflow](https://github.com/puckel/docker-airflow/tree/1.10.0-5)
