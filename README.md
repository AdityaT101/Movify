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


## Architecture and Schema
---
a.	The above mentioned APIs are used to load data into the local system. This data is in form of a bunch of CSV files. Amongst those, the following are staged on     the S3 bucket –
    1.	credits.csv
    2.	movies_metadata.csv
    3.	ratings.csv 
    4.	CPI.csv
            
b.	Data is then staged from these files into the Staging tables in redshift using the COPY command. Further this staged data is converted into Dimension and Fact     tables on redshift itself. This is done using the Airflow scheduler where in each task is executed in a particular order. The schema for the facts and  dimensions is as shown below.


![Movify](images/schema.png)



## Getting Started

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
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow


## Credits

- [Apache Airflow](https://github.com/apache/incubator-airflow)
- [docker-airflow](https://github.com/puckel/docker-airflow/tree/1.10.0-5)
