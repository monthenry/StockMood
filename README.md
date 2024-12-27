# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **[To be assigned]**

### Abstract

## Datasets Description 

## Queries 

## Requirements

* To have docker *and* docker-compose installed.
* Install docker and docker-compose exactly as it is described in the website.
* **do not do do apt install docker or docker-compose**

## How to spin the webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 501 for your own.

Run the following commands to clean up docker containers, images and volumes (use it with or without sudo depending on docker install):
```sh
sudo docker stop $(sudo docker ps -aq)
sudo docker rm $(sudo docker ps -aq)
sudo docker rmi $(sudo docker images -q)
sudo docker compose down --volumes
sudo docker volume prune -f
```

Run the following command to delete files from previous executions (while in the StockMood folder):
```sh
sudo rm -rf config logs plugins data/*.csv data/*.json data/*.tar.gz data/20061020_20131126_bloomberg_news
```

Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./dags ./logs ./plugins
```

And this **once**:
```sh
docker compose up airflow-init
```
If the exit code is 0 then it's all good.

### Running

```sh
docker compose up
```

After it is up, add a new connection:

* Name - postgres_default
* Conn type - postgres
* Host - localhost
* Port - 5432
* Database - airflow
* Username - airflow
* Password - airflow

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

