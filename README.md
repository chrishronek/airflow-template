# Airflow Repository Template
Use this template for an easy-to-use local development experience of Apache Airflow

## Gotchas
This repository template spins up Airflow for local development purposes only. It is not meant to be used in a 
production instance. Additionally, the only components that get spun up with this template are:
- Scheduler
- Webserver
- Airflow DB

Additionally, this template uses the LocalExecutor (as opposed to CeleryExecutor or KubernetesExecutor) for the sole 
purpose of testing isolated workloads locally.

This repository does NOT include a Triggerer component for deferrable operators (but it could easily be added in the 
`docker-compose.yaml`).

## Getting started
__Prerequisites__
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)

__Step By Step__
1. Ensure that Docker Desktop is installed and running on your computer
2. If your os can recognize Makefiles run `make up`. Otherwise run `docker-compose up --detach`
3. Wait for the docker containers to be healthy
4. Navigate to http://localhost:8080 in your web browser
    - username: `admin`
    - password: `admin`

__Make Commands__
If your using an OS that can recognize [Makefiles](https://opensource.com/article/18/8/what-how-makefile), then feel 
free to utilize the following commands to run Airflow locally:
- `clean`: stop and delete containers, delete volumes with database data and download images
- `down`: stop all services
- `init`: run database migrations and create the first user account
- `reset`: stops all services, delete containers, volumes and images, then start all services
- `restart`: stop and start containers
- `up`: start all services

__Windows Users__
If you are on a system that doesn't recognize Makefiles, you can look at the contents of the `./Makefile` in this 
repository to determine which `docker-compose` commands to run to achieve the same results.

## Managing Connections
There is a `connections_example.yaml` included with this repository. Since actual connection credentials should never be 
committed to a repository, duplicate this file and name it `connections.yaml` (which is in the `.gitignore`). In this 
`connections.yaml` file you can add definitions for Airflow connection objects that will get created when you create the 
Docker Container. That way, if you have to prune your Docker containers, you won't lose them.

An example connection to redshift is included, but you can replace it with any existing connection type supported by
Airflow.

## Managing Variables
There is a `variables_example.json` included with this repository. In some cases, you may want to store sensitive 
information in Airflow variables. Since creds should never be committed to a repository, duplicate this file and name it 
`variables.json` (which is in the `.gitignore`). In this `variables.json` file you can add variables for Airflow 
variable objects that will get created when you create the Docker container. That way, if you have to prune your Docker 
containers, you won't lose them.