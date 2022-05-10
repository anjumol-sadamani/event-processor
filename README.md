# Event Processor
> An event processing service in go that receives input data from a Kafka Producer, stores data to DB and outputs an aggregated report.

## Table of Contents
- [Event Processor](#event-processor)
  - [Table of Contents](#table-of-contents)
  - [Input Data](#input-data)
  - [Technologies Used](#technologies-used)
  - [Dependencies](#dependencies)
  - [Features Covered](#features-covered)
  - [Setup](#setup)
  - [Usage](#usage)


## Input Data
- Input Data 1 - Schema | Is parsed and stored in memory and DB 
- Input Data 2 - Queries | Queries are stored to DB with required additional metadata

## Technologies Used
- Go - version 1.18
- Docker
- Docker Compose
- PostgresSQL

## Dependencies
- Gin - https://github.com/gin-gonic/gin
- Gorm - https://gorm.io/

## Features Covered
- Aggregated Schema Usage as API
- Group results by day of month
- Considered for high throughput using channels for queuing
- Considered for recursive queries
- API for grouping result by Client, Client Version and Data Center
- Workers implemented using go routines

## Setup
1.  Prerequisites - Docker, Docker Compose
2.  Clone the repository
3.  Deploy depended Kafka cluster and Databases using command `docker-compose up` from project root
4.  GET Endpoints are exposed at http://localhost:3000/event-processor/api/v1/ 

## Usage

1.  Schema Usage
   
    GET - http://localhost:3000/event-processor/api/v1/count

    Output - 
    ```json
    {
    "deal": {
        "price": 371,
        "title": 335,
        "user": 335
    },
    "query": {
        "deal": 0,
        "user": 0
    },
    "user": {
        "deals": 0,
        "name": 196
    }
    }
    ```
2.  Schema Usage by Day of month
   
    GET - http://localhost:3000/event-processor/api/v1/countByDay

    Output - 
    ```json
    [
    {
        "deal": {
            "price": 378,
            "title": 345,
            "user": 345
        },
        "processed_time": "2022-05-10T00:00:00Z",
        "query": {
            "deal": 0,
            "user": 0
        },
        "user": {
            "deals": 0,
            "name": 199
        }
    }
    ]
    ```

3.  Schema Usage by client metadata
   
    GET - http://localhost:3000/event-processor/api/v1/countByClient

    Output - 
    ```json
    [
    {
        "deal": {
            "price": 378,
            "title": 345,
            "user": 345
        },
        "processed_time": "2022-05-10T00:00:00Z",
        "query": {
            "deal": 0,
            "user": 0
        },
        "user": {
            "deals": 0,
            "name": 199
        }
    }
    ]
    ```
   