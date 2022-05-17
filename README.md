# Event Processor

> An event processing service in go that receives input data from a Kafka Producer, stores data to DB and outputs an
> aggregated report.

## Table of Contents

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

- Service outputs schema usage.
  ie, Count the number of times a query field(tables or columns) is used on processed input.
  (Note : On counting the tables in the root of the json,should output "Query" as key and the count of tables as value.
  On counting the columns Table name should be the key and count of cols will be the value )
-

sample output :

````json
 {
  "Query": {
    "table1": "2",
    "table2": "3"
  },
  "table1": {
    "col1": "10",
    "col2": "20"
  },
  "table2": {
    "col1": "10",
    "col2": "20"
  },
  ...
}
  ````

- Group results by day
- For high throughput used channels for queuing
- API for grouping result by Client, Client Version and Data Center
- Workers implemented using go routines

## Setup

1. Prerequisites - Docker, Docker Compose
2. Clone the repository
3. Deploy depended Kafka cluster and Databases using command `docker-compose up` from project root
4. Run the app using command `go build . && go run main.go` from the project root
5. GET Endpoints are exposed at http://localhost:3000/event-processor/api/v1/

## Sample Input and Output

Input events to message queue -

```json
{
  "Query": {
    "deal": "Deal",
    "user": "User"
  },
  "Deal": {
    "title": "String",
    "price": "Float",
    "user": "User"
  },
  "User": {
    "name": "String",
    "deals": "[Deal]"
  }
}
```

```json
{
  "deal": {
    "title": true,
    "price": true,
    "user": {
      "name": true
    }
  }
}
```

```json
{
  "deal": {
    "price": true
  },
  "user": {
    "id": true,
    "name": true
  }
}
```

1. Output - Total count of tables and columns from processed events

   GET - http://localhost:3000/event-processor/api/v1/count

Output -

   ```json
{
  "Query": {
    "deal": 2,
    "user": 1
  },
  "Deal": {
    "title": 1,
    "price": 2,
    "user": 1
  },
  "User": {
    "name": 2,
    "id": 1,
    "deals": 0
  }
}
   ```

2. Total count of tables and columns from processed events grouped by day

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

3. Total count of tables and columns from processed events grouped by client metadata.

GET - http://localhost:3000/event-processor/api/v1/countByMetadata?group_by_tag=client,client_version

Output -

   ```json
   [
  {
    "client": "client_id",
    "client_version": "v1",
    "deal": {
      "price": 149,
      "title": 143,
      "user": 143
    },
    "query": {
      "deal": 0,
      "user": 0
    },
    "user": {
      "deals": 0,
      "name": 73
    }
  }
]
   ```
   
