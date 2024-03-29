### Using Postgrescli

docker run -it \
   -e POSTGRES_USER="root" \
   -e POSTGRES_PASSWORD="root" \
   -e POSTGRES_DB="ny_taxi" \
   -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
   -p 5432:5432 \
   postgres:13

pgcli -h 172.18.0.3 -p 5432 -u root -d ny_taxi
### Enabling Interaction with Postgres DB 
docker exec -it e66de0f0aa8c psql -U root -d ny_taxi

## Running PGAdmin
docker run -it \
   -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
   -e PGADMIN_DEFAULT_PASSWORD="root" \
   -p 8080:80 \
   dpage/pgadmin4

## Creating a docker network to enable container joining
docker network create pg-network

docker run -it \
   -e POSTGRES_USER="root" \
   -e POSTGRES_PASSWORD="root" \
   -e POSTGRES_DB="ny_taxi" \
   -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
   -p 5432:5432 \
   --network=pg-network \
   --name pg-database \
   postgres:13

docker run -it \
   -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
   -e PGADMIN_DEFAULT_PASSWORD="root" \
   -p 8080:80 \
   --network=pg-network \
   --name pgadmin \
   dpage/pgadmin4

## Convert notbook to python script
jupyter nbconvert --to=script upload-data.ipynb

python injest-data.py
   --user=root \
   --password=root \
   --host=localhost \
   --port=5432 \
   --db=ny_taxi \
   --table_name=yellow_taxi_data

### Ip address verification for a container
docker inspect <container_id>

### Enabling viscibility of containers at ingestion
docker run -it --network 2_docker_sql_default my_image:01  --user root --password root --host pgdatabase --port 5432 --db ny_taxi --table_name yellow_taxi_trips