

## Question 1 – Understanding Docker images

Run Docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container:

```bash
docker run -it --rm --entrypoint bash python:3.13
```

Check `pip` version inside the container:

```bash
pip --version
```

---

## Question 2 – Understanding Docker networking and docker-compose

* In a Docker network, containers communicate using the service name, not the container name.
* In our `docker-compose.yaml`, the Postgres service name is `db`.
* For the port mapping `5433:5432` → `5433` is the host port(Windows), `5432` is the container port (Postgres listens here).


## Question 3 – Download data (Windows PowerShell)

```powershell
# Download trips Parquet file
Invoke-WebRequest `
  -Uri https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet `
  -OutFile green_tripdata_2025-11.parquet

# Download zones CSV file
Invoke-WebRequest `
  -Uri https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv `
  -OutFile taxi_zone_lookup.csv
```


## Question 4 – Set up Postgres + pgAdmin

* pgAdmin link: [http://localhost:8085](http://localhost:8085)
* Create a server in pgAdmin connecting to Postgres

### Copy local files into the Postgres container

```powershell
docker cp .\green_tripdata_2025-11.parquet postgres:/green_tripdata_2025-11.parquet
docker cp .\taxi_zone_lookup.csv postgres:/taxi_zone_lookup.csv
```

---

## Install Python dependencies

```bash
pip install pyarrow sqlalchemy psycopg2-binary pandas
```

---

## Create tables

```sql
-- Trips
CREATE TABLE green_trips (
    lpep_pickup_datetime TIMESTAMP,
    trip_distance FLOAT,
    pulocationid INT,
    dolocationid INT,
    total_amount FLOAT,
    tip_amount FLOAT
);

-- Zones
CREATE TABLE zones (
    locationid INT,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);
```

---

## Question 3 – Counting short trips

```sql
SELECT COUNT(*) AS short_trips
FROM green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

## Question 4 – Longest trip per day (<100 miles)

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_day,
       MAX(trip_distance) AS max_distance
FROM green_trips
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_distance DESC
LIMIT 1;
```


## Question 5 – Pickup zone with largest `total_amount` on Nov 18

```sql
SELECT z.zone, SUM(g.total_amount) AS total_amount
FROM green_trips g
JOIN zones z ON g.pulocationid = z.locationid
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.zone
ORDER BY total_amount DESC
LIMIT 1;
```

## Question 6 – Largest tip from "East Harlem North"

```sql
SELECT dz.zone AS dropoff_zone,
       SUM(g.tip_amount) AS total_tip
FROM green_trips g
JOIN zones pz ON g.pulocationid = pz.locationid
JOIN zones dz ON g.dolocationid = dz.locationid
WHERE pz.zone = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime < '2025-12-01'
GROUP BY dz.zone
ORDER BY total_tip DESC
LIMIT 1;
```


## Question 7 – Terraform workflow

Sequence describing the workflow:

```text
terraform init, terraform apply -auto-approve, terraform destroy
```
