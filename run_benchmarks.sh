COMPOSE="docker-compose-3x4.yml"
./gen_compose.sh > $COMPOSE

docker compose -f $COMPOSE down
docker volume prune -f


docker compose -f $COMPOSE up --detach
sleep 120
cd scylla-rust-driver
time -p taskset -c 12-15 cargo run --release . --tasks 10000000
cd ..
docker compose -f $COMPOSE down

docker compose -f $COMPOSE up --detach
cd scylla-go-driver
sleep 120
time -p taskset -c 12-15 go run . --tasks 10000000
cd ..
docker compose -f $COMPOSE down
docker volume prune -f

docker compose -f $COMPOSE up --detach
sleep 120
cd gocql
time -p taskset -c 12-15 go run . --tasks 10000000
cd ..
docker compose -f $COMPOSE down
docker volume prune -f
