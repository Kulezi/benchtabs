COMPOSE="docker-compose-3x4.yml"
N_NODES=3
N_SHARDS=4
NODE_MEM=750M
NAP=120

./gen_compose.sh $N_NODES $N_SHARDS $NODE_MEM > $COMPOSE

docker compose -f $COMPOSE down
docker volume prune -f


docker compose -f $COMPOSE up --detach
sleep $NAP
cd scylla-rust-driver
time -p taskset -c 12-15 cargo run --release . --tasks 10000000
cd ..
docker compose -f $COMPOSE down

docker compose -f $COMPOSE up --detach
cd scylla-go-driver
sleep $NAP
time -p taskset -c 12-15 go run . --tasks 1000000 -profile-mem
cd ..
docker compose -f $COMPOSE down
docker volume prune -f

docker compose -f $COMPOSE up --detach
sleep $NAP
cd gocql
time -p taskset -c 12-15 go run . --tasks 1000000 -profile-mem
cd ..
docker compose -f $COMPOSE down
docker volume prune -f
