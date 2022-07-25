#!/usr/bin/bash
SCYLLA_VERSION=5.0.0
SCYLLA_IMAGE=scylladb/scylla

if [[ $# -lt 3 ]] ; then
  echo 'usage ./gen_compose <n_nodes> <smp> <memory>'
  exit 1
fi


NODES=$1
SMP=$2
MEM=$3

if [[ $((NODES*SMP)) -gt $(nproc --all) ]] ; then
  echo 'want '$((NODES*SMP))' cores, nproc --all shows '$(nproc --all)
  exit 1
fi
SCYLLA_ARGS='--seeds=node1,node2 --smp '$SMP' --memory '$MEM' --authenticator PasswordAuthenticator'
echo 'version: "3.7"

services:'

for i in $(seq 1 $NODES) ; do
  echo '  node'$i':'
  if [[ $SMP -eq 1 ]] ; then 
    echo '    cpuset: "'$((i-1))'"'
  else
    echo '    cpuset: "'$(((i-1)*SMP))-$((i*SMP-1))'"'
  fi
    echo '    image: '${SCYLLA_IMAGE}:${SCYLLA_VERSION}'
    command: '${SCYLLA_ARGS}'
    ports:
      - "'$((9041+i))':9042"
      - "'$((19041+i))':19042"
    networks:
      public:
        ipv4_address: 192.168.100.'$((99+i))
done
echo '
networks:
  public:
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 192.168.100.0/24'
