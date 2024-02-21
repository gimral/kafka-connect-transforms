docker-compose -f docker-compose-bidirectional.yaml up -d

docker-compose -f docker-compose-bidirectional.yaml exec kafka-ocp sh -c '
kafka-topics.sh --create --topic rep1 --partitions 2 --replication-factor 1 --bootstrap-server kafka-ocp:9092; 
kafka-topics.sh --create --topic rep2 --partitions 2 --replication-factor 1 --bootstrap-server kafka-ocp:9092; 
kafka-topics.sh --create --topic norep1 --partitions 2 --replication-factor 1 --bootstrap-server kafka-ocp:9092'

docker-compose -f docker-compose-bidirectional.yaml exec kafka-cld sh -c '
  kafka-topics.sh --create --topic rep1 --partitions 2 --replication-factor 1 --bootstrap-server kafka-cld:9094;
  kafka-topics.sh --create --topic rep2 --partitions 2 --replication-factor 1 --bootstrap-server kafka-cld:9094;
  kafka-topics.sh --create --topic norep2 --partitions 2 --replication-factor 1 --bootstrap-server kafka-cld:9094'