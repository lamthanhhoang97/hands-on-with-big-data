docker compose -f docker-compose.yaml exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9093 \
    --from-beginning \
    --property print.key=true \
    --topic tpch.public.orders