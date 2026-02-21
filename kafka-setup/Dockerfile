FROM confluentinc/cp-kafka:7.5.0

RUN echo '#!/bin/bash' > /create-topics.sh && \
    echo 'set -e' >> /create-topics.sh && \
    echo 'echo "Waiting for Kafka to be ready..."' >> /create-topics.sh && \
    echo 'cub kafka-ready -b "$KAFKA_BOOTSTRAP_SERVERS" 1 60' >> /create-topics.sh && \
    echo 'echo "Creating Kafka topics..."' >> /create-topics.sh && \
    echo 'kafka-topics --create --if-not-exists --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 3 --topic customer-application' >> /create-topics.sh && \
    echo 'kafka-topics --create --if-not-exists --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 3 --topic treatment-required' >> /create-topics.sh && \
    echo 'kafka-topics --create --if-not-exists --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 3 --topic treatment-results' >> /create-topics.sh && \
    echo 'kafka-topics --create --if-not-exists --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --replication-factor 1 --partitions 3 --topic customer-application-response' >> /create-topics.sh && \
    echo 'echo "Topics created successfully:"' >> /create-topics.sh && \
    echo 'kafka-topics --list --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS"' >> /create-topics.sh && \
    chmod +x /create-topics.sh

CMD ["/create-topics.sh"]
