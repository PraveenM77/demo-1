version: "3.8"

services:

  # Django application
  django:
    build: .
    container_name: django_app
    volumes:
      - .:/app
    environment:
      - POSTGRES_HOST=db
      - POSTGRES_PORT=5432
      - POSTGRES_DB=testdb
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=1234
      - KAFKA_BROKER=kafka:9092  # Kafka Broker Address
      - REDIS_HOST=redis
      - REDIS_PORT=6379
    depends_on:
      - db
      - elasticsearch
      - kafka
      - redis
    ports:
      - "8000:8000"
    networks:
      - mynetwork
    restart: unless-stopped
    entrypoint: ["./wait-for-it.sh", "elasticsearch:9200", "--timeout=600", "--", "bash", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8000"]
  # Web scraper service
  web:
    build: .
    container_name: python_scraper
    volumes:
      - .:/app  # Mounts the current directory to the container
    environment:
      - POSTGRES_HOST=db
      - POSTGRES_PORT=5432
      - POSTGRES_DB=testdb
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=1234
    depends_on:
      - db
      - elasticsearch  # Ensures the database and Elasticsearch are ready before starting the scraper
    entrypoint: ["./wait-for-it.sh", "elasticsearch:9200", "--", "sh", "-c", "python SkinProductA/webscrapflipkart.py && python SkinProductA/fromWebToDatabase.py && python SkinProductA/pushToElasticsearch.py && python SkinProductA/customer_details.py"]
    networks:
      - mynetwork
    

  # PostgreSQL service
  db:
    image: postgres:13
    container_name: postgres_db
    environment:
      - POSTGRES_DB=testdb
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=1234
    volumes:
      - pg_data:/var/lib/postgresql/data  # Persist data between container restarts
    networks:
      - mynetwork
    # Restart policy in case the DB fails
    restart: unless-stopped

  # Elasticsearch service
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:7.10.0
    container_name: elasticsearch
    environment:
      - discovery.type=single-node
      - ES_JAVA_OPTS=-Djava.security.egd=file:/dev/./urandom -Xmx2g -Xms2g -Des.ignore_system_memory=true
    ports:
      - "9200:9200"
    volumes:
      - es_data:/usr/share/elasticsearch/data
    networks:
      - mynetwork
    # Restart policy for Elasticsearch
    restart: unless-stopped

  kafka:
    image: bitnami/kafka:latest
    container_name: kafka_broker
    environment:
      - KAFKA_ENABLE_KRAFT=yes  # Enables KRaft mode (Kafka without Zookeeper)
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - ALLOW_PLAINTEXT_LISTENER=yes
    ports:
      - "9092:9092"
    networks:
      - mynetwork

  # Redis Service
  redis:
    image: redis:latest
    container_name: redis_cache
    ports:
      - "6379:6379"
    networks:
      - mynetwork
    restart: unless-stopped

# Define persistent volumes for PostgreSQL and Elasticsearch data
volumes:
  pg_data:
    driver: local

  es_data:
    driver: local

# Define the network to connect the services
networks:
  mynetwork:
    driver: bridge
