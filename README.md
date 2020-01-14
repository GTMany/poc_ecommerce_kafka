# POC ECOMMERCE KAFKA

Este projeto é uma POC utilizando o apache kafka em um cenário de ecommece.


## Tópicos

Para visualizar os tópicos via **linha de comando**:

**ECOMMERCE_NEW_ORDER:**
```sh
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ECOMMERCE_NEW_ORDER --from-beginning
```