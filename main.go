package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func main() {
	configDbzium := map[string]interface{}{
		"name": "products-connector",
		"config": map[string]string{
			"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
			"tasks.max":                      "1",
			"database.hostname":              "postgres",
			"database.port":                  "5432",
			"database.user":                  "admin",
			"database.password":              "admin-123",
			"database.dbname":                "Produtos",
			"database.server.name":           "postgres",
			"slot.name":                      "product_slot",
			"key.converter":                  "org.apache.kafka.connect.storage.StringConverter",
			"key.converter.schemas.enable":   "false",
			"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "false",
			"include.schema.changes":         "false",
		},
	}

	debeziumUrl := "http://localhost:8083/connectors/"

	data, err := json.Marshal(configDbzium)

	if err != nil {
		log.Fatal("DEU RUIM")
	}

	body := bytes.NewBuffer(data)

	connectorPost, err := http.Post(debeziumUrl, "application/json", body)

	if err != nil {
		log.Fatal("DEU RUIM")
	}


	defer connectorPost.Body.Close()


	fmt.Printf("Status: %d, Message: %s", connectorPost.StatusCode, body)

	var dataResp []byte

	if err = json.Unmarshal(body.Bytes(), &dataResp); err != nil {
		fmt.Println("Deu Ruim")
	}


	fmt.Println(string(dataResp))



	if connectorPost.StatusCode == 201 {
		fmt.Println("OK")
	}

	fmt.Println("Teste...!")
}
