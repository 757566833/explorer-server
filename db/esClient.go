package db

import (
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v7"
)

var EsClient *elasticsearch.Client

func InitEsClient() {

	ElasticsearchPath := os.Getenv("ELASTICSEARCH_PATH")
	ec, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{
		ElasticsearchPath,
	}})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	EsClient = ec
	res, err := EsClient.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}
	blockResponse, err := ec.Indices.Exists([]string{"block"})
	if err != nil {
		log.Fatalf("Error exists the block index: %s", err)
	}
	if blockResponse.StatusCode == 404 {
		var createIndexResponse, err = ec.Indices.Create("block")
		if err != nil {
			log.Fatalf("Error create the block index: %s", err)
		}
		if createIndexResponse.IsError() {
			log.Fatalf("Error create the block index: %s", err)
		}
	}
	defer blockResponse.Body.Close()
	txResponse, err := ec.Indices.Exists([]string{"tx"})
	if err != nil {
		log.Fatalf("Error exists the tx index: %s", err)
	}
	if txResponse.StatusCode == 404 {
		var createIndexResponse, err = ec.Indices.Create("tx")
		if err != nil {
			log.Fatalf("Error create the tx index: %s", err)
		}
		if createIndexResponse.IsError() {
			log.Fatalf("Error create the tx index: %s", err)
		}
	}
	defer txResponse.Body.Close()

	addressResponse, err := ec.Indices.Exists([]string{"address"})
	if err != nil {
		log.Fatalf("Error exists the tx index: %s", err)
	}
	if addressResponse.StatusCode == 404 {
		var createIndexResponse, err = ec.Indices.Create("address")
		if err != nil {
			log.Fatalf("Error create the tx index: %s", err)
		}
		if createIndexResponse.IsError() {
			log.Fatalf("Error create the tx index: %s", err)
		}
	}
	defer addressResponse.Body.Close()
}

type Shards struct {
	Total uint64 `json:"total"`
	// todo
	//Successful uint64 `json:"successful"`
	//skipped
	//"failed" : 0
}
type Hits1Total struct {
	Value    uint64 `json:"value"`
	Relation string `json:"relation"`
}
type Hits2 struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Id     string  `json:"_id"`
	Score  float32 `json:"_score"`
	Source string  `json:"_source"`
}
type Hits1 struct {
	Total Hits1Total `json:"total"`
	// todo
	MaxScore float32 `json:"max_score"`
	//Successful uint64 `json:"successful"`
	//skipped
	//"failed" : 0
	Hits []Hits2 `json:"hits"`
}
type EsSearchResponse struct {
	Took    uint64 `json:"took"`
	TimeOut bool   `json:"time_out"`
	Shards  Shards `json:"_shards"`
	Hits    Hits1  `json:"hits"`
}
