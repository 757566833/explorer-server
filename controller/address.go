package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"explorer/db"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

func GetAddressDetail(c *gin.Context) {
	address := c.Param("address")
	if address == "" {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
	blockReq := esapi.GetRequest{
		Index:      "address",
		DocumentID: address,
	}
	res, err := blockReq.Do(context.Background(), db.EsClient)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	var response any
	err2 := json.NewDecoder(res.Body).Decode(&response)
	if err2 != nil {
		panic(err2)
	}
	c.IndentedJSON(res.StatusCode, response)
}
func GetAddressesDetail(c *gin.Context) {
	addresses := c.DefaultQuery("addresses", "")
	if addresses == "" {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
	list := strings.Split(addresses, `,`)
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"ids": map[string]interface{}{
				"values": list,
			},
		},
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
	blockReq := esapi.SearchRequest{
		Index: []string{"address"},
		Body:  &buf,
	}
	res, err := blockReq.Do(context.Background(), db.EsClient)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	var response any
	err2 := json.NewDecoder(res.Body).Decode(&response)
	if err2 != nil {
		panic(err2)
	}
	c.IndentedJSON(res.StatusCode, response)
}
