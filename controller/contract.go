package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"explorer/db"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func GetContract(c *gin.Context) {
	contract := c.Param("contract")
	if contract == "" {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
	req := esapi.GetRequest{
		Index:      "contract",
		DocumentID: contract,
	}
	res, err := req.Do(context.Background(), db.EsClient)
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

func GetContracts(c *gin.Context) {
	blockStr := c.DefaultQuery("block", "")
	defaultSize := 20
	sizeStr := c.DefaultQuery("size", "20")
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		size = defaultSize
	}
	defaultPage := 1
	pageStr := c.DefaultQuery("page", "1")
	page, err := strconv.Atoi(pageStr)
	if err != nil {
		page = defaultPage
	}
	from := (page - 1) * size
	body := map[string]interface{}{}
	if blockStr != "" {
		body = map[string]interface{}{
			"sort": [1]interface{}{
				map[string]interface{}{
					"timestamp": map[string]interface{}{
						"order": "desc",
					},
				},
			},
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"blockNumber": blockStr,
				},
			},
		}
	} else {
		body = map[string]interface{}{
			"sort": [1]interface{}{
				map[string]interface{}{
					"timestamp": map[string]interface{}{
						"order": "desc",
					},
				},
			},
		}
	}
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		panic(err)
	}
	req := esapi.SearchRequest{
		Index: []string{"contract"},
		Size:  &size,
		From:  &from,
		Body:  &buf,
	}

	res, err := req.Do(context.Background(), db.EsClient)
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
