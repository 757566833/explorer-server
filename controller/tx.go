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

func GetTx(c *gin.Context) {
	tx := c.Param("tx")
	if tx == "" {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
	req := esapi.GetRequest{
		Index:      "tx",
		DocumentID: tx,
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

// GetTxs 获取所有的tx 如果指定block则获取所有block的tx
func GetTxs(c *gin.Context) {
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
					"number": blockStr,
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
		Index: []string{"tx"},
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

func GetTxByAddress(c *gin.Context) {
	address := c.Param("address")
	if address == "" {
		c.IndentedJSON(http.StatusBadRequest, "")
	}
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
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": [2]interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"to": map[string]interface{}{
								"value": address,
							},
						},
					},
					map[string]interface{}{
						"term": map[string]interface{}{
							"from": map[string]interface{}{
								"value": address,
							},
						},
					},
				},
			},
		},
		"sort": [1]interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		panic(err)
	}
	req := esapi.SearchRequest{
		Index: []string{"tx"},
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
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		panic(err)
	}
	// byt, err := io.ReadAll(res.Body)
	// str := string(byt)
	// if err != nil {
	// 	panic(err)
	// }
	// if res.StatusCode > 300 {
	// 	c.IndentedJSON(res.StatusCode, str)
	// }
	// fmt.Println(str)
	// response := []byte(``)
	// err = json.Unmarshal(response, &str)
	// if err != nil {
	// 	panic(err)
	// }
	c.IndentedJSON(res.StatusCode, response)
}

func GetContracts(c *gin.Context) {
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
	body := map[string]interface{}{
		"sort": [1]interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": [1]interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"contractAddress.keyword": map[string]interface{}{
								"value": "",
							},
						},
					},
				},
			},
		},
	}
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		panic(err)
	}
	req := esapi.SearchRequest{
		Index: []string{"tx"},
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
func GetContractTxs(c *gin.Context) {
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
	body := map[string]interface{}{
		"sort": [1]interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": [1]interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"logLength": map[string]interface{}{
								"gt": 0,
							},
						},
					},
				},
			},
		},
	}
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		panic(err)
	}
	req := esapi.SearchRequest{
		Index: []string{"tx"},
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

//
//// GetBalanceTxs 获取交易的tx 如果指定block则获取所有block的tx
//func GetBalanceTxs(c *gin.Context) {
//	blockStr := c.DefaultQuery("block", "")
//	defaultSize := 20
//	sizeStr := c.DefaultQuery("size", "20")
//	size, err := strconv.Atoi(sizeStr)
//	if err != nil {
//		size = defaultSize
//	}
//	defaultPage := 1
//	pageStr := c.DefaultQuery("page", "1")
//	page, err := strconv.Atoi(pageStr)
//	if err != nil {
//		page = defaultPage
//	}
//	from := (page - 1) * size
//	body := map[string]interface{}{}
//	if blockStr != "" {
//		body = map[string]interface{}{
//			"sort": [1]interface{}{
//				map[string]interface{}{
//					"timestamp": map[string]interface{}{
//						"order": "desc",
//					},
//				},
//			},
//			"query": map[string]interface{}{
//				"match": map[string]interface{}{
//					"number": blockStr,
//				},
//			},
//		}
//	} else {
//		body = map[string]interface{}{
//			"sort": [1]interface{}{
//				map[string]interface{}{
//					"timestamp": map[string]interface{}{
//						"order": "desc",
//					},
//				},
//			},
//			"query": map[string]interface{}{
//				"bool": map[string]interface{}{
//					"must": [1]interface{}{
//						map[string]interface{}{
//							"term": map[string]interface{}{
//								"contractAddress": map[string]interface{}{
//									"value": "0x0000000000000000000000000000000000000000",
//								},
//							},
//						},
//					},
//				},
//			},
//		}
//	}
//	var buf bytes.Buffer
//	err = json.NewEncoder(&buf).Encode(body)
//	if err != nil {
//		panic(err)
//	}
//	req := esapi.SearchRequest{
//		Index: []string{"tx"},
//		Size:  &size,
//		From:  &from,
//		Body:  &buf,
//	}
//
//	res, err := req.Do(context.Background(), db.EsClient)
//	if err != nil {
//		panic(err)
//	}
//	defer res.Body.Close()
//	var response any
//	err2 := json.NewDecoder(res.Body).Decode(&response)
//	if err2 != nil {
//		panic(err2)
//	}
//
//	c.IndentedJSON(res.StatusCode, response)
//}
//
//// GetContractDeploy 获取合同的tx
//func GetContractDeploy(c *gin.Context) {
//	defaultSize := 20
//	sizeStr := c.DefaultQuery("size", "20")
//	size, err := strconv.Atoi(sizeStr)
//	if err != nil {
//		size = defaultSize
//	}
//	defaultPage := 1
//	pageStr := c.DefaultQuery("page", "1")
//	page, err := strconv.Atoi(pageStr)
//	if err != nil {
//		page = defaultPage
//	}
//	from := (page - 1) * size
//	body := map[string]interface{}{
//		"sort": [1]interface{}{
//			map[string]interface{}{
//				"timestamp": map[string]interface{}{
//					"order": "desc",
//				},
//			},
//		},
//		"query": map[string]interface{}{
//			"bool": map[string]interface{}{
//				"must_not": [1]interface{}{
//					map[string]interface{}{
//						"term": map[string]interface{}{
//							"contractAddress": map[string]interface{}{
//								"value": "0x0000000000000000000000000000000000000000",
//							},
//						},
//					},
//				},
//			},
//		},
//	}
//	var buf bytes.Buffer
//	err = json.NewEncoder(&buf).Encode(body)
//	if err != nil {
//		panic(err)
//	}
//	req := esapi.SearchRequest{
//		Index: []string{"tx"},
//		Size:  &size,
//		From:  &from,
//		Body:  &buf,
//	}
//	res, err := req.Do(context.Background(), db.EsClient)
//	if err != nil {
//		panic(err)
//	}
//	defer res.Body.Close()
//	var response any
//	err2 := json.NewDecoder(res.Body).Decode(&response)
//	if err2 != nil {
//		panic(err2)
//	}
//
//	c.IndentedJSON(res.StatusCode, response)
//
//}
