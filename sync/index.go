package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"explorer/db"
	"explorer/log"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/ethereum/go-ethereum/core/types"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
)

type ESBlock struct {
	ParentHash  string      `json:"parentHash"`
	UncleHash   string      `json:"sha3Uncles"`
	Coinbase    string      `json:"miner"`
	Root        string      `json:"stateRoot"`
	TxHash      string      `json:"transactionsRoot"`
	ReceiptHash string      `json:"receiptsRoot"`
	Bloom       types.Bloom `json:"logsBloom"`
	Difficulty  string      `json:"difficulty"`
	Number      string      `json:"number"`
	GasLimit    string      `json:"gasLimit"`
	GasUsed     string      `json:"gasUsed"`
	Time        uint64      `json:"timestamp"`
	Extra       []byte      `json:"extraData"`
	MixDigest   string      `json:"mixHash"`
	Nonce       uint64      `json:"nonce"`

	// BaseFee was added by EIP-1559 and is ignored in legacy headers.
	BaseFee string `json:"baseFeePerGas" rlp:"optional"`

	Txns      int    `json:"txns"`
	BlockHash string `json:"blockHash"`
	Size      string `json:"size"`
	BurntFees string `json:"burntFees"`
}

type ESTx struct {
	Type       byte             `json:"type"`
	Nonce      string           `json:"nonce"`
	GasPrice   string           `json:"gasPrice"`
	GasTipCap  string           `json:"maxPriorityFeePerGas"`
	GasFeeCap  string           `json:"maxFeePerGas"`
	Gas        string           `json:"gasLimit"`
	Value      string           `json:"value"`
	Data       []byte           `json:"input"`
	Number     string           `json:"number"`
	V          string           `json:"v"`
	R          string           `json:"r"`
	S          string           `json:"s"`
	To         string           `json:"to"`
	Hash       string           `json:"hash"`
	Time       uint64           `json:"timestamp"`
	From       string           `json:"from"`
	AccessList types.AccessList `json:"accessList"`
	IsFake     bool             `json:"isFake"`
	BaseFee    string           `json:"baseFeePerGas" rlp:"optional"`

	// receipt
	ReceiptType       uint8        `json:"receiptType"`
	PostState         []byte       `json:"postState"`
	Status            string       `json:"status"`
	CumulativeGasUsed string       `json:"cumulativeGasUsed"`
	Bloom             types.Bloom  `json:"logsBloom"`
	Logs              []*types.Log `json:"logs"`
	LogLength         uint64       `json:"logLength"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          string `json:"transactionHash"`
	ContractAddress string `json:"contractAddress"`
	GasUsed         string `json:"gasUsed"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        string `json:"blockHash"`
	BlockNumber      string `json:"blockNumber"`
	TransactionIndex uint   `json:"transactionIndex"`
	TransactionFee   string `json:"transactionFee"`
	// 1559
	BurntFees    string `json:"burntFees"`
	TxSavingsFee string `json:"txSavingsFee"`
}

type ESAddress struct {
	address string `json:"address"`
	Type    uint8  `json:"type"`
}
type ESBlockHit1 struct {
	Source ESBlock `json:"_source"`
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Id     string  `json:"_id"`
	Score  string  `json:"_score"`
}
type ESTotal struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}
type ESBlockHit2 struct {
	Hits  []ESBlockHit1 `json:"hits"`
	Total ESTotal       `json:"total"`
}
type ESShards struct {
	Total      int64 `json:"total"`
	Successful int64 `json:"successful"`
	Skipped    int64 `json:"skipped"`
	Failed     int64 `json:"failed"`
}
type ESBlockRes struct {
	Took     int64       `json:"took"`
	TimedOut bool        `json:"timed_out"`
	Shards   ESShards    `json:"_shards"`
	Hits     ESBlockHit2 `json:"hits"`
}

var emptyContractAddress = "0x0000000000000000000000000000000000000000"

var searchBlock = []string{"block"}

func getEsLastBlockNumber() (string, error) {
	if db.EsClient != nil {
		var startStr = "0"
		size := 1
		from := 0
		_body := `{
		"sort": [
		   {
			 "timestamp":{
			   "order": "desc"
			 }
		   }
		 ]
	   }`
		blockReq := esapi.SearchRequest{
			Index: searchBlock,
			Size:  &size,
			From:  &from,
			Body:  strings.NewReader(_body),
		}
		esBlockResponse, err := blockReq.Do(context.Background(), db.EsClient)
		if err != nil {
			startStr = "0"
		}
		defer esBlockResponse.Body.Close()
		if esBlockResponse.StatusCode >= 300 {
			startStr = "0"
		} else {
			byt, err := io.ReadAll(esBlockResponse.Body)

			if err != nil {
				return "", errors.New("查询block失败")
			}
			var esBlockRes ESBlockRes
			err = json.Unmarshal(byt, &esBlockRes)
			if err != nil {
				startStr = "0"
			} else {
				startStr = esBlockRes.Hits.Hits[0].Source.Number
			}

		}
		return startStr, nil
	} else {
		return "", errors.New("es数据库未连接")
	}

}
func getRpcLastBlockNumber() (*big.Int, error) {
	if db.EthClient != nil {
		num, err := db.EthClient.BlockNumber(context.Background())
		length := new(big.Int).SetUint64(num)
		return length, err
	} else {
		return nil, errors.New("rpc未连接")
	}
}
func buildEsBlock(block *types.Block) *ESBlock {
	header := block.Header()
	body := block.Body()
	txLength := len(body.Transactions)
	esBlock := new(ESBlock)
	esBlock.ParentHash = header.ParentHash.String()
	esBlock.UncleHash = header.UncleHash.String()
	esBlock.Coinbase = header.Coinbase.String()
	esBlock.Root = header.Root.String()
	esBlock.TxHash = header.TxHash.String()
	esBlock.ReceiptHash = header.ReceiptHash.String()
	esBlock.Bloom = header.Bloom
	if header.Difficulty != nil {
		esBlock.Difficulty = header.Difficulty.String()
	}
	if header.Number != nil {
		esBlock.Number = header.Number.String()
	}

	esBlock.GasLimit = new(big.Int).SetUint64(header.GasLimit).String()
	esBlock.GasUsed = new(big.Int).SetUint64(header.GasUsed).String()
	esBlock.Time = header.Time
	esBlock.Extra = header.Extra
	esBlock.MixDigest = header.MixDigest.String()
	esBlock.Nonce = header.Nonce.Uint64()
	if header.BaseFee != nil {
		esBlock.BaseFee = header.BaseFee.String()
	}

	esBlock.Txns = txLength
	esBlock.BlockHash = block.Hash().String()
	esBlock.Size = block.Size().String()
	//1559
	if header.BaseFee != nil {
		burntFees := new(big.Int)
		burntFees.Mul(header.BaseFee, new(big.Int).SetUint64(header.GasUsed))
		esBlock.BurntFees = burntFees.String()
	} else {
		// todo
	}
	return esBlock
}
func createEsBlock(block *ESBlock) error {
	blockBuf, err := json.Marshal(block)
	if err != nil {
		return err
	}

	blockReq := esapi.IndexRequest{
		Index:      "block",
		DocumentID: block.Number,
		Body:       bytes.NewReader(blockBuf),
	}

	blockRes, blockErr := blockReq.Do(context.Background(), db.EsClient)
	if blockErr != nil {
		return err
	}
	defer blockRes.Body.Close()
	if blockRes.StatusCode >= 300 {
		return errors.New("http:es写入block出错")
	}
	return nil
}
func buildTx(tx *types.Transaction, header *types.Header) (*ESTx, error) {
	esTx := new(ESTx)
	esTx.Type = tx.Type()

	esTx.Nonce = new(big.Int).SetUint64(tx.Nonce()).String()
	gasPrice := tx.GasPrice()
	if gasPrice != nil {
		esTx.GasPrice = gasPrice.String()
	}

	gasTipCap := tx.GasTipCap()
	if gasTipCap != nil {
		esTx.GasTipCap = gasTipCap.String()
	}
	gasFeeCap := tx.GasFeeCap()
	if gasFeeCap != nil {
		esTx.GasFeeCap = gasFeeCap.String()
	}
	esTx.Gas = new(big.Int).SetUint64(tx.Gas()).String()
	value := tx.Value()
	if value != nil {
		esTx.Value = value.String()
	}

	esTx.Data = tx.Data()
	number := header.Number
	if number != nil {
		esTx.Number = header.Number.String()
	}

	to := tx.To()
	if to != nil {
		esTx.To = to.String()
	}

	esTx.Hash = tx.Hash().String()

	v, r, s := tx.RawSignatureValues()
	if v != nil {
		esTx.V = v.String()
	}
	if r != nil {
		esTx.R = r.String()
	}
	if s != nil {
		esTx.S = s.String()
	}

	esTx.Time = header.Time
	baseFee := header.BaseFee
	if baseFee != nil {
		esTx.BaseFee = baseFee.String()
	}
	// todo 为什么解析 需要用到chainId
	msg, err := tx.AsMessage(types.LatestSignerForChainID(tx.ChainId()), gasPrice)
	if err != nil {
		return nil, err
	}

	esTx.IsFake = msg.IsFake()
	esTx.AccessList = msg.AccessList()
	esTx.From = msg.From().String()
	receipt, err := db.EthClient.TransactionReceipt(context.Background(), tx.Hash())
	if err != nil {
		return nil, err
	}
	esTx.ReceiptType = receipt.Type
	esTx.PostState = receipt.PostState
	esTx.Status = new(big.Int).SetUint64(receipt.Status).String()
	esTx.CumulativeGasUsed = new(big.Int).SetUint64(receipt.CumulativeGasUsed).String()
	esTx.Bloom = receipt.Bloom
	esTx.Logs = receipt.Logs
	esTx.LogLength = uint64(len(receipt.Logs))
	esTx.TxHash = receipt.TxHash.String()

	esTx.GasUsed = new(big.Int).SetUint64(receipt.GasUsed).String()
	esTx.BlockHash = receipt.BlockHash.String()
	blockNumber := receipt.BlockNumber
	if blockNumber != nil {
		esTx.BlockNumber = blockNumber.String()
	}

	esTx.TransactionIndex = receipt.TransactionIndex
	// 1559
	if header.BaseFee != nil {
		// 交易费
		transactionFee := new(big.Int)
		transactionFee.Add(header.BaseFee, gasTipCap)
		transactionFee.Mul(transactionFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.TransactionFee = transactionFee.String()
		// Savings Fees
		txSavingsFee := new(big.Int)
		txSavingsFee.Sub(gasFeeCap, gasTipCap)
		txSavingsFee.Sub(txSavingsFee, header.BaseFee)
		txSavingsFee.Mul(transactionFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.TxSavingsFee = txSavingsFee.String()
		burntFees := new(big.Int)
		burntFees.Mul(header.BaseFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.BurntFees = burntFees.String()
	} else {
		transactionFee := new(big.Int)
		transactionFee.Mul(gasPrice, new(big.Int).SetUint64(tx.Gas()))
		esTx.TransactionFee = transactionFee.String()
	}
	log.Logger.Debug(`log length : ` + strconv.FormatInt(int64(len(receipt.Logs)), 10))
	log.Logger.Debug(`contract address : ` + receipt.ContractAddress.String())
	if receipt.ContractAddress.String() != emptyContractAddress {
		esTx.ContractAddress = receipt.ContractAddress.String()
	}
	return esTx, nil
}
func buildAddress(address string, _type uint8) *ESAddress {
	esAddress := new(ESAddress)
	esAddress.Type = _type
	esAddress.address = address

	return esAddress
}
func bulkBuildTx(block *types.Block) (*bytes.Buffer, []string, []string, error) {

	body := block.Body()
	header := block.Header()
	length := len(body.Transactions)
	var contractArray []string
	var addressArray []string
	if length > 0 {
		// 创建tx的body
		txBuf := new(bytes.Buffer)
		// 创建address的body
		//addressBuf := new(bytes.Buffer)
		for _, tx := range body.Transactions {
			createLine := map[string]interface{}{
				"create": map[string]interface{}{
					"_index": "tx",
					"_id":    tx.Hash(),
				},
			}
			createStr, err := json.Marshal(createLine)
			if err != nil {
				return nil, nil, nil, err
			}
			txBuf.Write(createStr)
			txBuf.WriteByte('\n')
			esTx, err := buildTx(tx, header)
			addressArray = append(addressArray, esTx.From)
			if esTx.To != "" {
				addressArray = append(addressArray, esTx.To)
			}

			if esTx.ContractAddress != "" && esTx.ContractAddress != emptyContractAddress {
				contractArray = append(contractArray, esTx.ContractAddress)
			}

			if err != nil {
				return nil, nil, nil, err
			}
			paramsStr, paramsErr := json.Marshal(esTx)
			if paramsErr != nil {
				return nil, nil, nil, err
			}
			txBuf.Write(paramsStr)
			txBuf.WriteByte('\n')
		}
		return txBuf, addressArray, contractArray, nil
	} else {

		return nil, addressArray, contractArray, nil
	}
}
func bulkBuildAddress(address map[string]bool, contract map[string]bool) (*bytes.Buffer, error) {
	addressBuf := new(bytes.Buffer)
	for _address, bool := range address {
		if bool {
			createLine := map[string]interface{}{
				"create": map[string]interface{}{
					"_index": "address",
					"_id":    _address,
				},
			}
			createStr, err := json.Marshal(createLine)
			if err != nil {
				return nil, err
			}

			addressBuf.Write(createStr)
			addressBuf.WriteByte('\n')
			esAddress := buildAddress(_address, 1)
			paramsStr, paramsErr := json.Marshal(esAddress)
			if paramsErr != nil {
				return nil, err
			}
			addressBuf.Write(paramsStr)
			addressBuf.WriteByte('\n')
		}
	}

	for _contract, bool := range contract {
		if bool {
			createLine := map[string]interface{}{
				"create": map[string]interface{}{
					"_index": "address",
					"_id":    _contract,
				},
			}
			createStr, err := json.Marshal(createLine)
			if err != nil {
				return nil, err
			}

			addressBuf.Write(createStr)
			addressBuf.WriteByte('\n')
			esAddress := buildAddress(_contract, 2)
			paramsStr, paramsErr := json.Marshal(esAddress)
			if paramsErr != nil {
				return nil, err
			}
			addressBuf.Write(paramsStr)
			addressBuf.WriteByte('\n')
		}
	}

	return addressBuf, nil

}

func bulkCreate(buf *bytes.Buffer) (string, error) {
	if buf != nil && buf.Len() > 0 {

		req := esapi.BulkRequest{
			Body: bytes.NewReader(buf.Bytes()),
		}
		res, err := req.Do(context.Background(), db.EsClient)

		if err != nil {
			return "", err
		}
		defer res.Body.Close()
		if res.StatusCode >= 300 {
			return "", errors.New("批量写入http返回报错")
		} else {
			return "", nil
		}
	}
	return "", nil
}
func getAddressListByList(list []string) (*db.EsSearchResponse, error) {
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
		return nil, err
	}
	listReq := esapi.SearchRequest{
		Index: []string{"address"},
		Body:  &buf,
	}
	res, err := listReq.Do(context.Background(), db.EsClient)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}

	var response db.EsSearchResponse
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return nil, err
	}
	return &response, nil

}

func Sync() {
	if db.EsClient != nil && db.EthClient != nil {
		// 获取 数据库的最后一个块
		startStr, err := getEsLastBlockNumber()
		if err != nil {
			log.Logger.Error(err.Error())
			os.Exit(1)
		}
		// 获取区块链的最后一个块
		length, err := getRpcLastBlockNumber()
		if err != nil {
			log.Logger.Error(err.Error())
			os.Exit(1)
		}
		startBg, b := new(big.Int).SetString(startStr, 10)
		if !b {

			startBg = big.NewInt(0)
		}

		// 如果数据库比区块链小，就开始更新
		for i := startBg; i.Cmp(length) == -1; i.Add(i, big.NewInt(1)) {
			log.Logger.Info(i.String())
			block, err := db.EthClient.BlockByNumber(context.Background(), i)
			if err != nil {
				log.Logger.Error("获取区块信息出错")
				log.Logger.Error(err.Error())
				os.Exit(1)
			}
			// 存储block
			esBlock := buildEsBlock(block)
			err = createEsBlock(esBlock)
			if err != nil {
				log.Logger.Error("es block 入库出错")
				log.Logger.Error(err.Error())
				os.Exit(1)
			}
			txBuf, address, contract, err := bulkBuildTx(block)

			if err != nil {
				log.Logger.Error("build tx出错")
				log.Logger.Error(err.Error())
				os.Exit(1)
			}

			if txBuf != nil {

				_, err = bulkCreate(txBuf)
				if err != nil {
					log.Logger.Error("批量创建tx出错")
					log.Logger.Error(err.Error())
					os.Exit(1)
				}
			}

			allAddress := append(address, contract...)
			res, err := getAddressListByList(allAddress)
			if err != nil {
				log.Logger.Error("获取地址列表出错")
				log.Logger.Error(err.Error())
				os.Exit(1)
			}
			addressMap := map[string]bool{}
			for _, _address := range address {
				addressMap[_address] = true
			}
			contractMap := map[string]bool{}
			for _, _contract := range contract {
				contractMap[_contract] = true
			}
			for _, hit := range res.Hits.Hits {
				if addressMap[hit.Id] {
					addressMap[hit.Id] = false
				}
				if contractMap[hit.Id] {
					contractMap[hit.Id] = false
				}
			}
			addressBuf, err := bulkBuildAddress(addressMap, contractMap)

			if err != nil {
				log.Logger.Error("构建入库地址列表出错")
				log.Logger.Error(err.Error())
				os.Exit(1)
			}

			if addressBuf != nil && addressBuf.Len() > 0 {
				_, err = bulkCreate(addressBuf)
				if err != nil {
					log.Logger.Error("入库地址列表出错")
					log.Logger.Error(err.Error())
					os.Exit(1)
				}
			}

		}
	}

	timer1 := time.NewTimer(time.Second * 5)
	<-timer1.C //阻塞，5秒以后继续执行
	Sync()
}
