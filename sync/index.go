package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"explorer/db"
	"explorer/log"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"io"
	"math/big"
	"os"
	"strings"
	"time"
)

type ESBlock struct {
	ParentHash  common.Hash      `json:"parentHash"       gencodec:"required"`
	UncleHash   common.Hash      `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    common.Address   `json:"miner"`
	Root        common.Hash      `json:"stateRoot"        gencodec:"required"`
	TxHash      common.Hash      `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash common.Hash      `json:"receiptsRoot"     gencodec:"required"`
	Bloom       types.Bloom      `json:"logsBloom"        gencodec:"required"`
	Difficulty  string           `json:"difficulty"       gencodec:"required"`
	Number      string           `json:"number"           gencodec:"required"`
	GasLimit    string           `json:"gasLimit"         gencodec:"required"`
	GasUsed     string           `json:"gasUsed"          gencodec:"required"`
	Time        uint64           `json:"timestamp"        gencodec:"required"`
	Extra       []byte           `json:"extraData"        gencodec:"required"`
	MixDigest   common.Hash      `json:"mixHash"`
	Nonce       types.BlockNonce `json:"nonce"`

	// BaseFee was added by EIP-1559 and is ignored in legacy headers.
	BaseFee string `json:"baseFeePerGas" rlp:"optional"`

	Txns      int         `json:"txns"                  gencodec:"required"`
	BlockHash common.Hash `json:"blockHash"             gencodec:"required"`
	Size      string      `json:"size"                  gencodec:"required"`
	BurntFees string      `json:"burntFees"                 `
}

type ESTx struct {
	Type       byte             `json:"type"                        gencodec:"required"`
	Nonce      string           `json:"nonce"`
	GasPrice   string           `json:"gasPrice"                    gencodec:"required"`
	GasTipCap  string           `json:"maxPriorityFeePerGas"        gencodec:"required"`
	GasFeeCap  string           `json:"maxFeePerGas"                gencodec:"required"`
	Gas        string           `json:"gasLimit"                    gencodec:"required"`
	Value      string           `json:"value"                       gencodec:"required"`
	Data       []byte           `json:"input"                       gencodec:"required"`
	Number     string           `json:"number"                      gencodec:"required"`
	V          string           `json:"v"                           gencodec:"required"`
	R          string           `json:"r"                           gencodec:"required"`
	S          string           `json:"s"                           gencodec:"required"`
	To         *common.Address  `json:"to"                          gencodec:"required"`
	Hash       common.Hash      `json:"hash"                        gencodec:"required"`
	Time       uint64           `json:"timestamp"                   gencodec:"required"`
	From       common.Address   `json:"from"                        gencodec:"required"`
	AccessList types.AccessList `json:"accessList"                  gencodec:"required"`
	IsFake     bool             `json:"isFake"                      gencodec:"required"`
	BaseFee    string           `json:"baseFeePerGas" rlp:"optional"`

	// receipt
	ReceiptType       uint8        `json:"receiptType"`
	PostState         []byte       `json:"postState"`
	Status            string       `json:"status"`
	CumulativeGasUsed string       `json:"cumulativeGasUsed"       gencodec:"required"`
	Bloom             types.Bloom  `json:"logsBloom"               gencodec:"required"`
	Logs              []*types.Log `json:"logs"                    gencodec:"required"`
	LogLength         uint64       `json:"logLength"               gencodec:"required"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          common.Hash     `json:"transactionHash"         gencodec:"required"`
	ContractAddress *common.Address `json:"contractAddress"`
	GasUsed         string          `json:"gasUsed"                 gencodec:"required"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        common.Hash `json:"blockHash"`
	BlockNumber      string      `json:"blockNumber"`
	TransactionIndex uint        `json:"transactionIndex"`
	TransactionFee   string      `json:"transactionFee"`
	// 1559
	BurntFees    string `json:"burntFees"`
	TxSavingsFee string `json:"txSavingsFee"`
}

type ESAddress struct {
	address string `json:"address"                        gencodec:"required"`
	Type    uint8  `json:"type"  gencodec:"required"`
}
type ESBlockHit1 struct {
	Source ESBlock `json:"_source"                       gencodec:"required"`
	Index  string  `json:"_index"                        gencodec:"required"`
	Type   string  `json:"_type"                         gencodec:"required"`
	Id     string  `json:"_id"                           gencodec:"required"`
	Score  string  `json:"_score"                        gencodec:"required"`
}
type ESTotal struct {
	Value    int64  `json:"value"                        gencodec:"required"`
	Relation string `json:"relation"                        gencodec:"required"`
}
type ESBlockHit2 struct {
	Hits  []ESBlockHit1 `json:"hits"                       gencodec:"required"`
	Total ESTotal       `json:"total"                       gencodec:"required"`
}
type ESShards struct {
	Total      int64 `json:"total"                       gencodec:"required"`
	Successful int64 `json:"successful"                       gencodec:"required"`
	Skipped    int64 `json:"skipped"                       gencodec:"required"`
	Failed     int64 `json:"failed"                       gencodec:"required"`
}
type ESBlockRes struct {
	Took     int64       `json:"took"                       gencodec:"required"`
	TimedOut bool        `json:"timed_out"                       gencodec:"required"`
	Shards   ESShards    `json:"_shards"                       gencodec:"required"`
	Hits     ESBlockHit2 `json:"hits"                       gencodec:"required"`
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
	esBlock.ParentHash = header.ParentHash
	esBlock.UncleHash = header.UncleHash
	esBlock.Coinbase = header.Coinbase
	esBlock.Root = header.Root
	esBlock.TxHash = header.TxHash
	esBlock.ReceiptHash = header.ReceiptHash
	esBlock.Bloom = header.Bloom
	esBlock.Difficulty = header.Difficulty.String()
	esBlock.Number = header.Number.String()
	esBlock.GasLimit = new(big.Int).SetUint64(header.GasLimit).String()
	esBlock.GasUsed = new(big.Int).SetUint64(header.GasUsed).String()
	esBlock.Time = header.Time
	esBlock.Extra = header.Extra
	esBlock.MixDigest = header.MixDigest
	esBlock.Nonce = header.Nonce
	esBlock.BaseFee = header.BaseFee.String()
	esBlock.Txns = txLength
	esBlock.BlockHash = block.Hash()
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
	esTx.GasPrice = tx.GasPrice().String()
	esTx.GasTipCap = tx.GasTipCap().String()
	esTx.GasFeeCap = tx.GasFeeCap().String()
	esTx.Gas = new(big.Int).SetUint64(tx.Gas()).String()
	esTx.Value = tx.Value().String()
	esTx.Data = tx.Data()
	esTx.Number = header.Number.String()
	esTx.To = tx.To()
	esTx.Hash = tx.Hash()

	v, r, s := tx.RawSignatureValues()
	esTx.V = v.String()
	esTx.R = r.String()
	esTx.S = s.String()
	esTx.Time = header.Time
	esTx.BaseFee = header.BaseFee.String()
	msg, err := tx.AsMessage(types.LatestSignerForChainID(tx.ChainId()), tx.GasPrice())
	if err != nil {
		return nil, err
	}

	esTx.IsFake = msg.IsFake()
	esTx.AccessList = msg.AccessList()
	esTx.From = msg.From()
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
	esTx.TxHash = receipt.TxHash

	esTx.GasUsed = new(big.Int).SetUint64(receipt.GasUsed).String()
	esTx.BlockHash = receipt.BlockHash
	esTx.BlockNumber = receipt.BlockNumber.String()
	esTx.TransactionIndex = receipt.TransactionIndex
	// 1559
	if header.BaseFee != nil {
		// 交易费
		transactionFee := new(big.Int)
		transactionFee.Add(header.BaseFee, tx.GasTipCap())
		transactionFee.Mul(transactionFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.TransactionFee = transactionFee.String()
		// Savings Fees
		txSavingsFee := new(big.Int)
		txSavingsFee.Sub(tx.GasFeeCap(), tx.GasTipCap())
		txSavingsFee.Sub(txSavingsFee, header.BaseFee)
		txSavingsFee.Mul(transactionFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.TxSavingsFee = txSavingsFee.String()
		burntFees := new(big.Int)
		burntFees.Mul(header.BaseFee, new(big.Int).SetUint64(tx.Gas()))
		esTx.BurntFees = burntFees.String()
	} else {
		transactionFee := new(big.Int)
		transactionFee.Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()))
		esTx.TransactionFee = transactionFee.String()
	}
	if receipt.ContractAddress.String() != emptyContractAddress {
		esTx.ContractAddress = &receipt.ContractAddress
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
			addressArray = append(addressArray, esTx.From.String())
			addressArray = append(addressArray, esTx.To.String())
			if esTx.ContractAddress != nil && esTx.ContractAddress.String() != emptyContractAddress {
				contractArray = append(contractArray, esTx.ContractAddress.String())
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
