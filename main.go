package main

import (
	"explorer/db"
	"explorer/log"
	"explorer/route"
	"os"
)

func main() {
	db.InitEsClient()
	db.InitEthClient()
	log.InitLogger()
	ExplorerServerPort := os.Getenv("EXPLORER_SERVER_PORT")
	router := route.InitRouter()
	//go sync.Sync()
	router.Run("0.0.0.0:" + ExplorerServerPort)
}
