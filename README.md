# 区块链浏览器服务端

## 预览地址

<https://explorer.fzcode.com/>

## 客户端项目地址

<https://github.com/757566833/explorer-client>

## 环境配置

1. elasticsearch数据库。本项目将区块链数据读取并同步到elasticsearch项目中

## 参数说明

1. ELASTICSEARCH_PATH:elasticsearch 的restful url 一般默认端口是9200
2. EXPLORER_SERVER_PORT: 本项目启动所占用的端口
3. CHAIN_HTTP_URL: 区块链的rpc url

## 代码简介

1. controller 控制层 (todo)
2. db 数据库的两个客户端 ，分别是eth 和 es
3. log 配置日志的代码
4. route restful所有的路由地址
5. sync 将区块链数据同步到es的代码
