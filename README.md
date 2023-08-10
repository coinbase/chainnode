<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Endpoint Group](#endpoint-group)
  - [Overriding the Configuration](#overriding-the-configuration)
- [Testing](#testing)
  - [Unit Test](#unit-test)
  - [Integration Test](#integration-test)
  - [Functional Test](#functional-test)
    - [Run Functional Test Locally](#run-functional-test-locally)
- [Local Development](#local-development)
  - [Running Server](#running-server)
  - [Manage workflow locally](#manage-workflow-locally)
- [Storage](#storage)
- [API](#api)
  - [DNS](#dns)
  - [API Documentation](#api-documentation)
  - [Eth namespace](#eth-namespace)
  - [Debug namespace](#debug-namespace)
  - [Net namespace](#net-namespace)
  - [Web3 namespace](#web3-namespace)
  - [Batch Request](#batch-request)
  - [Bor namespace (only for **Polygon**)](#bor-namespace-only-for-polygon)
  - [GraphQL](#graphql)
  - [Connect to Geth JavaScript Console](#connect-to-geth-javascript-console)
- [TAG Generator for Proxy Methods](#tag-generator-for-proxy-methods)
  - [TAG Usage:](#tag-usage)
  - [Changing TAG's generated code format:](#changing-tags-generated-code-format)
  - [Updating TAG's Unit Test:](#updating-tags-unit-test)
- [JSONRPC Server Batch Request Limit](#jsonrpc-server-batch-request-limit)
  - [Default Limit](#default-limit)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview
ChainNode is a highly available and scalable Node as a Service (NaaS) built on top of [ChainStorage](https://github.com/coinbase/chainstorage). Under the hooks, the on-chain data is backed by a fast key-value store, and most of the queries are served directly by the key-value store. The data is continuously replicated from ChainStorage (which in turn synchronizes the changes from a small node cluster), and re-indexed for various query patterns.

## Quick Start

Make sure your local go version is 1.18 by running the following commands:
```shell
brew install go@1.18
brew unlink go
brew link go@1.18
```

To set up for the first time (only done once):
```shell
make bootstrap
```

Rebuild everything:
```shell
make build
```

## Configuration

### Environment Variables

ChainNode depends on the following environment variables to resolve the path of the configuration.
The directory structure is as follows: `config/{namespace}/{blockchain}/{network}/{environment}.yml`.

- `CHAINNODE_NAMESPACE`:
  A `{namespace}` is logical grouping of several services, each of which manages its own blockchain and network. The
  default namespace is [chainnode](/config/chainnode).
  To deploy a different namespace, set the env var to the name of a subdirectory of [./config](/config).
- `CHAINNODE_CONFIG`:
  This env var, in the format of `{blockchain}-{network}`, determines the blockchain and network managed by the service.
  The naming is defined
  in [c3/common](/protos/coinbase/c3/common/common.proto).
- `CHAINNODE_ENVIRONMENT`:
  This env var controls the `{environment}` in which the service is deployed. Possible values include `production`
  , `development`, and `local` (which is also the default value).

### Endpoint Group

Endpoint group is an abstraction for one or more JSON-RPC endpoints.
[EndpointProvider](internal/clients/blockchain/endpoints/endpoint_provider.go) uses the `endpoint_group` config to implement
client-side routing to the node provider.

ChainNode utilizes two endpoint groups to serve its needs to the node provider:
* primary: This endpoint group is used to proxy the requests from the downstream node providers. Certain APIs, such as eth_call 
  and eth_getBalance, cannot be fulfilled by the storage layer, because the data is not available in ChainNode data source ChainStorage.
  To handle such requests, ChainNode redirect the request to this primary endpoint provider instead.
* validator: This endpoint group is used for validation purpose. For a percentage of incoming requests to ChainNode, the validation
  is performed by comparing the response from ChainNode and the response from this validator endpoint provider for an identical request.

If your node provider, e.g. QuickNode, already has built-in load balancing, your endpoint group may contain only one
endpoint, as illustrated by the following configuration:
```yaml
chain:
  client:
    primary:
      endpoint_group: |
        {
          "endpoints": [
            {
              "name": "quicknode-foo-bar-sticky",
              "url": "https://foo-bar.matic.quiknode.pro/****",
              "weight": 1
            }
          ]
        }
    validator:
      endpoint_group: |
        {
          "endpoints": [
            {
              "name": "quicknode-foo-bar-round-robin",
              "url": "https://foo-bar.matic.quiknode.pro/****",
              "weight": 1
            }
          ]
        }
```

### Overriding the Configuration

You may override any configuration using an environment variable. The environment variable should be prefixed with
"CHAINNODE_". For nested dictionary, use underscore to separate the keys.

For example, you may override the endpoint group config at runtime by injecting the following environment variables:
* primary: CHAINNODE_CHAIN_CLIENT_PRIMARY_ENDPOINT_GROUP
* validator: CHAINNODE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP

Alternatively, you may override the configuration by creating `secrets.yml` within the same directory. Its attributes
will be merged into the runtime configuration and take the highest precedence. Note that this file may contain
credentials and is excluded from check-in by `.gitignore`.

## Testing
### Unit Test
```shell
# Run everything
make test

# Run the controller package only
make test TARGET=internal/controller/...
```

### Integration Test
```shell
# Run everything
make integration

# Run the workflow package only
make integration TARGET=internal/workflow/...

# Run TestIntegrationBlobByHashStorageTestSuite only
make integration TARGET=internal/storage/... TEST_FILTER=TestIntegrationBlobByHashStorageTestSuite
```

### Functional Test
#### Run Functional Test Locally
```shell
# Run everything
make functional

# Run ControllerTestSuite only
make functional TARGET=internal/controller/... TEST_FILTER=ControllerTestSuite

# Run one test in ControllerTestSuite
make functional TARGET=internal/controller/... TEST_FILTER=ControllerTestSuite/TestBlocksByHash
```

## Local Development
### Running Server

Start the dockers by the docker-compose file from project root folder:
```shell
make localstack
```

The next step is to start the server locally in a new terminal:
```shell
# Ethereum Mainnet
# Use aws local stack
# Starts API server
make server

# Setup ChainStorage SDK credentials:
export CHAINSTORAGE_SDK_AUTH_HEADER=cb-nft-api-token
export CHAINSTORAGE_SDK_AUTH_TOKEN=****

# Starts workflow workers
make worker

# If want to start testnet (goerli) server
# Use aws local stack
make server CHAINNODE_CONFIG_NAME=ethereum-goerli
make worker CHAINNODE_CONFIG_NAME=ethereum-goerli
```

### Manage workflow locally
After finishing running server and worker, manage workflows using admin CLI:

```shell
# start coordinator locally
go run ./cmd/admin workflow start --workflow coordinator  --input '{}' --blockchain ethereum --env local --network mainnet

# stop coordinator locally
go run ./cmd/admin workflow stop --workflow coordinator   --input '' --blockchain ethereum --env local --network mainnet

# start an ingestor for blocks collection locally.
# Note that if you already run coordinator locally, there is no need to start an individual ingestor.
# This command is used when a new ingestor is being developed.
# When local testing, please specify the desired tag inside the input struct. Example: '{"collection":"blocks", tag:1}'
go run ./cmd/admin workflow start --workflow ingestor --input '{"collection":"blocks"}' --blockchain ethereum --env local --network mainnet


# stop an ingestor for blocks collection locally
go run ./cmd/admin workflow stop --workflow ingestor --input '{}' --blockchain ethereum --env local --network mainnet --workflowID workflow.ingestor/blocks/{tag}
```

## Storage

Inspect a table using AWS CLI:

```shell
# scan the entire table
aws dynamodb --no-sign-request --region local --endpoint-url http://localhost:4566 scan --table-name chainnode-collection-ethereum-mainnet --attributes-to-get "pk" "sk" "tag" "height" "hash"

# get a specific item
aws dynamodb --no-sign-request --region local --endpoint-url http://localhost:4566 get-item --table-name chainnode-collection-ethereum-mainnet --key '{"pk": {"S": "1#checkpoints"}, "sk": {"S": "blocks"}}'
```

The data within each dynamoDB item is compressed uzing gzip format. `base64 -d | gunzip` can be used to 
uncompress it to make it readable in the terminal. Example:

```shell
# Make data human-readable and format JSON
aws dynamodb --no-sign-request --region local --endpoint-url http://localhost:4566 get-item --table-name chainnode-collection-ethereum-mainnet --key '{"pk": {"S": "<partition_key>"}, "sk": {"S": "<sort_key>"}}' | jq -r '.Item.data.B' | base64 -d | gunzip | jq
```

## API

### DNS
- Local: `localhost:8000`

### API Documentation
[JSON-RPC API Methods](https://ethereum.org/en/developers/docs/apis/json-rpc/#json-rpc-methods)

### Eth namespace
```shell
# eth_blockNumber: https://eth.wiki/json-rpc/API#eth_blocknumber
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}' | jq

# eth_getBlockByNumber: https://eth.wiki/json-rpc/API#eth_getblockbynumber
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["0xdad3c1", false]}' | jq

# eth_getBlockByNumber with proxying turned off
curl -s localhost:8000/v1 -H "Content-Type: application/json" -H "x-chainnode-routing-mode: native-only" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["pending", false]}' | jq

# eth_getBlockByHash: https://eth.wiki/json-rpc/API#eth_getblockbyhash
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByHash", "params": ["0x849a3ac8f0d81df1a645701cdb9f90e58500d2eabb80ff3b7f4e8c13f025eff2", false]}' | jq

# eth_getBlockTransactionCountByHash https://eth.wiki/json-rpc/API#eth_getblocktransactioncountbyhash
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockTransactionCountByHash", "params": ["0x849a3ac8f0d81df1a645701cdb9f90e58500d2eabb80ff3b7f4e8c13f025eff2"]}' | jq

# GetBlockTransactionCountByNumber https://eth.wiki/json-rpc/API#eth_getblocktransactioncountbynumber
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockTransactionCountByNumber", "params": ["0xdad3c1"]}' | jq

# eth_getTransactionByHash: https://eth.wiki/json-rpc/API#eth_gettransactionbyhash
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionByHash", "params": ["0x633982a26e0cfba940613c52b31c664fe977e05171e35f62da2426596007e249"]}' | jq

# eth_getTransactionReceipt: https://eth.wiki/json-rpc/API#eth_gettransactionreceipt
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": ["0x633982a26e0cfba940613c52b31c664fe977e05171e35f62da2426596007e249"]}' | jq

# eth_getTransactionByBlockHashAndIndex https://eth.wiki/json-rpc/API#eth_gettransactionbyblockhashandindex
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionByBlockHashAndIndex", "params": ["0x849a3ac8f0d81df1a645701cdb9f90e58500d2eabb80ff3b7f4e8c13f025eff2", "0x0"]}' | jq

# eth_getTransactionByBlockNumberAndIndex https://eth.wiki/json-rpc/API#eth_gettransactionbyblocknumberandindex
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionByBlockNumberAndIndex", "params": ["0xdad3c1", "0x0"]}' | jq

# eth_getLogs: https://eth.wiki/json-rpc/API#eth_getlogs
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": [{"fromBlock": "0xdad3c1", "toBlock": "0xdad3c2"}]}' | jq

# eth_call: https://eth.wiki/json-rpc/API#eth_call
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params":[{ "to": "0x514910771af9ca656af840dff83e8264ecf986ca", "data": "0x70a08231000000000000000000000000f27eee60abacb983251fea941dd7350280a538ba"}, "latest"]}' | jq

# eth_getBalance: https://eth.wiki/json-rpc/API#eth_getbalance
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params":["0x8d97689c9818892b700e27f316cc3e41e17fbeb9", "latest"]}' | jq

# eth_getCode: https://eth.wiki/json-rpc/API#eth_getcode
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getCode", "params":["0x7f268357a8c2552623316e2562d90e642bb538e5", "latest"]}' | jq

# eth_getTransactionCount: https://eth.wiki/json-rpc/API#eth_gettransactioncount
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionCount", "params":["0xe222489ae12e15713cc1d65dd0ab2f5b18721bfd", "latest"]}' | jq

# eth_chainId: https://docs.alchemy.com/alchemy/apis/ethereum/eth-chainid
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"}' | jq

# eth_sendRawTransaction: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_sendrawtransaction
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": ["0xf889808609184e72a00082271094000000000000000000000000000000000000000080a47f74657374320000000000000000000000000000000000000000000000000000006000571ca08a8bbf888cfa37bbf0bb965423625641fc956967b81d12e23709cead01446075a01ce999b56a8a88504be365442ea61239198e23d1fce7d00fcfc5cd3b44b7215f"]}'

# eth_gasPrice: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_gasprice
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_gasPrice"}'

# eth_getStorageAt: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getstorageat
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getStorageAt", "params": ["0x6c8f2a135f6ed072de4503bd7c4999a1a17f824b", "0x0", "latest"]}'

# eth_estimateGas: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_estimategas
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_estimateGas", "params": [{"from": "0x8d97689c9818892b700e27f316cc3e41e17fbeb9", "to": "0xd3cda913deb6f67967b99d67acdfa1712c293601", "value": "0x1"}]}'

#eth_protocolVersion: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_protocolversion
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_protocolVersion"}'

#eth_syncing: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_syncing
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_syncing"}'

# eth_feeHistory
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_feeHistory", "params": [4, "latest", [25, 75]]}'

# eth_mining: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_mining
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_mining"}'

# eth_hashrate: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_hashrate
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_hashrate"}'

# eth_accounts: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_accounts
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_accounts"}'

# eth_newFilter: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_newfilter
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_newFilter", "params":[{}]}'

# eth_newBlockFilter: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_newblockfilter
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_newBlockFilter"}'

# eth_uninstallFilter: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_uninstallfilter
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_uninstallFilter", "params":["0x81440f9af726125cb7fc671eb0f2d8728d6ad699989a"]}'

# eth_getFilterChanges: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getfilterchanges
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getFilterChanges", "params":["0x81440f9af726125cb7fc671eb0f2d8728d6ad699989a"]}'

# eth_getFilterLogs: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getfilterlogs
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getFilterLogs", "params":["0x81440f9af726125cb7fc671eb0f2d8728d6ad699989a"]}'

# eth_getWork: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getwork
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_getWork"}'

# eth_submitWork: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_submitwork
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_submitWork", "params": ["0x0000000000000001", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "0xD1FE5700000000000000000000000000D1FE5700000000000000000000000000"]}'

# eth_submitHashrate: https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_submithashrate
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "eth_submitHashrate", "params":["0x500000", "0x59daa26581d0acd1fce254fb7e85952f4c09d0915afd33d3886cd914bc7d283c"]}'
```

### Debug namespace
```bash
# debug_traceBlockByHash: https://geth.ethereum.org/docs/rpc/ns-debug#debug_traceblockbyhash
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "debug_traceBlockByHash", "params": ["0xe075488f2716495e97c43f6eb2994964074a70245cca5844b308479ccbbb9ae7", {"tracer": "callTracer"}]}' | jq

# debug_traceBlockByNumber: https://geth.ethereum.org/docs/rpc/ns-debug#debug_traceblockbynumber
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "debug_traceBlockByNumber", "params": ["0xe11130", {"tracer": "callTracer"}]}' | jq
```

### Net namespace
```bash
# net_version: https://eth.wiki/json-rpc/API#net_version
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "net_version"}' | jq

# net_listening: https://ethereum.org/en/developers/docs/apis/json-rpc/#net_listening
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "net_listening"}' | jq

# net_peercount: https://ethereum.org/en/developers/docs/apis/json-rpc/#net_peercount
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "net_peerCount"}' | jq
```

### Web3 namespace
```bash
# web3_clientVersion: https://ethereum.org/en/developers/docs/apis/json-rpc/#web3_clientversion
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "web3_clientVersion"}'
```

### Batch Request
```bash
# batch request of eth_getTransactionReceipt: https://www.jsonrpc.org/specification#batch
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '[{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": ["0x633982a26e0cfba940613c52b31c664fe977e05171e35f62da2426596007e249"]}, { "jsonrpc": "2.0", "id": 2, "method": "eth_getTransactionReceipt", "params": ["0x3a7d521b20b5684e0e9ec14aeebe8ccab67137f7d5c2589efb55b0625fcc9c6d"]}]' | jq
```

### Bor namespace (only for **Polygon**)
```bash
# bor_getAuthor: https://www.quicknode.com/docs/polygon/bor_getAuthor
curl -s localhost:8000/v1 -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "bor_getAuthor", "params": ["latest"]}' | jq .
```

### GraphQL
```bash
# GraphQL is available in select network: https://geth.ethereum.org/docs/interacting-with-geth/rpc/graphql
curl -s localhost:8000/v1/graphql -H "Content-Type: application/json" -d '{ "query": "query { block { number } }" }'
```

### Connect to Geth JavaScript Console
- Install geth following the [instructions](https://geth.ethereum.org/docs/install-and-build/installing-geth)
- Run the following command:
```bash
~$ geth attach localhost:8000/v1
Welcome to the Geth JavaScript console!

instance: Geth/v1.10.19-omnibus-1fd05ab6/linux-amd64/go1.18.3
at block: 15135980 (Wed Jul 13 2022 11:51:25 GMT-0700 (PDT))
modules: debug:1.0 eth:1.0 net:1.0 rpc:1.0 web3:1.0

To exit, press ctrl-d
> eth.blockNumber
15135980
> eth.getBlockByNumber(15135980)
...
> eth.getTransaction("0xc16410b8245b404e20319b6825e846b7b0c985da8e62a1db7c66524877530194")
...
> debug.traceBlockByNumber(15135980, {"tracer": "callTracer"})
...
```

## TAG Generator for Proxy Methods
ChainNode Template API Generator (TAG) is a templated code generator capable of self-implementing ChainNode Proxy API methods.
### TAG Usage:
  1. Fill in TAG Template (`internal/tag_generator/tag_template.txt`) such that it contains the method signature of all proxy methods that require implementation. 
  2. The code generator is automatically invoked via `make build`. If you would only like to call the TAG generator instead of the entire build process, run `make tag`
  3. Code-generated implementation for proxy methods will be stored in internal/controller/ethereum/handler with all of the generated files being denoted with the `*tag_generated.go` suffix

### Changing TAG's generated code format:
  - The code generated by TAG are based on a series of text/template files found in `internal/tag/code_templates`. Templates define the structure of the generated code while unique attributes of the structure are filled out in `generator.go`
  - Example of Code Generation Template:
    ```bash
    func (n {{.Namespace}}) {{.MethodName}}({{.ParametersAndTypes}}) (json.RawMessage, error) {
        return n.receiver.{{.MethodName}}({{.Parameters}})
    }
     ```
    - All elements of the template that are to be filled out by the generator have placeholder values: `{{.VariableName}}`
    - Fixed values including parentheses, newlines, tabs can be directly hard coded in the templates.
  - Example of Using Code Generation Template in Generator:
    - Code Generation Templates are filled out in the generator using Maps. Declare a map such that the keys are the placeholder values in the templates, values are the values to fill in the template
    ```bash
    for i := range element {
        vars := make(map[string]interface{})
        vars["Namespace"] = element[i].Namespace
        vars["MethodName"] = element[i].ApiName
        vars["ParametersAndTypes"] = parseParametersAndTypes(element[i].Parameter, element[i].ParameterType)
        vars["Parameters"] = parseParameters(element[i].Parameter)
        namespace = tag.ProcessTemplate(namespaceTmpl, vars)
    }
    ```
    - In this example: we replace the Namespace, MethodName, ParametersAndTypes, Parameters placeholder values and substitute in the actual values to be used for code generation.
    - `tag.ProcessTemplate` takes 2 parameters: `string containing path to the template file` as well as a `map containing the key value pairs to be injected into the template`. The function injects the actual values into the template file, returning a string containing the contents of the template with the injected values.
  - IMPORTANT: following any changes to TAG's code templates, it is important that the unit test also be updated (procedure in the following section)

### Updating TAG's Unit Test:
  - TAG's unit test only needs to be updated when the format of TAG's code generation is changed (see section above). 
  - Unit test should be directly edited (`internal/utils/fixtures/tag/*`) such that the expected values now reflect the changes of the new generated code format.
  - Unit tests for TAG's code generation functions are stored in `internal/tag_generator/unit_test_templates/*`; responsible for testing functions used for TAG code generation process.
  - Unit tests for TAG's code generation output are stored in `internal/utils/fixtures/tag/*`; responsible for validating code generation output.

## JSONRPC Server Batch Request Limit
### Default Limit
  - The JSONRPC service batch request limit has a default limit defined per chain in the `base.yml` configuration file(note: it could also have overrides in different environments, please check `development.yml` and `production.yml` as well). e.g. ethereum default limit is [1000](https://github.com/coinbase/chainnode/blob/5da99f53ee5bc9c1a313e2e5078702406abb42b4/config/chainnode/ethereum/mainnet/base.yml#L77)
