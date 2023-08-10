package arbitrumutil

import c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"

const (
	ARBITRUM_NITRO_UPGRADE_BLOCK_HEIGHT = uint64(22207818)
)

func IsArbitrumAndBeforeNITROUpgrade(blockchain c3common.Blockchain, network c3common.Network, blockHeight uint64) bool {
	return IsArbitrum(blockchain, network) &&
		IsBeforeNITROUpgrade(blockHeight) &&
		blockHeight >= 0
}

func IsArbitrum(blockchain c3common.Blockchain, network c3common.Network) bool {
	// TODO: needs to be updated if Arbitrum_Testnet is onboarded in the future
	return network == c3common.Network_NETWORK_ARBITRUM_MAINNET &&
		blockchain == c3common.Blockchain_BLOCKCHAIN_ARBITRUM
}

func IsBeforeNITROUpgrade(blockHeight uint64) bool {
	return blockHeight < ARBITRUM_NITRO_UPGRADE_BLOCK_HEIGHT
}
