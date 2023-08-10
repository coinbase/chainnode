package arbitrumutil

import (
	"testing"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

const (
	blockHeightBeforeNITROUpgrade = ARBITRUM_NITRO_UPGRADE_BLOCK_HEIGHT - 1
	blockHeightAfterNITROUpgrade  = ARBITRUM_NITRO_UPGRADE_BLOCK_HEIGHT
	blockTag                      = uint32(1)
	eventTag                      = uint32(3)
	sequence                      = api.Sequence(123)
)

func TestIsArbitrumAndBeforeNITROUpgrade(t *testing.T) {
	require := testutil.Require(t)

	// Arbitrum and BEFORE the NITRO upgrade
	arbitrumEvent := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	arbitrumEvent.Block.Height = blockHeightBeforeNITROUpgrade
	block := &chainstorageapi.Block{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    c3common.Network_NETWORK_ARBITRUM_MAINNET,
	}

	require.True(IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), arbitrumEvent.GetBlock().GetHeight()))

	// Arbitrum and AFTER the NITRO upgrade
	arbitrumEvent.Block.Height = blockHeightAfterNITROUpgrade
	require.False(IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), arbitrumEvent.GetBlock().GetHeight()))

	// Non-Arbitrum and AFTER the NITRO upgrade
	block = &chainstorageapi.Block{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
	}
	require.False(IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), arbitrumEvent.GetBlock().GetHeight()))

	// Non-Arbitrum and BEFORE the NITRO upgrade
	arbitrumEvent.Block.Height = blockHeightBeforeNITROUpgrade
	require.False(IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), arbitrumEvent.GetBlock().GetHeight()))
}

func TestIsArbitrum(t *testing.T) {
	require := testutil.Require(t)

	block := &chainstorageapi.Block{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    c3common.Network_NETWORK_ARBITRUM_MAINNET,
	}

	require.True(IsArbitrum(block.GetBlockchain(), block.GetNetwork()))

	block.Network = c3common.Network_NETWORK_ETHEREUM_MAINNET
	block.Blockchain = c3common.Blockchain_BLOCKCHAIN_ETHEREUM

	require.False(IsArbitrum(block.GetBlockchain(), block.GetNetwork()))
}

func TestIsBeforeNITROUpgrade(t *testing.T) {
	require := testutil.Require(t)

	require.True(IsBeforeNITROUpgrade(uint64(1000000)))
	require.False(IsBeforeNITROUpgrade(uint64(30000000)))
}
