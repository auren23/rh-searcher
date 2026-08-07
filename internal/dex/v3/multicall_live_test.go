package v3

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func TestLiveMulticall3Batch(t *testing.T) {
	url := os.Getenv("RH_ALCHEMY_RPC")
	if url == "" {
		t.Skip("no RH_ALCHEMY_RPC")
	}
	cli, err := ethclient.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewAdapter(cli, "uniswap-v3", common.HexToAddress("0x1f7d7550b1b028f7571e69a784071f0205fd2efa"),
		common.HexToAddress("0x8876789976dEcBfCbBbe364623C63652db8C0904"), "universal", common.Hash{}, 8930)
	// WETH/USDC pool from universe sample (verified earlier)
	p1 := &Pool{Address: common.HexToAddress("0x856532422288a8d35459b6cbbb8b99c99937882B"), TickSpacing: 60}
	p2 := &Pool{Address: common.HexToAddress("0xE4490b47FE9c26626A58d12f68646811e9A5a61c"), TickSpacing: 60}
	block := big.NewInt(30199151)
	calls, err := a.RefreshPoolsStateAt(context.Background(), []*Pool{p1, p2}, block)
	if err != nil {
		t.Fatal(err)
	}
	// 1 次往返 = 阶段1（slot0+liquidity）；有流动性池 +1 次（tickBitmap）
	if calls < 1 || calls > 2 {
		t.Fatalf("calls=%d want 1..2", calls)
	}
	liqSeen := 0
	for _, p := range []*Pool{p1, p2} {
		if p.SqrtPriceX96 == nil || p.SqrtPriceX96.Sign() <= 0 {
			t.Fatalf("pool %s no price", p.Address.Hex())
		}
		if p.Liquidity != nil && p.Liquidity.Sign() > 0 {
			liqSeen++
			if !p.BitmapLoaded(p.WordPos()) {
				t.Fatalf("pool %s bitmap not loaded", p.Address.Hex())
			}
		}
		t.Logf("pool=%s price=%s liq=%v tick=%d word=%d", p.Address.Hex(), p.SqrtPriceX96.String(), p.Liquidity, p.Tick, p.WordPos())
	}
	if liqSeen == 0 {
		t.Logf("note: sampled pools have zero liquidity (typical for 1%%-fee universe)")
	}
}
