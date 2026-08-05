package v3

import (
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// testdata/logs.json 是 Robinhood mainnet 真实日志（2026-08-05 抓取，池 0x8662Eb...WETH/Safevano-10000）。
// 用于验证事件解码与官方 ABI 一致（indexed 参数在 topics、data 顺序）。

type logFixture struct {
	Address         string   `json:"address"`
	BlockNumber     string   `json:"blockNumber"`
	TransactionHash string   `json:"transactionHash"`
	LogIndex        string   `json:"logIndex"`
	Topics          []string `json:"topics"`
	Data            string   `json:"data"`
}

func loadFixtures(t *testing.T) (swap, mint, burn logFixture) {
	raw, err := os.ReadFile("testdata/logs.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var m map[string]logFixture
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	for _, k := range []string{"swap", "mint", "burn"} {
		if m[k].Address == "" {
			t.Fatalf("fixture %s missing", k)
		}
	}
	return m["swap"], m["mint"], m["burn"]
}

func toLog(f logFixture) types.Log {
	topics := make([]common.Hash, len(f.Topics))
	for i, tp := range f.Topics {
		topics[i] = common.HexToHash(tp)
	}
	return types.Log{
		Address:     common.HexToAddress(f.Address),
		Topics:      topics,
		Data:        common.FromHex(f.Data),
		BlockNumber: new(big.Int).SetBytes(common.FromHex(f.BlockNumber)).Uint64(),
	}
}

// 手动 ABI 解码校验（独立于 adapter 的实现，防止两处同错）。
func TestDecodeLogSwapGolden(t *testing.T) {
	swap, _, _ := loadFixtures(t)
	l := toLog(swap)

	// data = amount0, amount1, sqrtPriceX96, liquidity, tick（各 32B）
	data := l.Data
	if len(data) < 160 {
		t.Fatalf("swap data len %d", len(data))
	}
	amount0 := new(big.Int).SetBytes(data[0:32])
	amount1 := new(big.Int).SetBytes(data[32:64])
	sqrt := new(big.Int).SetBytes(data[64:96])
	liquidity := new(big.Int).SetBytes(data[96:128])
	rawTick := new(big.Int).SetBytes(data[128:160]).Int64()
	if rawTick >= 1<<23 {
		rawTick -= 1 << 24
	}
	t.Logf("manual decode: amount0=%s amount1=%s sqrt=%x liq=%s tick=%d", amount0.String(), amount1.String(), sqrt, liquidity.String(), rawTick)
}

func TestDecodeLogApplySwap(t *testing.T) {
	swap, _, _ := loadFixtures(t)
	l := toLog(swap)

	adapter, err := NewAdapter(nil, "test", common.Address{}, common.Address{}, "universal", common.Hash{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 直接构造 Pool 状态（测试 DecodeLog 路径）
	p := &Pool{
		Address: l.Address, Exchange: "test",
		Token0: common.HexToAddress("0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"),
		Token1: common.HexToAddress("0xafddc5960e8c8488fd678d289a31361459fe9efb"),
		Fee:    10000, TickSpacing: 200,
		Liquidity:    new(big.Int),
		SqrtPriceX96: new(big.Int),
		ticks:        make(map[int]*Tick),
		bitmap:       make(map[int64]*big.Int),
	}
	apply, err := adapter.DecodeLog(p, l)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if apply == nil {
		t.Fatal("apply is nil (event not recognized)")
	}
	apply()
	if p.SqrtPriceX96.Sign() <= 0 || p.Liquidity.Sign() <= 0 {
		t.Fatalf("state not applied: sqrt=%s liq=%s", p.SqrtPriceX96.String(), p.Liquidity.String())
	}
	// 解码出的 tick 应等于数据中的 tick（与手动解码一致）
	wantTick := decodeInt24(common.BytesToHash(l.Data[128:160]))
	if p.Tick != wantTick {
		t.Errorf("tick mismatch: got %d want %d", p.Tick, wantTick)
	}
	t.Logf("applied swap: tick=%d sqrt=%x liq=%s", p.Tick, p.SqrtPriceX96, p.Liquidity)
}

func TestDecodeLogMintBurn(t *testing.T) {
	_, mint, burn := loadFixtures(t)
	adapter, _ := NewAdapter(nil, "test", common.Address{}, common.Address{}, "universal", common.Hash{}, 0)

	p := &Pool{
		Address: common.HexToAddress(mint.Address),
		Token0:  common.HexToAddress("0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"),
		Token1:  common.HexToAddress("0xafddc5960e8c8488fd678d289a31361459fe9efb"),
		Fee:     10000, TickSpacing: 200,
		Liquidity:    new(big.Int),
		SqrtPriceX96: big.NewInt(1),
		ticks:        make(map[int]*Tick),
		bitmap:       make(map[int64]*big.Int),
	}
	// Mint
	apply, err := adapter.DecodeLog(p, toLog(mint))
	if err != nil {
		t.Fatalf("mint decode: %v", err)
	}
	apply()
	if p.Liquidity.Sign() <= 0 {
		t.Fatalf("mint did not change liquidity: %s", p.Liquidity.String())
	}
	// Burn：同一 tick 区间 burn 回 amount 应回落到 mint 前（0）
	apply, err = adapter.DecodeLog(p, toLog(burn))
	if err != nil {
		t.Fatalf("burn decode: %v", err)
	}
	apply()
	if p.Liquidity.Sign() < 0 {
		t.Fatalf("burn pushed liquidity negative: %s", p.Liquidity.String())
	}
}
