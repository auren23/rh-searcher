package v3

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// RouteCapablePools：只有 token 拥有 >=2 个 WETH 池时才进入实时订阅集。
func TestRouteCapablePools(t *testing.T) {
	weth := common.HexToAddress("0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73")
	a := common.HexToAddress("0x0000000000000000000000000000000000000001")
	b := common.HexToAddress("0x0000000000000000000000000000000000000002")
	c := common.HexToAddress("0x0000000000000000000000000000000000000003")
	d := common.HexToAddress("0x0000000000000000000000000000000000000004")
	tok1 := common.HexToAddress("0x0000000000000000000000000000000000000011")
	tok2 := common.HexToAddress("0x0000000000000000000000000000000000000012")
	tok3 := common.HexToAddress("0x0000000000000000000000000000000000000013")
	pools := []UniversePool{
		{Address: a.Hex(), Token0: weth.Hex(), Token1: tok1.Hex()}, // tok1: 2 池 → capable
		{Address: b.Hex(), Token0: weth.Hex(), Token1: tok1.Hex()},
		{Address: c.Hex(), Token0: tok2.Hex(), Token1: weth.Hex()}, // tok2: 1 池 → 不订阅
		{Address: d.Hex(), Token0: tok3.Hex(), Token1: tok3.Hex()}, // 无 WETH → 跳过
	}
	got := RouteCapablePools(pools, weth)
	if len(got) != 2 {
		t.Fatalf("got %d pools, want 2: %v", len(got), got)
	}
	seen := map[common.Address]bool{}
	for _, p := range got {
		seen[p] = true
	}
	if !seen[a] || !seen[b] || seen[c] || seen[d] {
		t.Fatalf("route-capable set wrong: %v", got)
	}
}

// Mint/Burn topic0 必须与 rh-arbitrage 事件过滤硬编码常量一致（签名漂移防护）。
func TestMintBurnTopics(t *testing.T) {
	if MintTopic() != common.HexToHash("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde") {
		t.Fatalf("MintTopic mismatch: %s", MintTopic().Hex())
	}
	if BurnTopic() != common.HexToHash("0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c") {
		t.Fatalf("BurnTopic mismatch: %s", BurnTopic().Hex())
	}
}

// InvalidateBitmapCache 删除该池的跨 head bitmap word 缓存（白盒，无需 RPC）。
func TestInvalidateBitmapCache(t *testing.T) {
	pool := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	a := &Adapter{bitmapWordCache: map[common.Address]map[int64]*big.Int{
		pool: {5: big.NewInt(123), 6: big.NewInt(456)},
	}}
	a.InvalidateBitmapCache(pool)
	if len(a.bitmapWordCache) != 0 {
		t.Fatalf("bitmap cache not invalidated: %v", a.bitmapWordCache)
	}
	// 其他池不受影响
	pool2 := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	a2 := &Adapter{bitmapWordCache: map[common.Address]map[int64]*big.Int{
		pool:  {5: big.NewInt(1)},
		pool2: {7: big.NewInt(2)},
	}}
	a2.InvalidateBitmapCache(pool)
	if len(a2.bitmapWordCache) != 1 || a2.bitmapWordCache[pool2][7].Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("invalidate hit wrong pool: %v", a2.bitmapWordCache)
	}
}
