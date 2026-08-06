package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/auren23/rh-searcher/internal/arbitrage"
)

// mockRPCServer 最小 JSON-RPC mock：记录 eth_estimateGas 的 params。
type mockRPCServer struct {
	mu         sync.Mutex
	estimateCalls []struct{ data, block string }
	estErr     error  // 注入错误（nil = 成功）
	estGas     uint64 // 成功返回值
	headers    map[string]any
}

func (m *mockRPCServer) handle(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		ID     int             `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	switch req.Method {
	case "eth_estimateGas":
		var params []json.RawMessage
		_ = json.Unmarshal(req.Params, &params)
		m.mu.Lock()
		rec := struct{ data, block string }{}
		if len(params) > 0 {
			var obj struct{ Data string `json:"data"` }
			_ = json.Unmarshal(params[0], &obj)
			rec.data = obj.Data
		}
		if len(params) > 1 {
			_ = json.Unmarshal(params[1], &rec.block)
		}
		m.estimateCalls = append(m.estimateCalls, rec)
		m.mu.Unlock()
		if m.estErr != nil && len(params) > 1 {
			// 只对历史调用（带 block 参数）注入错误；latest 调用正常成功
			writeRPCError(w, req.ID, m.estErr)
			return
		}
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": fmt.Sprintf("0x%x", m.estGas)}
		json.NewEncoder(w).Encode(resp)
	case "eth_call":
		var params []json.RawMessage
		_ = json.Unmarshal(req.Params, &params)
		result := "0x" + strings.Repeat("00", 31) + "01" // profit=1
		if len(params) > 0 {
			var obj struct {
				To string `json:"to"`
			}
			_ = json.Unmarshal(params[0], &obj)
			if strings.EqualFold(obj.To, "0x00000000000000000000000000000000000000C8") {
				// NodeInterface.gasEstimateL1Component：96 字节
				// (gasEstimateForL1=1000, baseFee=1e9, l1BaseFeeEstimate=1e9)
				result = "0x" + strings.Repeat("00", 31) + "03e8" +
					strings.Repeat("00", 31) + "01" +
					strings.Repeat("00", 31) + "01"
			}
		}
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}
		json.NewEncoder(w).Encode(resp)
	case "eth_getBlockByNumber":
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID,
			"result": map[string]any{
				"number": "0x64", "timestamp": "0x64", "baseFeePerGas": "0x3b9aca00",
				"hash": "0x" + strings.Repeat("ab", 32),
				"parentHash": "0x" + strings.Repeat("cd", 32),
				"sha3Uncles": "0x" + strings.Repeat("1d", 32),
				"miner": "0x" + strings.Repeat("11", 20),
				"stateRoot": "0x" + strings.Repeat("22", 32),
				"transactionsRoot": "0x" + strings.Repeat("33", 32),
				"receiptsRoot": "0x" + strings.Repeat("44", 32),
				"logsBloom": "0x" + strings.Repeat("00", 256),
				"difficulty": "0x0", "gasLimit": "0x1c9c380", "gasUsed": "0x0",
				"extraData": "0x", "mixHash": "0x" + strings.Repeat("55", 32),
				"nonce": "0x0000000000000000",
			}}
		json.NewEncoder(w).Encode(resp)
	default:
		writeRPCError(w, req.ID, fmt.Errorf("method not found"))
	}
}

func writeRPCError(w http.ResponseWriter, id int, err error) {
	code := -32000
	msg := err.Error()
	if strings.Contains(msg, "invalid params") {
		code = -32602
	}
	if strings.Contains(msg, "method not found") {
		code = -32601
	}
	resp := map[string]any{"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": msg}}
	json.NewEncoder(w).Encode(resp)
}

func newSimForTest(t *testing.T, m *mockRPCServer) (*ExecutorSimulator, context.Context, *big.Int) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(srv.Close)
	cli, err := ethclient.Dial(srv.URL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sim := NewExecutorSimulator(cli, common.Address{0xee}, common.Address{0xff}, 5_000_000)
	return sim, context.Background(), big.NewInt(100)
}

func mockCandidate() *arbitrage.Candidate {
	return &arbitrage.Candidate{
		Route: []arbitrage.Hop{
			{Pool: common.Address{1}, TokenIn: common.Address{2}, TokenOut: common.Address{3}},
			{Pool: common.Address{4}, TokenIn: common.Address{3}, TokenOut: common.Address{2}},
		},
		InputAmount: big.NewInt(1e15),
	}
}

// P0-1: historical 估算使用原始 calldata（历史 deadline），且第二个参数是目标区块。
func TestHistoricalEstimateGasUsesOriginalCalldataAndBlock(t *testing.T) {
	m := &mockRPCServer{estGas: 300000}
	sim, ctx, block := newSimForTest(t, m)
	res, err := sim.Simulate(ctx, mockCandidate(), 4663, block)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if res.GasEstimateMode != GasEstimateComplete {
		t.Fatalf("mode=%s want historical_complete", res.GasEstimateMode)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.estimateCalls) != 1 {
		t.Fatalf("estimate calls=%d want 1", len(m.estimateCalls))
	}
	call := m.estimateCalls[0]
	if call.block != "0x64" {
		t.Fatalf("estimate block=%s want 0x64", call.block)
	}
	// 原始 calldata（历史 deadline）→ 与保存的 CalldataHash 一致
	want, _ := BuildExecuteV3CycleCalldataAt(mockCandidate(), 4663, 100)
	if call.data != "0x"+common.Bytes2Hex(want) {
		t.Fatalf("estimate data mismatch (must be original historical calldata)")
	}
	if res.GasPriceWei.Uint64() != 1e9 { // baseFeePerGas = 1 gwei
		t.Fatalf("gas price=%d want historical base fee 1e9", res.GasPriceWei)
	}
}

// P0-2: 明确不支持（-32602）→ fallback latest，标记 latest_approximation。
func TestHistoricalEstimateUnsupportedFallsBack(t *testing.T) {
	m := &mockRPCServer{estErr: fmt.Errorf("invalid params: too many arguments")}
	sim, ctx, block := newSimForTest(t, m)
	res, err := sim.Simulate(ctx, mockCandidate(), 4663, block)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if res.GasEstimateMode != GasEstimateLatest {
		t.Fatalf("mode=%s want latest_approximation", res.GasEstimateMode)
	}
	// 第二次调用不再尝试 historical（缓存）：带 block 参数的调用必须只有 1 次
	m.estErr = nil
	if _, err := sim.Simulate(ctx, mockCandidate(), 4663, block); err != nil {
		t.Fatalf("simulate2: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	histCalls := 0
	for _, c := range m.estimateCalls {
		if c.block != "" {
			histCalls++
		}
	}
	if histCalls != 1 {
		t.Fatalf("historical estimate calls=%d want 1 (cached unsupported)", histCalls)
	}
}

// P0-2: 基础设施错误（429）→ 返回 error（区块保持未评估）。
func TestHistoricalEstimateInfraErrorRetries(t *testing.T) {
	m := &mockRPCServer{estErr: fmt.Errorf("429 Too Many Requests")}
	sim, ctx, block := newSimForTest(t, m)
	_, err := sim.Simulate(ctx, mockCandidate(), 4663, block)
	if err == nil {
		t.Fatalf("expected infra error")
	}
	if !isInfraError(err) {
		t.Fatalf("error must be infra-classified: %v", err)
	}
}
