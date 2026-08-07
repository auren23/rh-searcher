// Multicall3 批量 eth_call 封装。
// Robinhood 链 canonical Multicall3（0xcA11bde...，3808 字节）已实测部署；
// aggregate3 允许单次 eth_call 携带任意数量只读调用，是 token-group 批量
// 状态读取的主要路径。不可用时由 Adapter 回退 JSON-RPC batch。
package v3

import (
	"context"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// multicall3Addr 各链通用的 canonical Multicall3 部署地址（Robinhood 已确认）。
var multicall3Addr = common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

// mcCall aggregate3 的一条调用（allowFailure=true：单条失败不整体 revert）。
type mcCall struct {
	Target   common.Address
	CallData []byte
}

// mcResult aggregate3 的一条返回值。
type mcResult struct {
	Success    bool
	ReturnData []byte
}

type multicall3 struct {
	cli  *ethclient.Client
	addr common.Address
}

func newMulticall3(cli *ethclient.Client) *multicall3 {
	return &multicall3{cli: cli, addr: multicall3Addr}
}

// aggregate3 调用 Multicall3.aggregate3((address,bool,bytes)[]) 执行全部只读调用。
// 单条失败（success=false）不返回错误：由调用方按索引检查。
func (m *multicall3) aggregate3(ctx context.Context, calls []mcCall, block *big.Int) ([]mcResult, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	tupleType, err := abi.NewType("tuple[]", "", []abi.ArgumentMarshaling{
		{Name: "target", Type: "address"},
		{Name: "allowFailure", Type: "bool"},
		{Name: "callData", Type: "bytes"},
	})
	if err != nil {
		return nil, fmt.Errorf("multicall3 call type: %w", err)
	}
	type call3 struct {
		Target       common.Address
		AllowFailure bool
		CallData     []byte
	}
	args := make([]call3, 0, len(calls))
	for _, c := range calls {
		args = append(args, call3{Target: c.Target, AllowFailure: true, CallData: c.CallData})
	}
	calldata, err := abi.Arguments{{Type: tupleType}}.Pack(args)
	if err != nil {
		return nil, fmt.Errorf("multicall3 pack: %w", err)
	}
	sel := crypto.Keccak256([]byte("aggregate3((address,bool,bytes)[])"))[:4]
	res, err := m.cli.CallContract(ctx, ethereum.CallMsg{To: &m.addr, Data: append(sel, calldata...)}, block)
	if err != nil {
		return nil, fmt.Errorf("multicall3 aggregate3: %w", err)
	}
	resType, err := abi.NewType("tuple[]", "", []abi.ArgumentMarshaling{
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	})
	if err != nil {
		return nil, fmt.Errorf("multicall3 result type: %w", err)
	}
	vals, err := abi.Arguments{{Type: resType}}.Unpack(res)
	if err != nil {
		return nil, fmt.Errorf("multicall3 unpack: %w", err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("multicall3 empty result")
	}
	// Unpack 返回的 tuple[] 是 ABI 包动态生成的匿名结构体类型；
	// 切片级 ConvertType 失败（命名元素 vs 匿名元素），元素级转换可行。
	type mc3Result struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	}
	srcList := reflect.ValueOf(vals[0])
	dstType := reflect.TypeOf([]mc3Result{})
	if srcList.Kind() != reflect.Slice {
		return nil, fmt.Errorf("multicall3 unexpected result type %T", vals[0])
	}
	dstList := reflect.MakeSlice(dstType, srcList.Len(), srcList.Len())
	for i := 0; i < srcList.Len(); i++ {
		dstList.Index(i).Set(srcList.Index(i).Convert(dstType.Elem()))
	}
	list := dstList.Interface().([]mc3Result)
	if len(list) != len(calls) {
		return nil, fmt.Errorf("multicall3 result count %d != calls %d", len(list), len(calls))
	}
	out := make([]mcResult, 0, len(list))
	for _, r := range list {
		out = append(out, mcResult{Success: r.Success, ReturnData: r.ReturnData})
	}
	return out, nil
}
