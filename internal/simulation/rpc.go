package simulation

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// RPCSimulator 用 eth_call 模拟执行合约调用。
// 必须使用独立的 sim RPC 组，避免占用读取/发送端点。
type RPCSimulator struct {
	cli    *ethclient.Client
	from   common.Address
	to     common.Address
	maxGas uint64
}

func NewRPCSimulator(cli *ethclient.Client, from, to common.Address, maxGas uint64) *RPCSimulator {
	return &RPCSimulator{cli: cli, from: from, to: to, maxGas: maxGas}
}

func (s *RPCSimulator) Simulate(ctx context.Context, tx *types.Transaction) (SimulationResult, error) {
	msg := ethereum.CallMsg{
		From:  s.from,
		To:    &s.to,
		Gas:   s.maxGas,
		Data:  tx.Data(),
		Value: tx.Value(),
	}
	out, err := s.cli.CallContract(ctx, msg, nil)
	if err != nil {
		return SimulationResult{Success: false, RevertMsg: decodeRevert(err)}, nil
	}
	gas, err := s.cli.EstimateGas(ctx, msg)
	if err != nil {
		return SimulationResult{Success: false, RevertMsg: decodeRevert(err)}, nil
	}
	price, err := s.cli.SuggestGasPrice(ctx)
	if err != nil {
		price = big.NewInt(0)
	}
	return SimulationResult{
		Success:    true,
		GasUsed:    gas,
		GasPrice:   price,
		OutputWETH: new(big.Int).SetBytes(out),
	}, nil
}

// decodeRevert 提取 revert reason（尽力而为）。
func decodeRevert(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
