// Package chain 提供链数据源抽象：区块与日志订阅、历史查询、重组处理。
// 数据源可替换（WS / RPC / Sequencer Feed / 回放文件），策略引擎只依赖接口。
package chain

import (
	"context"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BlockSource 区块数据源。MVP 用 WebSocket，后续可加 SequencerFeedSource / ReplayFileSource。
type BlockSource interface {
	// SubscribeBlocks 订阅新头；通道在断线重连后继续推送，不关闭。
	SubscribeBlocks(ctx context.Context) (<-chan BlockEvent, <-chan error)
	// BlockByNumber 按高度取区块（用于补块）。
	BlockByNumber(ctx context.Context, number uint64) (*types.Block, error)
}

// LogSource 日志数据源。
type LogSource interface {
	// SubscribeLogs 订阅匹配 FilterQuery 的日志。
	SubscribeLogs(ctx context.Context, query ethereum.FilterQuery) (<-chan types.Log, <-chan error)
	// HistoricalLogs 拉取历史日志（断线补齐、回放）。
	HistoricalLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
}

// BlockEvent 区块事件。
type BlockEvent struct {
	Number  uint64
	Hash    common.Hash
	Parent  common.Hash
	Time    uint64
	Reorged bool // 由重组检测标记：该区块已被父链替换
}

// Source 组合数据源，由注册表管理。
type Source interface {
	BlockSource
	LogSource
}
