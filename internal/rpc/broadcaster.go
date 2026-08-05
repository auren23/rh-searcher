package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// GMGNBroadcaster 通过 GMGN 快速 RPC 发送交易（生产首选）。
type GMGNBroadcaster struct {
	Group *Group // 对应 config 中 send 组里的 GMGN 端点
}

func NewGMGNBroadcaster(g *Group) *GMGNBroadcaster { return &GMGNBroadcaster{Group: g} }

func (b *GMGNBroadcaster) SendRawTransaction(ctx context.Context, rawTx []byte) (common.Hash, error) {
	c := b.Group.Pick()
	if c == nil {
		return common.Hash{}, errors.New("no healthy GMGN endpoint")
	}
	start := time.Now()
	err := c.RPC.CallContext(ctx, nil, "eth_sendRawTransaction", common.Bytes2Hex(rawTx))
	c.Record(err == nil, time.Since(start))
	if err != nil {
		return common.Hash{}, fmt.Errorf("gmgn send: %w", err)
	}
	// eth_sendRawTransaction 返回 tx hash；go-ethereum CallContext 到 nil 不解析返回值，
	// 直接本地重算 hash 更可靠：
	return hashRawTx(rawTx), nil
}

// SequencerBroadcaster 通过 Robinhood Sequencer Endpoint 发送（备用）。
type SequencerBroadcaster struct {
	Group *Group
}

func NewSequencerBroadcaster(g *Group) *SequencerBroadcaster { return &SequencerBroadcaster{Group: g} }

func (b *SequencerBroadcaster) SendRawTransaction(ctx context.Context, rawTx []byte) (common.Hash, error) {
	c := b.Group.Pick()
	if c == nil {
		return common.Hash{}, errors.New("no healthy sequencer endpoint")
	}
	start := time.Now()
	err := c.RPC.CallContext(ctx, nil, "eth_sendRawTransaction", common.Bytes2Hex(rawTx))
	c.Record(err == nil, time.Since(start))
	if err != nil {
		return common.Hash{}, fmt.Errorf("sequencer send: %w", err)
	}
	return hashRawTx(rawTx), nil
}

// FallbackBroadcaster 主备切换：主发送失败时自动降级到备用端点。
type FallbackBroadcaster struct {
	Primary   Broadcaster
	Secondary Broadcaster
}

func NewFallbackBroadcaster(primary, secondary Broadcaster) *FallbackBroadcaster {
	return &FallbackBroadcaster{Primary: primary, Secondary: secondary}
}

func (b *FallbackBroadcaster) SendRawTransaction(ctx context.Context, rawTx []byte) (common.Hash, error) {
	hash, err := b.Primary.SendRawTransaction(ctx, rawTx)
	if err == nil {
		return hash, nil
	}
	if b.Secondary != nil {
		return b.Secondary.SendRawTransaction(ctx, rawTx)
	}
	return common.Hash{}, err
}

// hashRawTx 计算 raw tx bytes 的 keccak256 作为交易哈希。
func hashRawTx(rawTx []byte) common.Hash {
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(rawTx); err != nil {
		return common.Hash{}
	}
	return tx.Hash()
}
