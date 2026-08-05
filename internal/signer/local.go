package signer

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// LocalSigner 本地私钥签名器。私钥从环境变量读取，绝不落盘/入库/进日志。
type LocalSigner struct {
	key     *ecdsa.PrivateKey
	addr    common.Address
	chainID *big.Int
}

// NewLocalSigner 从 hex 私钥（0x 前缀可选）构造签名器。
func NewLocalSigner(privHex string, chainID *big.Int) (*LocalSigner, error) {
	hex := privHex
	if len(hex) >= 2 && hex[:2] == "0x" {
		hex = hex[2:]
	}
	key, err := crypto.HexToECDSA(hex)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return &LocalSigner{
		key:     key,
		addr:    crypto.PubkeyToAddress(key.PublicKey),
		chainID: chainID,
	}, nil
}

func (s *LocalSigner) Address() common.Address { return s.addr }

func (s *LocalSigner) SignTx(tx *types.Transaction) (*types.Transaction, error) {
	return types.SignTx(tx, types.LatestSignerForChainID(s.chainID), s.key)
}
