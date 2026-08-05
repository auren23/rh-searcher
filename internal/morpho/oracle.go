package morpho

import "math/big"

// Oracle 接口：价格来源。MVP 支持链上 oracle 直读，M6 接入真实合约。
type Oracle interface {
	// Price 返回 asset 的 oracle 价格。
	Price(asset string) (*big.Int, error)
}
