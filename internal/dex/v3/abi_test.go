package v3

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

// tickBitmap(int16) 负数 wordPos 的 ABI 编码必须是二补码符号扩展。
func TestTickBitmapWordABI(t *testing.T) {
	int16Type, err := abi.NewType("int16", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	args := abi.Arguments{{Type: int16Type}}
	cases := []struct {
		pos  int16
		want string // 期望的 32 字节 hex（右对齐二补码）
	}{
		{-2, "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"},
		{-1, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{0, "0000000000000000000000000000000000000000000000000000000000000000"},
		{1, "0000000000000000000000000000000000000000000000000000000000000001"},
	}
	for _, c := range cases {
		enc, err := args.Pack(c.pos)
		if err != nil {
			t.Fatalf("pack %d: %v", c.pos, err)
		}
		if got := hex.EncodeToString(enc); got != c.want {
			t.Errorf("pack(%d): got %s want %s", c.pos, got, c.want)
		}
		// 符号位检查：负数必须有 0xff 前缀
		if c.pos < 0 && enc[0] != 0xff {
			t.Errorf("pack(%d): expected 0xff sign extension, got %02x", c.pos, enc[0])
		}
		if c.pos >= 0 && enc[0] != 0x00 {
			t.Errorf("pack(%d): expected zero-padding, got %02x", c.pos, enc[0])
		}
	}
}
