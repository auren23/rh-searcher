package config

import (
	"os"
	"path/filepath"
	"testing"
)

// LoadMerged 必须合并 executor 段（否则模拟器永远不会启用）。
func TestLoadMergedMergesExecutor(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "main.yaml")
	exec := filepath.Join(dir, "exec.yaml")
	os.WriteFile(main, []byte("chain:\n  id: 4663\n"), 0o644)
	os.WriteFile(exec, []byte("executor:\n  contract: 0xabc\n  wallet: 0xdef\n"), 0o644)

	cfg, err := LoadMerged(main, exec)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Executor.Contract != "0xabc" || cfg.Executor.Wallet != "0xdef" {
		t.Errorf("executor not merged: %+v", cfg.Executor)
	}
}

// env 展开：${VAR:-default}
func TestExpandEnvDefault(t *testing.T) {
	os.Setenv("RH_TEST_EXPAND", "")
	got := expand("${RH_TEST_EXPAND:-https://default}")
	if got != "https://default" {
		t.Errorf("default not applied: %q", got)
	}
	os.Setenv("RH_TEST_EXPAND", "https://real")
	got = expand("${RH_TEST_EXPAND:-https://default}")
	if got != "https://real" {
		t.Errorf("env not used: %q", got)
	}
}
