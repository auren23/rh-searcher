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

// LoadMerged 必须合并 arbitrage 段（资金限制与模拟参数）。
func TestLoadMergedMergesArbitrage(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "main.yaml")
	arb := filepath.Join(dir, "arb.yaml")
	os.WriteFile(main, []byte("chain:\n  id: 4663\n"), 0o644)
	os.WriteFile(arb, []byte("arbitrage:\n  max_input_wei: \"30000000000000000\"\n  min_profit_wei: \"10000000000000\"\n  safety_margin_wei: \"5000000000000\"\n  simulation_top_k: 5\n"), 0o644)

	cfg, err := LoadMerged(main, arb)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Arbitrage.MaxInputWei != "30000000000000000" ||
		cfg.Arbitrage.MinProfitWei != "10000000000000" ||
		cfg.Arbitrage.SafetyMarginWei != "5000000000000" ||
		cfg.Arbitrage.SimulationTopK != 5 {
		t.Errorf("arbitrage not merged: %+v", cfg.Arbitrage)
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
