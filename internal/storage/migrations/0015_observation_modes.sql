-- 0015_observation_modes.sql
-- 零成本观察模式的字段：
--   simulation_mode: local_only | latest_observe | historical_strict
--   state_quality:   historical | latest_consistent | latest_mixed_state | local
--   state_age_ms:    快照/评估时的状态年龄（head 滞后）
--   analysis_selected: 研究用组内最佳（local/latest 模式），与 live selected 分离
ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS simulation_mode TEXT,
    ADD COLUMN IF NOT EXISTS state_quality TEXT,
    ADD COLUMN IF NOT EXISTS state_age_ms BIGINT,
    ADD COLUMN IF NOT EXISTS analysis_selected BOOLEAN NOT NULL DEFAULT FALSE;
