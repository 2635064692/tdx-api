CREATE TABLE IF NOT EXISTS sys_config (
    config_key VARCHAR(100) NOT NULL PRIMARY KEY,
    config_value TEXT NOT NULL,
    remark VARCHAR(200),
    update_ts BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO sys_config (config_key, config_value, remark, update_ts)
VALUES
    ('QUOTE_WS_SUBSCRIBE_CODES', '603*', 'quote storage subscribe code patterns', UNIX_TIMESTAMP(NOW(3)) * 1000),
    ('QUOTE_WS_TRADE_SESSIONS', '09:15-11:30,13:00-15:00', 'quote storage trade sessions', UNIX_TIMESTAMP(NOW(3)) * 1000),
    ('QUOTE_STORAGE_ENABLED', '0', 'quote storage enable switch', UNIX_TIMESTAMP(NOW(3)) * 1000),
    ('QUOTE_STORAGE_SOURCE', 'real', 'quote storage source: real or mock', UNIX_TIMESTAMP(NOW(3)) * 1000),
    ('QUOTE_STORAGE_BATCH_SIZE', '1000', 'quote storage batch size', UNIX_TIMESTAMP(NOW(3)) * 1000),
    ('QUOTE_STORAGE_FLUSH_INTERVAL_MS', '5000', 'quote storage flush interval milliseconds', UNIX_TIMESTAMP(NOW(3)) * 1000)
ON DUPLICATE KEY UPDATE
    config_value = VALUES(config_value),
    remark = VALUES(remark),
    update_ts = VALUES(update_ts);

CREATE TABLE IF NOT EXISTS quote_tick_realtime (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(6) NOT NULL,
    exchange VARCHAR(2) NOT NULL,
    trade_date DATE NOT NULL,
    event_ts BIGINT NOT NULL COMMENT 'quote event timestamp in milliseconds',
    seq BIGINT NOT NULL COMMENT 'global monotonically increasing sequence',
    volume BIGINT NOT NULL,
    amount DOUBLE NOT NULL,
    inside_vol INT NOT NULL,
    outside_vol INT NOT NULL,
    bid1_price INT DEFAULT NULL,
    bid1_vol INT DEFAULT NULL,
    bid2_price INT DEFAULT NULL,
    bid2_vol INT DEFAULT NULL,
    bid3_price INT DEFAULT NULL,
    bid3_vol INT DEFAULT NULL,
    bid4_price INT DEFAULT NULL,
    bid4_vol INT DEFAULT NULL,
    bid5_price INT DEFAULT NULL,
    bid5_vol INT DEFAULT NULL,
    ask1_price INT DEFAULT NULL,
    ask1_vol INT DEFAULT NULL,
    ask2_price INT DEFAULT NULL,
    ask2_vol INT DEFAULT NULL,
    ask3_price INT DEFAULT NULL,
    ask3_vol INT DEFAULT NULL,
    ask4_price INT DEFAULT NULL,
    ask4_vol INT DEFAULT NULL,
    ask5_price INT DEFAULT NULL,
    ask5_vol INT DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_code_event_seq (code, event_ts, seq),
    KEY idx_trade_date (trade_date),
    KEY idx_code_date (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='realtime quote tick storage';

CREATE TABLE IF NOT EXISTS quote_tick_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(6) NOT NULL,
    exchange VARCHAR(2) NOT NULL,
    trade_date DATE NOT NULL,
    volume BIGINT NOT NULL COMMENT 'aggregated volume for the trade date',
    amount DOUBLE NOT NULL COMMENT 'aggregated amount for the trade date',
    inside_vol INT NOT NULL COMMENT 'aggregated inside volume for the trade date',
    outside_vol INT NOT NULL COMMENT 'aggregated outside volume for the trade date',
    quote_ticks TEXT NOT NULL COMMENT 'compressed quote tick payload in JSON',
    archived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_code_trade_date (code, trade_date),
    KEY idx_trade_date (trade_date),
    KEY idx_code_date (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='archived quote tick storage';
