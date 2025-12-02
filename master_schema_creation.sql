-- SCHEMA CREATION

-- Sessions: Create table
CREATE TABLE sessions (
    user_id BIGINT NOT NULL,
    session_id BIGINT NOT NULL PRIMARY KEY,
    session_start TIMESTAMP NOT NULL,
    channel VARCHAR(50),
    device VARCHAR(50),
    UNIQUE(user_id, session_id)
);

-- Sessions: Create Indexes
CREATE INDEX idx_sessions_start ON sessions (session_start);
CREATE INDEX idx_sessions_channel ON sessions (channel);
CREATE INDEX idx_sessions_device ON sessions (device);
CREATE INDEX idx_sessions_user ON sessions (user_id);

CREATE INDEX idx_sessions_start_channel ON sessions(session_start, channel);
CREATE INDEX idx_sessions_start_device ON sessions(session_start, device);





-- Pageviews: Create table
CREATE TABLE pageviews (
    session_id BIGINT REFERENCES sessions(session_id),
    pageview_id BIGINT PRIMARY KEY,
    pv_timestamp TIMESTAMP NOT NULL,
    referer VARCHAR(2048),
    url VARCHAR(2048),
    PAGETYPE VARCHAR(50)
);

-- Pageviews: Create Indexes
CREATE INDEX idx_pv_session ON pageviews (session_id);
CREATE INDEX idx_pv_timestamp ON pageviews (pv_timestamp);
CREATE INDEX idx_pv_url ON pageviews (url);
CREATE INDEX idx_pv_pt ON pageviews (PAGETYPE);



-- Transactions: Create Table
CREATE TABLE transactions (
    transaction_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    session_id BIGINT NOT NULL,
    pageview_id BIGINT REFERENCES pageviews(pageview_id),
    t_timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id, session_id) REFERENCES sessions(user_id, session_id),
    PRIMARY KEY (transaction_id,t_timestamp)
)PARTITION BY RANGE (t_timestamp);


-- Transactions: Function to create monthly partitions
CREATE OR REPLACE FUNCTION create_monthly_partition(
    base_table TEXT,
    start_date DATE,
    end_date DATE
)
RETURNS VOID AS $$
DECLARE
    curr DATE := date_trunc('month', start_date);
BEGIN
    WHILE curr < end_date LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I_%s PARTITION OF %I
             FOR VALUES FROM (%L) TO (%L);',
            base_table,
            to_char(curr, 'YYYYMM'),
            base_table,
            curr,
            curr + INTERVAL '1 month'
        );

        curr := curr + INTERVAL '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT create_monthly_partition('transactions', '2019-01-01', '2026-01-01');


-- Transactions: Create Indexes
CREATE INDEX idx_tx_timestamp ON transactions (t_timestamp);
CREATE INDEX idx_tx_session ON transactions (session_id);
CREATE INDEX idx_tx_user ON transactions (user_id);
CREATE INDEX idx_tx_pageview ON transactions (pageview_id);




-- Event Name Mapping - Create Table
CREATE TABLE event_name_mapping(
    event_type_id INT PRIMARY KEY,
    event_name VARCHAR(50) UNIQUE
);




-- Events - Create table
CREATE TABLE events (
    session_id BIGINT REFERENCES sessions(session_id),
    pageview_id BIGINT REFERENCES pageviews(pageview_id),
    event_id BIGINT NOT NULL,
    event_type_id INT NOT NULL REFERENCES event_name_mapping(event_type_id),
    event_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, event_type_id),
    UNIQUE (event_id, event_type_id)
) PARTITION BY LIST (event_type_id);


-- Events - Partition by event_type_id to easily get the aggregations for each event
CREATE TABLE events_type_1 PARTITION OF events FOR VALUES IN (1);
CREATE TABLE events_type_2 PARTITION OF events FOR VALUES IN (2);
CREATE TABLE events_type_3 PARTITION OF events FOR VALUES IN (3);
CREATE TABLE events_type_4 PARTITION OF events FOR VALUES IN (4);
CREATE TABLE events_type_5 PARTITION OF events FOR VALUES IN (5);
CREATE TABLE events_type_6 PARTITION OF events FOR VALUES IN (6);

-- Events - Create Indexes
CREATE INDEX idx_ev_session ON events (session_id);
CREATE INDEX idx_ev_pageview ON events (pageview_id);
CREATE INDEX idx_ev_timestamp ON events (event_timestamp);
CREATE INDEX idx_ev_type ON events (event_type_id);
CREATE INDEX ON events_type_5 (event_timestamp);
CREATE INDEX ON events_type_5 (pageview_id);
CREATE INDEX ON events_type_5 (session_id);

CREATE INDEX idx_events_timestamp_session_pageview ON events(event_timestamp, session_id, pageview_id);



-- Attributes table - Create Table
CREATE TABLE event_attributes (
    event_id BIGINT NOT NULL,
    event_type_id INT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    currency CHAR(3) DEFAULT 'USD' NOT NULL,
    PRIMARY KEY (event_id, sku, event_type_id),
    -- Reference the composite PRIMARY KEY
    FOREIGN KEY (event_id, event_type_id) REFERENCES events(event_id, event_type_id)
)PARTITION BY LIST (event_type_id)
;

-- Event_Attributes - Partition by event_type_id to easily get the attributes for each event
CREATE TABLE events_att_type_3 PARTITION OF event_attributes FOR VALUES IN (3);
CREATE TABLE events_att_type_4 PARTITION OF event_attributes FOR VALUES IN (4);
CREATE TABLE events_att_type_5 PARTITION OF event_attributes FOR VALUES IN (5);
CREATE TABLE events_att_type_6 PARTITION OF event_attributes FOR VALUES IN (6);

-- Attributes table - Create Indexes
CREATE INDEX idx_attr_eventid ON event_attributes (event_id, event_type_id);
CREATE INDEX idx_attr_sku ON event_attributes (sku);
CREATE INDEX ON events_att_type_5 (event_id);
CREATE INDEX ON events_att_type_5 (sku);
CREATE INDEX ON events_att_type_4 (event_id);
CREATE INDEX ON events_att_type_4 (sku);


