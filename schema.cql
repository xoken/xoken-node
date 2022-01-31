CREATE KEYSPACE xoken WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE xoken.misc_store (
    key text PRIMARY KEY,
    value frozen<tuple<boolean, int, bigint, text>>
);

CREATE TABLE xoken.transactions (
    tx_id text PRIMARY KEY,
    block_info frozen<tuple<text, int, int>>,
    tx_serialized blob,
    inputs set  <frozen<tuple<frozen<tuple<text, int>>, int, frozen<tuple<text, bigint>>>>>,
    fees bigint
);

CREATE TABLE xoken.blocks_by_hash (
    block_hash text PRIMARY KEY,
    block_header text,
    next_block_hash text,
    block_height int,
    block_size int,
    tx_count int,
    coinbase_tx blob
);

CREATE TABLE xoken.ep_transactions (
    epoch boolean,
    tx_id text,
    tx_serialized blob,
    inputs set <frozen<tuple<frozen<tuple<text, int>>, int, frozen<tuple<text, bigint>>>>>,
    fees bigint,
    PRIMARY KEY (epoch, tx_id)
);

CREATE TABLE xoken.ep_script_hash_outputs (
    epoch boolean,
    script_hash text,
    nominal_tx_index bigint,
    output frozen<tuple<text, int>>,
    PRIMARY KEY (epoch, script_hash, nominal_tx_index, output)
) WITH CLUSTERING ORDER BY (script_hash ASC, nominal_tx_index DESC, output ASC);

CREATE TABLE xoken.ep_txid_outputs (
    epoch boolean,
    txid text,
    output_index int,
    address text,
    script text,
    spend_info frozen<tuple<text, int>>,
    value bigint,
    PRIMARY KEY(epoch, txid, output_index)
 ) WITH CLUSTERING ORDER BY (txid ASC, output_index ASC);

CREATE TABLE xoken.script_hash_outputs (
    script_hash text,
    nominal_tx_index bigint,
    output frozen<tuple<text, int>>,
    PRIMARY KEY (script_hash, nominal_tx_index, output)
) WITH CLUSTERING ORDER BY (nominal_tx_index DESC);

CREATE TABLE xoken.txid_outputs (
    txid text,
    output_index int,
    address text,
    script text,
    spend_info frozen<tuple<text, int>>,
    value bigint,
    PRIMARY KEY(txid, output_index)
) WITH CLUSTERING ORDER BY (output_index asc);

CREATE TABLE xoken.blocks_by_height (
    block_height int PRIMARY KEY,
    block_hash text,
    next_block_hash text,
    block_header text,
    block_size int,
    tx_count int,
    coinbase_tx blob
);

CREATE TABLE xoken.user_permission (
    username text PRIMARY KEY,
    password text,
    first_name text,
    last_name text,
    emailid text,
    created_time timestamp,
    permissions set <text>,
    api_quota int,
    api_used int,
    api_expiry_time timestamp,
    session_key text,
    session_key_expiry_time timestamp
);

CREATE TABLE xoken.blockhash_txids (
    block_hash text,
    page_number int,
    txids frozen<list<text>>,
    PRIMARY KEY((block_hash, page_number))
);

CREATE TABLE xoken.script_output_protocol (
    proto_str text,
    txid text,
    fees bigint,
    size int,
    output_index int,
    nominal_tx_index bigint,
    PRIMARY KEY (proto_str, nominal_tx_index)
) WITH CLUSTERING ORDER BY (nominal_tx_index ASC);

CREATE TABLE xoken.ep_script_output_protocol (
    epoch boolean,
    proto_str text,
    txid text,
    fees bigint,
    size int,
    output_index int,
    nominal_tx_index bigint,
    PRIMARY KEY (epoch, proto_str, nominal_tx_index)
) WITH CLUSTERING ORDER BY (proto_str ASC, nominal_tx_index DESC);
