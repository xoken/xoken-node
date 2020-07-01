CREATE KEYSPACE xoken WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE xoken.misc_store (
    key text PRIMARY KEY,
    value frozen<tuple<boolean, int, bigint, text>>
); 

CREATE TABLE xoken.transactions (
    tx_id text PRIMARY KEY,
    block_info frozen<tuple<frozen<tuple<text, int>>, int>>,
    tx_serialized blob
);

CREATE TABLE xoken.blocks_by_hash (
    block_hash text PRIMARY KEY,
    block_header text,
    block_height int,
    block_size int,
    tx_count int,
    coinbase_tx blob
); 

CREATE TABLE xoken.ep_address_outputs (
    epoch boolean,
    address text,
    output frozen<tuple<text, int>>,
    is_output_spent boolean,
    is_type_receive boolean,
    other_address text,
    prev_outpoint frozen<tuple<text, int>>,
    value bigint,
    PRIMARY KEY (epoch, address, output)
);

CREATE TABLE xoken.address_outputs (
    address text,
    nominal_tx_index bigint,
    output frozen<tuple<text, int>>,
    is_type_receive boolean,
    other_address text,
    PRIMARY KEY (address, nominal_tx_index, output)
) WITH CLUSTERING ORDER BY (nominal_tx_index DESC);

CREATE TABLE xoken.blocks_by_height (
    block_height int PRIMARY KEY,
    block_hash text,
    block_header text,
    block_size int,
    tx_count int,
    coinbase_tx blob
); 

CREATE TABLE xoken.ep_transactions (
    epoch boolean,
    tx_id text,
    tx_serialized blob,
    PRIMARY KEY (epoch, tx_id)
); 

CREATE TABLE xoken.txid_outputs (
    txid text,
    idx int,
    block_info frozen<tuple<frozen<tuple<text, int>>, int>>,
    is_output_spent boolean,
    spending_txid text,
    spending_index int,
    spending_tx_block_height int,
    prev_outpoint frozen<tuple<text, int>>,
    input_index int,
    value bigint,
    PRIMARY KEY(txid, idx)
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
    txids set <text>,
    PRIMARY KEY(block_hash, page_number)
) WITH CLUSTERING ORDER BY (page_number ASC);
