CREATE CONSTRAINT ON ( mnode:mnode ) ASSERT mnode.v IS UNIQUE;

CREATE CONSTRAINT ON ( namestate:namestate ) ASSERT namestate.name IS UNIQUE;

CREATE CONSTRAINT ON ( nutxo:nutxo ) ASSERT nutxo.outpoint IS UNIQUE;

CREATE CONSTRAINT ON (block: block) ASSERT block.hash IS UNIQUE;

CREATE CONSTRAINT ON (protocol: protocol) ASSERT (protocol.name) IS UNIQUE;
