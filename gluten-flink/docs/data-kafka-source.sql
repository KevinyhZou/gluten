
CREATE TABLE src_json_Tbl (
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
 ) WITH (
    'connector'='kafka',
    'topic' = '*****',
    'properties.bootstrap.servers' = '*****'
    'properties.group.id' = '*****',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
 );

 create table snk_json_Tbl(
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
    ) with('connector' = 'print');

 insert into snk_json_Tbl select a,b,c,d,f,g,h,e,r,y,m from src_json_Tbl;

-- The test data as below
-- {"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":"2025-05-06 11:30:00", "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e131"}}


==============================================================================================================================
CREATE TABLE kafka (
    event_type int,
    person ROW<
        id  BIGINT,
        name  VARCHAR,
        emailAddress  VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        `dateTime` TIMESTAMP(3),
        extra  VARCHAR>,
    auction ROW<
        id  BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        `dateTime`  TIMESTAMP(3),
        expires  TIMESTAMP(3),
        seller  BIGINT,
        category  BIGINT,
        extra  VARCHAR>,
    bid ROW<
        auction  BIGINT,
        bidder  BIGINT,
        price  BIGINT,
        channel  VARCHAR,
        url  VARCHAR,
        `dateTime`  TIMESTAMP(3),
        extra  VARCHAR>,
    `dateTime` AS
        CASE
            WHEN event_type = 0 THEN person.`dateTime`
            WHEN event_type = 1 THEN auction.`dateTime`
            ELSE bid.`dateTime`
        END,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'test_in_1',
    'properties.bootstrap.servers' = '10.152.38.33:9098',
    'properties.group.id' = 'nexmark',
    'scan.startup.mode' = 'latest-offset',
    'sink.partitioner' = 'round-robin',
    'format' = 'json'
);

CREATE VIEW bid AS
SELECT
    bid.auction,
    bid.bidder,
    bid.price,
    bid.channel,
    bid.url,
    `dateTime`,
    bid.extra
FROM kafka WHERE event_type = 2;

CREATE TABLE nexmark_q0 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q0 SELECT auction, bidder, price, `dateTime`, extra FROM bid;
