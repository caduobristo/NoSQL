import redis
import datetime
import os

TPCH_PATH = "../tpch-dbgen"

# Conexao com Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def parse_date(d):
    y, m, da = map(int, d.split('-'))
    return datetime.date(y, m, da).toordinal()

def read_tbl(path):
    """Lê arquivo .tbl e já remove o último campo vazio causado pelo '|' final."""
    with open(path, "r") as f:
        for line in f:
            parts = line.strip().split("|")
            if parts[-1] == "":
                parts = parts[:-1]
            yield parts

def load_region():
    print("Carregando region...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "region.tbl")):
        r_regionkey, r_name, r_comment = parts
        key = f"region:{r_regionkey}"

        r.hset(key, mapping={
            "r_regionkey": int(r_regionkey),
            "r_name": r_name,
            "r_comment": r_comment
        })

def load_nation():
    print("Carregando nation...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "nation.tbl")):
        n_nationkey, n_name, n_regionkey, n_comment = parts
        key = f"nation:{n_nationkey}"

        r.hset(key, mapping={
            "n_nationkey": int(n_nationkey),
            "n_name": n_name,
            "n_regionkey": int(n_regionkey),
            "n_comment": n_comment
        })

def load_supplier():
    print("Carregando supplier...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "supplier.tbl")):
        (s_suppkey, s_name, s_address, s_nationkey,
         s_phone, s_acctbal, s_comment) = parts

        key = f"supplier:{s_suppkey}"

        r.hset(key, mapping={
            "s_suppkey": int(s_suppkey),
            "s_name": s_name,
            "s_address": s_address,
            "s_nationkey": int(s_nationkey),
            "s_phone": s_phone,
            "s_acctbal": float(s_acctbal),
            "s_comment": s_comment
        })

def load_part():
    print("Carregando part...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "part.tbl")):
        (p_partkey, p_name, p_mfgr, p_brand, p_type,
         p_size, p_container, p_retailprice, p_comment) = parts

        key = f"part:{p_partkey}"

        r.hset(key, mapping={
            "p_partkey": int(p_partkey),
            "p_name": p_name,
            "p_mfgr": p_mfgr,
            "p_brand": p_brand,
            "p_type": p_type,
            "p_size": int(p_size),
            "p_container": p_container,
            "p_retailprice": float(p_retailprice),
            "p_comment": p_comment
        })

        # Índices úteis
        r.sadd(f"part:size:{p_size}", p_partkey)

        # Para BRASS
        if p_type.endswith("BRASS"):
            r.sadd("part:type:BRASS", p_partkey)

def load_partsupp():
    print("Carregando partsupp...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "partsupp.tbl")):
        (ps_partkey, ps_suppkey, ps_availqty,
         ps_supplycost, ps_comment) = parts

        key = f"partsupp:{ps_partkey}:{ps_suppkey}"

        r.hset(key, mapping={
            "ps_partkey": int(ps_partkey),
            "ps_suppkey": int(ps_suppkey),
            "ps_availqty": int(ps_availqty),
            "ps_supplycost": float(ps_supplycost),
            "ps_comment": ps_comment
        })

        # Índice por partkey
        r.sadd(f"partsupp:by_part:{ps_partkey}", f"{ps_partkey}:{ps_suppkey}")

def load_customer():
    print("Carregando customer...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "customer.tbl")):
        (c_custkey, c_name, c_address, c_nationkey,
         c_phone, c_acctbal, c_mktsegment, c_comment) = parts

        key = f"customer:{c_custkey}"

        r.hset(key, mapping={
            "c_custkey": int(c_custkey),
            "c_name": c_name,
            "c_address": c_address,
            "c_nationkey": int(c_nationkey),
            "c_phone": c_phone,
            "c_acctbal": float(c_acctbal),
            "c_mktsegment": c_mktsegment,
            "c_comment": c_comment
        })

        # Índice por segmento
        r.sadd(f"customer:segment:{c_mktsegment}", c_custkey)

def load_orders():
    print("Carregando orders...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "orders.tbl")):
        (o_orderkey, o_custkey, o_orderstatus, o_totalprice,
         o_orderdate, o_orderpriority, o_clerk,
         o_shippriority, o_comment) = parts

        key = f"orders:{o_orderkey}"

        r.hset(key, mapping={
            "o_orderkey": int(o_orderkey),
            "o_custkey": int(o_custkey),
            "o_orderstatus": o_orderstatus,
            "o_totalprice": float(o_totalprice),
            "o_orderdate": o_orderdate,
            "o_orderpriority": o_orderpriority,
            "o_clerk": o_clerk,
            "o_shippriority": int(o_shippriority),
            "o_comment": o_comment
        })

        # Índice por cliente -> usada na Q3
        r.sadd(f"orders:by_customer:{o_custkey}", o_orderkey)

        # Índice por data
        score = parse_date(o_orderdate)
        r.zadd("orders:by_date", {o_orderkey: score})

def load_lineitem():
    print("Carregando lineitem...")
    for parts in read_tbl(os.path.join(TPCH_PATH, "lineitem.tbl")):
        (l_orderkey, l_partkey, l_suppkey, l_linenumber,
         l_quantity, l_extendedprice, l_discount, l_tax,
         l_returnflag, l_linestatus, l_shipdate, l_commitdate,
         l_receiptdate, l_shipinstruct, l_shipmode, l_comment) = parts

        key = f"lineitem:{l_orderkey}:{l_linenumber}"

        r.hset(key, mapping={
            "l_orderkey": int(l_orderkey),
            "l_linenumber": int(l_linenumber),
            "l_quantity": float(l_quantity),
            "l_extendedprice": float(l_extendedprice),
            "l_discount": float(l_discount),
            "l_tax": float(l_tax),
            "l_returnflag": l_returnflag,
            "l_linestatus": l_linestatus,
            "l_shipdate": l_shipdate
        })

        score = parse_date(l_shipdate)
        r.zadd("lineitem:by_shipdate", {key: score})

load_region()
load_nation()
load_supplier()
load_part()
load_partsupp()
load_customer()
load_orders()
load_lineitem()

print("=== TODOS OS DADOS FORAM CARREGADOS COM SUCESSO ===")
