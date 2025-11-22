import json, os

# mapeamentos de campos por tabela
SCHEMAS = {
    "region":    ["r_regionkey","r_name","r_comment"],
    "nation":    ["n_nationkey","n_name","n_regionkey","n_comment"],
    "supplier":  ["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],
    "part":      ["p_partkey","p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment"],
    "partsupp":  ["ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment"],
    "customer":  ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"],
    "orders":    ["o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"],
    "lineitem":  ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"],
}

def to_int(x):   return int(x) if x != "" else None
def to_float(x): return float(x) if x != "" else None
def to_date(x):
    if not x: return None
    return x

def cast(field, val):
    if val == "": return None
    if field.endswith("key") or field in ("l_linenumber","ps_availqty","p_size","o_shippriority"):
        return to_int(val)
    if field.endswith("acctbal") or field.endswith("price") or field.endswith("discount") or field.endswith("tax") or field.endswith("supplycost") or field == "o_totalprice" or field == "l_quantity":
        return to_float(val)
    if field.endswith("date"):
        return to_date(val)
    return val

def convert(tbl_name, in_path, out_path):
    cols = SCHEMAS[tbl_name]
    with open(in_path, "r", encoding="utf-8", errors="ignore") as fin, open(out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            # remove o último '|', split pelo '|'
            parts = line.rstrip("\n").rstrip("|").split("|")
            if len(parts) != len(cols):
                continue
            doc = { col: cast(col, val) for col, val in zip(cols, parts) }
            fout.write(json.dumps(doc) + "\n")

if __name__ == "__main__":
    base = os.getcwd()
    tables = list(SCHEMAS.keys())
    for t in tables:
        in_file  = os.path.join(base, "tpch-dbgen", f"{t}.tbl")
        out_file = os.path.join(base, "jsons", f"{t}.jsonl")
        if os.path.exists(in_file):
            print(f"{in_file} -> {out_file}")
            convert(t, in_file, out_file)
