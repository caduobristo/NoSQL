import redis
import datetime
import time

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def parse_date_str_to_date(d: str) -> datetime.date:
    y, m, da = map(int, d.split("-"))
    return datetime.date(y, m, da)

def parse_date_str_to_ordinal(d: str) -> int:
    return parse_date_str_to_date(d).toordinal()

# =========================================================
# Q1 - Pricing Summary Report (apenas lineitem)
# =========================================================

def query1_redis():
    """
    Equivalente à Q1 do TPC-H:
    - Filtra l_shipdate <= '1998-12-01' - 90 dias
    - Agrupa por (l_returnflag, l_linestatus)
    - Calcula SUM, AVG, COUNT
    """
    # Data limite: 1998-12-01 - 90 dias
    base_date = datetime.date(1998, 12, 1)
    cutoff_date = base_date - datetime.timedelta(days=90)
    cutoff_score = cutoff_date.toordinal()

    print("== Q1 Redis ==")
    print("Data limite:", cutoff_date)

    start = time.perf_counter()

    # Recupera chaves de lineitem com l_shipdate <= cutoff_date
    keys = r.zrangebyscore("lineitem:by_shipdate", "-inf", cutoff_score)

    groups = {}  # (l_returnflag, l_linestatus) -> agregados

    for key in keys:
        item = r.hgetall(key)
        if not item:
            continue

        rf = item["l_returnflag"]
        ls = item["l_linestatus"]
        grp_key = (rf, ls)

        qty = float(item["l_quantity"])
        ext = float(item["l_extendedprice"])
        disc = float(item["l_discount"])
        tax = float(item["l_tax"])

        if grp_key not in groups:
            groups[grp_key] = {
                "sum_qty": 0.0,
                "sum_base_price": 0.0,
                "sum_disc_price": 0.0,
                "sum_charge": 0.0,
                "sum_disc": 0.0,
                "count": 0
            }

        g = groups[grp_key]
        g["sum_qty"] += qty
        g["sum_base_price"] += ext
        g["sum_disc_price"] += ext * (1 - disc)
        g["sum_charge"] += ext * (1 - disc) * (1 + tax)
        g["sum_disc"] += disc
        g["count"] += 1

    # Montar resultado final com médias
    result_rows = []
    for (rf, ls), g in groups.items():
        cnt = g["count"] if g["count"] > 0 else 1
        row = {
            "l_returnflag": rf,
            "l_linestatus": ls,
            "sum_qty": g["sum_qty"],
            "sum_base_price": g["sum_base_price"],
            "sum_disc_price": g["sum_disc_price"],
            "sum_charge": g["sum_charge"],
            "avg_qty": g["sum_qty"] / cnt,
            "avg_price": g["sum_base_price"] / cnt,
            "avg_disc": g["sum_disc"] / cnt,
            "count_order": g["count"]
        }
        result_rows.append(row)

    # Ordenação igual à Q1: por returnflag, linestatus
    result_rows.sort(key=lambda r: (r["l_returnflag"], r["l_linestatus"]))

    end = time.perf_counter()
    elapsed = end - start

    # Mostrar algumas linhas (opcional)
    for row in result_rows:
        print(row)

    print(f"Tempo Q1 (Redis): {elapsed:.3f} s\n")
    return elapsed, result_rows

# =========================================================
# Q2 - Minimum Cost Supplier
# =========================================================

def get_region_name_by_nationkey(nationkey: int) -> str:
    """
    nation:n -> n_regionkey -> region:r -> r_name
    """
    nation = r.hgetall(f"nation:{nationkey}")
    if not nation:
        return None
    regionkey = nation.get("n_regionkey")
    if regionkey is None:
        return None
    region = r.hgetall(f"region:{regionkey}")
    if not region:
        return None
    return region.get("r_name")

def query2_redis():
    """
    Equivalente à Q2 do TPC-H:
    - part, supplier, partsupp, nation, region
    - Filtro: p_size = 15, p_type LIKE '%BRASS', região 'EUROPE'
    - ps_supplycost = mínimo por partkey
    """

    print("== Q2 Redis ==")

    start = time.perf_counter()

    # 1) Pegar todas as partes com p_size = 15 e p_type LIKE '%BRASS'
    # Conjuntos construídos no loader:
    #   part:size:15
    #   part:type:BRASS
    partkeys_size_15 = r.smembers("part:size:15")
    partkeys_brass = r.smembers("part:type:BRASS")

    # interseção em Python (poderia usar SINTER também)
    partkeys = set(partkeys_size_15).intersection(partkeys_brass)

    rows = []

    for p_partkey in partkeys:
        # Carregar dados da part
        part = r.hgetall(f"part:{p_partkey}")
        if not part:
            continue

        p_mfgr = part["p_mfgr"]

        # 2) Para cada partkey, obter todos os suppliers via partsupp:by_part:<p_partkey>
        ps_keys = r.smembers(f"partsupp:by_part:{p_partkey}")
        # ps_keys contem strings tipo "ps_partkey:ps_suppkey"
        candidates = []

        for ps_key in ps_keys:
            try:
                ps_partkey_str, ps_suppkey_str = ps_key.split(":")
            except ValueError:
                # caso algo estranho esteja na chave
                continue

            ps_partkey = int(ps_partkey_str)
            ps_suppkey = int(ps_suppkey_str)

            partsupp = r.hgetall(f"partsupp:{ps_partkey}:{ps_suppkey}")
            if not partsupp:
                continue

            ps_supplycost = float(partsupp["ps_supplycost"])

            supplier = r.hgetall(f"supplier:{ps_suppkey}")
            if not supplier:
                continue

            s_acctbal = float(supplier["s_acctbal"])
            s_name = supplier["s_name"]
            s_address = supplier["s_address"]
            s_phone = supplier["s_phone"]
            s_comment = supplier["s_comment"]
            s_nationkey = int(supplier["s_nationkey"])

            r_name = get_region_name_by_nationkey(s_nationkey)
            if r_name != "EUROPE":
                continue

            # Aqui também poderíamos obter n_name:
            nation = r.hgetall(f"nation:{s_nationkey}")
            n_name = nation.get("n_name", "") if nation else ""

            candidates.append({
                "ps_supplycost": ps_supplycost,
                "s_acctbal": s_acctbal,
                "s_name": s_name,
                "n_name": n_name,
                "p_partkey": int(p_partkey),
                "p_mfgr": p_mfgr,
                "s_address": s_address,
                "s_phone": s_phone,
                "s_comment": s_comment
            })

        if not candidates:
            continue

        # 3) Encontrar o(s) supplier(s) com menor ps_supplycost para este partkey
        min_cost = min(c["ps_supplycost"] for c in candidates)
        for c in candidates:
            if c["ps_supplycost"] == min_cost:
                rows.append(c)

    # 4) Ordenação igual à Q2:
    # ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
    rows.sort(key=lambda r: (-r["s_acctbal"], r["n_name"], r["s_name"], r["p_partkey"]))

    end = time.perf_counter()
    elapsed = end - start

    # Mostrar algumas linhas (opcional)
    for row in rows[:20]:
        print(row)

    print(f"Tempo Q2 (Redis): {elapsed:.3f} s\n")
    return elapsed, rows

# =========================================================
# Q3 - Shipping Priority
# =========================================================

def query3_redis():
    """
    Equivalente à Q3 do TPC-H:
    - Tabelas: customer, orders, lineitem
    - Filtros:
      c_mktsegment = 'BUILDING'
      o_orderdate < '1995-03-15'
      l_shipdate > '1995-03-15'
    - Agrupa por (l_orderkey, o_orderdate, o_shippriority)
      e soma revenue = SUM(l_extendedprice * (1 - l_discount))
    """

    print("== Q3 Redis ==")

    cutoff_str = "1995-03-15"
    cutoff_date = parse_date_str_to_date(cutoff_str)

    start = time.perf_counter()

    # 1) Clientes do segmento BUILDING
    custkeys = r.smembers("customer:segment:BUILDING")

    # Resultado por (orderkey, orderdate, shippriority)
    groups = {}  # (orderkey, orderdate, shippriority) -> revenue

    for c_custkey in custkeys:
        # 2) Pedidos desse cliente
        orderkeys = r.smembers(f"orders:by_customer:{c_custkey}")

        for o_orderkey in orderkeys:
            order = r.hgetall(f"orders:{o_orderkey}")
            if not order:
                continue

            o_orderdate_str = order["o_orderdate"]
            o_orderdate = parse_date_str_to_date(o_orderdate_str)

            # Filtro de data: o_orderdate < '1995-03-15'
            if not (o_orderdate < cutoff_date):
                continue

            o_shippriority = int(order["o_shippriority"])

            # 3) Buscar itens de lineitem desse pedido
            #    usando padrão lineitem:<orderkey>:*
            pattern = f"lineitem:{o_orderkey}:*"
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                for key in keys:
                    item = r.hgetall(key)
                    if not item:
                        continue

                    l_shipdate_str = item["l_shipdate"]
                    l_shipdate = parse_date_str_to_date(l_shipdate_str)

                    # Filtro l_shipdate > '1995-03-15'
                    if not (l_shipdate > cutoff_date):
                        continue

                    l_extendedprice = float(item["l_extendedprice"])
                    l_discount = float(item["l_discount"])

                    revenue = l_extendedprice * (1 - l_discount)

                    grp_key = (int(o_orderkey), o_orderdate_str, o_shippriority)
                    groups[grp_key] = groups.get(grp_key, 0.0) + revenue

                if cursor == 0:
                    break

    # Transformar em lista de linhas
    rows = []
    for (orderkey, orderdate_str, shippriority), revenue in groups.items():
        rows.append({
            "l_orderkey": orderkey,
            "revenue": revenue,
            "o_orderdate": orderdate_str,
            "o_shippriority": shippriority
        })

    # Ordenação: revenue DESC, o_orderdate ASC
    rows.sort(key=lambda r: (-r["revenue"], r["o_orderdate"]))

    end = time.perf_counter()
    elapsed = end - start

    for row in rows[:20]:
        print(row)

    print(f"Tempo Q3 (Redis): {elapsed:.3f} s\n")
    return elapsed, rows

# =========================================================
# MAIN - para rodar as três e já ver os tempos
# =========================================================

if __name__ == "__main__":
    t1, _ = query1_redis()
    t2, _ = query2_redis()
    t3, _ = query3_redis()

    print("Resumo dos tempos (s):")
    print(f"Q1 (Redis): {t1:.3f}")
    print(f"Q2 (Redis): {t2:.3f}")
    print(f"Q3 (Redis): {t3:.3f}")
