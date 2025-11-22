import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def check_count(prefix):
    """Conta chaves com o padrão prefix:* usando SCAN, sem travar o Redis."""
    cursor = 0
    total = 0
    pattern = f"{prefix}:*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=5000)
        total += len(keys)
        if cursor == 0:
            break
    return total

def print_result(name, value, expected=None):
    if expected is None:
        print(f"[OK] {name}: {value}")
        return

    if value == expected:
        print(f"[OK] {name}: {value} (esperado)")
    else:
        print(f"[!] {name}: {value} (ESPERADO: {expected})")

def exists_prefix(prefix):
    cursor = 0
    pattern = f"{prefix}:*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=5000)
        if len(keys) > 0:
            return True
        if cursor == 0:
            break
    return False

def validate_tpch():
    print("======================================")
    print("    VALIDAÇÃO DO CARREGAMENTO TPC-H   ")
    print("======================================")

    errors = []

    # Region
    region_count = check_count("region")
    print_result("region", region_count, 5)
    if region_count != 5:
        errors.append("region incompleto")

    # Nation
    nation_count = check_count("nation")
    print_result("nation", nation_count, 25)
    if nation_count != 25:
        errors.append("nation incompleto")

    # Supplier
    supplier_count = check_count("supplier")
    print_result("supplier", supplier_count)
    if supplier_count == 0:
        errors.append("supplier vazio")

    # Part
    part_count = check_count("part")
    print_result("part", part_count)
    if part_count == 0:
        errors.append("part vazio")

    # Partsupp
    ps_count = check_count("partsupp")
    print_result("partsupp", ps_count)
    if ps_count == 0:
        errors.append("partsupp vazio")

    # Customer
    cust_count = check_count("customer")
    print_result("customer", cust_count)
    if cust_count == 0:
        errors.append("customer vazio")

    # Orders
    order_count = check_count("orders")
    print_result("orders", order_count)
    if order_count == 0:
        errors.append("orders vazio")

    # Lineitem
    print("\nChecando lineitem (isso pode demorar um pouco)...")
    lineitem_count = check_count("lineitem")
    print_result("lineitem", lineitem_count)
    if lineitem_count == 0:
        errors.append("lineitem vazio")

    # Índice lineitem:by_shipdate
    print("\nÍndices importantes:")
    shipdate_zset = r.zcard("lineitem:by_shipdate")
    print_result("lineitem:by_shipdate (ZSET)", shipdate_zset)
    if shipdate_zset == 0:
        errors.append("índice lineitem:by_shipdate vazio")

    # Índice part:size:15
    size15_count = r.scard("part:size:15")
    print_result("part:size:15", size15_count)
    if size15_count == 0:
        errors.append("índice part:size:15 vazio")

    # Índice part:type:BRASS
    brass_count = r.scard("part:type:BRASS")
    print_result("part:type:BRASS", brass_count)
    if brass_count == 0:
        errors.append("índice part:type:BRASS vazio")

    # Índice orders:by_customer
    print("\nVerificando índice orders:by_customer (amostra)...")
    if exists_prefix("orders:by_customer"):
        print("[OK] Índice orders:by_customer encontrado")
    else:
        print("[!] Índice orders:by_customer não encontrado")
        errors.append("orders:by_customer vazio")

    # Índice customer:segment:BUILDING
    segment_count = r.scard("customer:segment:BUILDING")
    print_result("customer:segment:BUILDING", segment_count)
    if segment_count == 0:
        errors.append("customer:segment:BUILDING vazio")

    # Índice orders:by_date
    print("\nVerificando índice orders:by_date...")
    orders_by_date = r.zcard("orders:by_date")
    print_result("orders:by_date", orders_by_date)
    if orders_by_date == 0:
        errors.append("orders:by_date vazio")

    print("\n======================================")
    print("              RESULTADO               ")
    print("======================================")
    if not errors:
        print("Todos os dados e índices foram carregados corretamente!")
    else:
        print("Problemas encontrados:")
        for e in errors:
            print(" -", e)

    print("======================================")

if __name__ == "__main__":
    validate_tpch()
