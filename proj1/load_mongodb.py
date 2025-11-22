import json
import os
from pymongo import MongoClient
from datetime import datetime

def load_json_to_mongodb(json_file, collection):
    """Carrega um arquivo JSONL para uma collection do MongoDB"""
    count = 0
    with open(json_file, 'r', encoding='utf-8') as f:
        docs = []
        for line in f:
            if line.strip():
                doc = json.loads(line)
                docs.append(doc)
                count += 1
                # Inserir em lotes de 1000
                if len(docs) >= 1000:
                    collection.insert_many(docs)
                    docs = []
        if docs:
            collection.insert_many(docs)
    return count

def denormalize_data(client, db):
    """Denormaliza dados para otimizar queries"""
    print("Denormalizando dados...")
    
    # Carregar dados básicos primeiro
    region_col = db['region']
    nation_col = db['nation']
    supplier_col = db['supplier']
    customer_col = db['customer']
    part_col = db['part']
    orders_col = db['orders']
    lineitem_col = db['lineitem']
    partsupp_col = db['partsupp']
    
    # Criar índices para melhor performance
    print("Criando índices...")
    lineitem_col.create_index("l_shipdate")
    lineitem_col.create_index("l_orderkey")
    lineitem_col.create_index("l_returnflag")
    lineitem_col.create_index("l_linestatus")
    orders_col.create_index("o_orderkey")
    orders_col.create_index("o_custkey")
    orders_col.create_index("o_orderdate")
    customer_col.create_index("c_custkey")
    customer_col.create_index("c_mktsegment")
    part_col.create_index("p_partkey")
    part_col.create_index("p_size")
    part_col.create_index("p_type")
    supplier_col.create_index("s_suppkey")
    supplier_col.create_index("s_nationkey")
    nation_col.create_index("n_nationkey")
    nation_col.create_index("n_regionkey")
    region_col.create_index("r_regionkey")
    region_col.create_index("r_name")
    partsupp_col.create_index([("ps_partkey", 1), ("ps_suppkey", 1)])
    
    # Embedar nation em supplier e customer
    print("Embedando nation em supplier e customer...")
    nation_dict = {n['n_nationkey']: n for n in nation_col.find()}
    region_dict = {r['r_regionkey']: r for r in region_col.find()}
    
    # Adicionar nation e region aos suppliers
    for supplier in supplier_col.find():
        nation = nation_dict.get(supplier['s_nationkey'])
        if nation:
            nation_copy = nation.copy()
            region = region_dict.get(nation['n_regionkey'])
            if region:
                nation_copy['region'] = region
            supplier_col.update_one(
                {'s_suppkey': supplier['s_suppkey']},
                {'$set': {'nation': nation_copy}}
            )
    
    # Adicionar nation e region aos customers
    for customer in customer_col.find():
        nation = nation_dict.get(customer['c_nationkey'])
        if nation:
            nation_copy = nation.copy()
            region = region_dict.get(nation['n_regionkey'])
            if region:
                nation_copy['region'] = region
            customer_col.update_one(
                {'c_custkey': customer['c_custkey']},
                {'$set': {'nation': nation_copy}}
            )
    
    # Embedar lineitems em orders (útil para Q1 e Q3)
    print("Embedando lineitems em orders...")
    for order in orders_col.find():
        lineitems = list(lineitem_col.find({'l_orderkey': order['o_orderkey']}))
        if lineitems:
            orders_col.update_one(
                {'o_orderkey': order['o_orderkey']},
                {'$set': {'lineitems': lineitems}}
            )
    
    # Embedar partsupp em part (útil para Q2)
    print("Embedando partsupp em part...")
    for part in part_col.find():
        partsupps = list(partsupp_col.find({'ps_partkey': part['p_partkey']}))
        if partsupps:
            # Adicionar supplier info a cada partsupp
            enriched_partsupps = []
            for ps in partsupps:
                supplier = supplier_col.find_one({'s_suppkey': ps['ps_suppkey']})
                if supplier:
                    ps_copy = ps.copy()
                    ps_copy['supplier'] = supplier
                    enriched_partsupps.append(ps_copy)
                else:
                    enriched_partsupps.append(ps)
            part_col.update_one(
                {'p_partkey': part['p_partkey']},
                {'$set': {'partsupps': enriched_partsupps}}
            )
    
    print("Denormalização concluída!")

def main():
    # Configuração de conexão
    client = MongoClient('mongodb://localhost:27017/')
    db = client['tpch']
    
    # Limpar collections existentes
    print("Limpando collections existentes...")
    db.drop_collection('region')
    db.drop_collection('nation')
    db.drop_collection('supplier')
    db.drop_collection('customer')
    db.drop_collection('part')
    db.drop_collection('partsupp')
    db.drop_collection('orders')
    db.drop_collection('lineitem')
    
    base_dir = os.getcwd()
    jsons_dir = os.path.join(base_dir, 'jsons')
    
    if not os.path.exists(jsons_dir):
        print(f"Erro: Diretório {jsons_dir} não encontrado!")
        print("Execute primeiro o script tpch_to_json.py para gerar os arquivos JSON.")
        return
    
    tables = ['region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem']
    
    # Carregar dados básicos
    print("Carregando dados básicos...")
    for table in tables:
        json_file = os.path.join(jsons_dir, f'{table}.jsonl')
        if os.path.exists(json_file):
            print(f"Carregando {table}...")
            collection = db[table]
            count = load_json_to_mongodb(json_file, collection)
            print(f"  {count} documentos inseridos em {table}")
        else:
            print(f"  Aviso: {json_file} não encontrado!")
    
    # Denormalizar dados
    denormalize_data(client, db)
    
    print("\nCarregamento concluído!")
    print(f"Database: {db.name}")
    print(f"Collections: {db.list_collection_names()}")

if __name__ == "__main__":
    main()


