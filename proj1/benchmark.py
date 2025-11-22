import time
import mysql.connector
from pymongo import MongoClient
from mongodb_queries import query1_mongodb, query2_mongodb, query3_mongodb
import os

# Configurações de conexão
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',  # Ajuste conforme necessário
    'password': 'fkvpqg91',  # Ajuste conforme necessário
    'database': 'tpch'
}

MONGODB_URI = 'mongodb://localhost:27017/'
MONGODB_DB = 'tpch'

def execute_mysql_query(query_file):
    """Executa uma query SQL no MySQL e retorna o tempo de execução"""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    with open(query_file, 'r', encoding='utf-8') as f:
        query = f.read()
    
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()
    
    cursor.close()
    conn.close()
    
    return end_time - start_time, len(results)

def execute_mongodb_query(query_func, db):
    """Executa uma query MongoDB e retorna o tempo de execução"""
    start_time = time.time()
    results = query_func(db)
    end_time = time.time()
    
    return end_time - start_time, len(results)

def generate_report(results):
    """Gera relatório markdown e atualiza README"""
    report_lines = [
        "# NoSQL",
        "",
        "Projetos da disciplina Nosql - Banco De Dados Não Relacionais",
        "",
        "# Benchmark TPC-H (Q1–Q3) — MySQL vs MongoDB",
        "",
        "## Resultados (segundos)",
        "| Query | MySQL | MongoDB | Diferença |",
        "|-------|--------|---------|-----------|"
    ]
    
    for query in ['Q1', 'Q2', 'Q3']:
        mysql_t = results['mysql'].get(query, 0)
        mongo_t = results['mongodb'].get(query, 0)
        diff = mongo_t - mysql_t
        diff_str = f"{diff:+.3f}" if diff != 0 else "0.000"
        report_lines.append(f"| {query}    | {mysql_t:.3f} | {mongo_t:.3f} | {diff_str} |")
    
    report_content = "\n".join(report_lines)
    
    # Salvar no README
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    return report_content

def main():
    print("=" * 60)
    print("BENCHMARK TPC-H: MySQL vs MongoDB")
    print("=" * 60)
    print()
    
    # Conectar ao MongoDB
    print("Conectando ao MongoDB...")
    try:
        mongo_client = MongoClient(MONGODB_URI)
        mongo_db = mongo_client[MONGODB_DB]
        
        # Verificar se o MongoDB tem dados
        if mongo_db['orders'].count_documents({}) == 0:
            print("AVISO: MongoDB parece estar vazio!")
            print("Execute primeiro o script load_mongodb.py para carregar os dados.")
            print()
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return
    
    results = {
        'mysql': {},
        'mongodb': {}
    }
    
    base_dir = os.getcwd()
    queries_dir = os.path.join(base_dir, 'tpch-dbgen', 'queries')
    
    # Query 1
    print("Executando Query 1...")
    print("-" * 60)
    
    try:
        mysql_time, mysql_count = execute_mysql_query(
            os.path.join(queries_dir, '1.sql')
        )
        results['mysql']['Q1'] = mysql_time
        print(f"MySQL Q1:   {mysql_time:.3f}s ({mysql_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MySQL Q1: {e}")
        results['mysql']['Q1'] = 0
    
    try:
        mongo_time, mongo_count = execute_mongodb_query(query1_mongodb, mongo_db)
        results['mongodb']['Q1'] = mongo_time
        print(f"MongoDB Q1: {mongo_time:.3f}s ({mongo_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MongoDB Q1: {e}")
        results['mongodb']['Q1'] = 0
    print()
    
    # Query 2
    print("Executando Query 2...")
    print("-" * 60)
    
    try:
        mysql_time, mysql_count = execute_mysql_query(
            os.path.join(queries_dir, '2.sql')
        )
        results['mysql']['Q2'] = mysql_time
        print(f"MySQL Q2:   {mysql_time:.3f}s ({mysql_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MySQL Q2: {e}")
        results['mysql']['Q2'] = 0
    
    try:
        mongo_time, mongo_count = execute_mongodb_query(query2_mongodb, mongo_db)
        results['mongodb']['Q2'] = mongo_time
        print(f"MongoDB Q2: {mongo_time:.3f}s ({mongo_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MongoDB Q2: {e}")
        results['mongodb']['Q2'] = 0
    print()
    
    # Query 3
    print("Executando Query 3...")
    print("-" * 60)
    
    try:
        mysql_time, mysql_count = execute_mysql_query(
            os.path.join(queries_dir, '3.sql')
        )
        results['mysql']['Q3'] = mysql_time
        print(f"MySQL Q3:   {mysql_time:.3f}s ({mysql_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MySQL Q3: {e}")
        results['mysql']['Q3'] = 0
    
    try:
        mongo_time, mongo_count = execute_mongodb_query(query3_mongodb, mongo_db)
        results['mongodb']['Q3'] = mongo_time
        print(f"MongoDB Q3: {mongo_time:.3f}s ({mongo_count} resultados)")
    except Exception as e:
        print(f"Erro ao executar MongoDB Q3: {e}")
        results['mongodb']['Q3'] = 0
    print()
    
    # Gerar relatório
    print("=" * 60)
    print("RESUMO DOS RESULTADOS")
    print("=" * 60)
    print()
    print("| Query | MySQL (s) | MongoDB (s) | Diferença |")
    print("|-------|-----------|-------------|-----------|")
    
    for query in ['Q1', 'Q2', 'Q3']:
        mysql_t = results['mysql'].get(query, 0)
        mongo_t = results['mongodb'].get(query, 0)
        diff = mongo_t - mysql_t
        diff_str = f"{diff:+.3f}" if diff != 0 else "0.000"
        print(f"| {query}    | {mysql_t:.3f} | {mongo_t:.3f} | {diff_str} |")
    
    print()
    print("Gerando relatório no README.md...")
    generate_report(results)
    print("Relatório salvo com sucesso!")
    
    mongo_client.close()

if __name__ == "__main__":
    main()


