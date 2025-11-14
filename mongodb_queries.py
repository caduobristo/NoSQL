from pymongo import MongoClient
from datetime import datetime, timedelta

def query1_mongodb(db):
    """Q1: Pricing Summary Report Query"""
    lineitem_col = db['lineitem']
    
    # Calcular data limite: 1998-12-01 - 90 dias
    date_limit = datetime(1998, 12, 1) - timedelta(days=90)
    
    pipeline = [
        {
            '$match': {
                'l_shipdate': {'$lte': date_limit.strftime('%Y-%m-%d')}
            }
        },
        {
            '$group': {
                '_id': {
                    'returnflag': '$l_returnflag',
                    'linestatus': '$l_linestatus'
                },
                'sum_qty': {'$sum': '$l_quantity'},
                'sum_base_price': {'$sum': '$l_extendedprice'},
                'sum_disc_price': {
                    '$sum': {
                        '$multiply': [
                            '$l_extendedprice',
                            {'$subtract': [1, '$l_discount']}
                        ]
                    }
                },
                'sum_charge': {
                    '$sum': {
                        '$multiply': [
                            {'$multiply': [
                                '$l_extendedprice',
                                {'$subtract': [1, '$l_discount']}
                            ]},
                            {'$add': [1, '$l_tax']}
                        ]
                    }
                },
                'avg_qty': {'$avg': '$l_quantity'},
                'avg_price': {'$avg': '$l_extendedprice'},
                'avg_disc': {'$avg': '$l_discount'},
                'count_order': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id.returnflag': 1,
                '_id.linestatus': 1
            }
        },
        {
            '$project': {
                '_id': 0,
                'l_returnflag': '$_id.returnflag',
                'l_linestatus': '$_id.linestatus',
                'sum_qty': 1,
                'sum_base_price': 1,
                'sum_disc_price': 1,
                'sum_charge': 1,
                'avg_qty': 1,
                'avg_price': 1,
                'avg_disc': 1,
                'count_order': 1
            }
        }
    ]
    
    return list(lineitem_col.aggregate(pipeline))

def query2_mongodb(db):
    """Q2: Minimum Cost Supplier Query - Versão otimizada com agregação"""
    part_col = db['part']
    
    # Pipeline de agregação otimizado
    pipeline = [
        # Filtrar partes: p_size = 15 e p_type LIKE '%BRASS'
        {
            '$match': {
                'p_size': 15,
                'p_type': {'$regex': 'BRASS$'}
            }
        },
        # Desdobrar partsupps em documentos separados
        {
            '$unwind': {
                'path': '$partsupps',
                'preserveNullAndEmptyArrays': False
            }
        },
        # Filtrar apenas partsupps da região EUROPE
        {
            '$match': {
                'partsupps.supplier.nation.region.r_name': 'EUROPE'
            }
        },
        # Agrupar por partkey para encontrar o menor custo
        {
            '$group': {
                '_id': '$p_partkey',
                'p_mfgr': {'$first': '$p_mfgr'},
                'min_cost': {'$min': '$partsupps.ps_supplycost'},
                'partsupps': {'$push': '$partsupps'}
            }
        },
        # Desdobrar novamente para processar apenas os com menor custo
        {
            '$unwind': '$partsupps'
        },
        # Filtrar apenas os partsupps com o menor custo
        {
            '$match': {
                '$expr': {'$eq': ['$partsupps.ps_supplycost', '$min_cost']}
            }
        },
        # Projetar campos finais
        {
            '$project': {
                '_id': 0,
                's_acctbal': '$partsupps.supplier.s_acctbal',
                's_name': '$partsupps.supplier.s_name',
                'n_name': '$partsupps.supplier.nation.n_name',
                'p_partkey': '$_id',
                'p_mfgr': 1,
                's_address': '$partsupps.supplier.s_address',
                's_phone': '$partsupps.supplier.s_phone',
                's_comment': '$partsupps.supplier.s_comment'
            }
        },
        # Ordenar: s_acctbal DESC, n_name, s_name, p_partkey
        {
            '$sort': {
                's_acctbal': -1,
                'n_name': 1,
                's_name': 1,
                'p_partkey': 1
            }
        }
    ]
    
    return list(part_col.aggregate(pipeline))

def query3_mongodb(db):
    """Q3: Shipping Priority Query"""
    orders_col = db['orders']
    
    # Data limite: 1995-03-15
    date_limit = datetime(1995, 3, 15)
    
    pipeline = [
        {
            '$match': {
                'o_orderdate': {'$lt': date_limit.strftime('%Y-%m-%d')}
            }
        },
        {
            '$lookup': {
                'from': 'customer',
                'localField': 'o_custkey',
                'foreignField': 'c_custkey',
                'as': 'customer'
            }
        },
        {
            '$unwind': '$customer'
        },
        {
            '$match': {
                'customer.c_mktsegment': 'BUILDING'
            }
        },
        {
            '$unwind': '$lineitems'
        },
        {
            '$match': {
                'lineitems.l_shipdate': {'$gt': date_limit.strftime('%Y-%m-%d')}
            }
        },
        {
            '$group': {
                '_id': {
                    'l_orderkey': '$o_orderkey',
                    'o_orderdate': '$o_orderdate',
                    'o_shippriority': '$o_shippriority'
                },
                'revenue': {
                    '$sum': {
                        '$multiply': [
                            '$lineitems.l_extendedprice',
                            {'$subtract': [1, '$lineitems.l_discount']}
                        ]
                    }
                }
            }
        },
        {
            '$sort': {
                'revenue': -1,
                '_id.o_orderdate': 1
            }
        },
        {
            '$project': {
                '_id': 0,
                'l_orderkey': '$_id.l_orderkey',
                'revenue': 1,
                'o_orderdate': '$_id.o_orderdate',
                'o_shippriority': '$_id.o_shippriority'
            }
        }
    ]
    
    return list(orders_col.aggregate(pipeline))

