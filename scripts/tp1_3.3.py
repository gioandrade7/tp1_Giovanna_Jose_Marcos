# -*- coding: utf-8 -*-
# UNIVERSIDADE FEDERAL DO AMAZONAS
# INSTITUTO DE COMPUTAÇÃO
# BANCO DE DADOS I - 2022/02 (2023)
# PROFESSOR: ALTIGRAN SOARES DA SILVA
# ALUNOS: JOSÉ MATEUS CORDOVA RODRIGUES
# ALUNOS: GIOVANNA ANDRADE SANTOS
# ALUNOS: MARCOS AVNER PIMENTA DE LIMA
# TRABALHO PRÁTICO I

from database import DatabaseManager
from typing import Iterable, Any

COMENTARIOS_UTEIS_SQL = "((SELECT product.product_id, product.product_asin, product.product_title, review.customer_id, review.review_date, review.review_rating, review.review_helpful FROM product, review WHERE product.product_id=%s ORDER BY review.review_rating DESC, review.review_helpful DESC LIMIT 5) UNION ALL (SELECT product.product_id, product.product_asin, product.product_title, review.customer_id, review.review_date, review.review_rating, review.review_helpful FROM product, review WHERE product.product_id=%s ORDER BY review.review_rating ASC, review.review_helpful DESC LIMIT 5))"

# (
# (SELECT product.product_id, product.product_asin, product.product_title, review.customer_id, review.review_date, review.review_rating, review.review_helpful 
# FROM product, review 
# WHERE product.product_id=%s 
# ORDER BY rating DESC, helpful DESC LIMIT 5) 

# UNION ALL 

# (SELECT product.product_id, product.product_asin, product.product_title, review.customer_id, review.review_date, review.review_rating, review.review_helpful FROM product, review 
# WHERE product.product_id=%s 
# ORDER BY rating ASC, helpful DESC LIMIT 5)
# )

PRODUTOS_SIMILARES_SQL = "SELECT p.product_id, p.product_asin, p.product_title, p.product_salesrank, sim.product_id, sim.product_asin, sim.product_title, sim.product_salesrank FROM similar_product s INNER JOIN product p ON s.product_asin = p.product_asin INNER JOIN product sim ON s.similar_asin = sim.product_asin WHERE p.product_id=50 AND sim.product_salesrank > p.product_salesrank ORDER BY sim.product_salesrank DESC;"

# SELECT s.product_asin, p.product_title, p.product_salesrank, s.similar_asin, sim.product_title, sim.product_salesrank
# FROM similar_product s
# INNER JOIN product p ON s.product_asin = p.product_asin
# INNER JOIN product sim ON s.similar_asin = sim.product_asin
# WHERE p.product_asin =%s
# AND sim.product_salesrank > p.product_salesrank
# ORDER BY sim.product_salesrank DESC;

EVOLUCAO_AVALIACAO_SQL = "SELECT product.product_title, review_date, round(AVG(review_rating),2) AS avg_rating FROM product INNER JOIN review ON product.product_id=review.product_id WHERE product.product_id=%s GROUP BY product.product_title, review.review_date ORDER BY review_date ASC;"

# SELECT product.product_title,review_date, round(AVG(review_rating),2) AS avg_rating
# FROM product INNER JOIN review ON product.product_id=review.product_id
# WHERE product_id=%s
# GROUP BY product.product_title, review.review_date 
# ORDER BY review_date ASC;

LIDERES_VENDAS_POR_CATEGORIA_SQL = "SELECT product_id,product_title,product_salesrank,product_group FROM (SELECT product_id,product_title,product_salesrank,product_group,Rank() OVER (PartitiON BY product_group ORDER BY  product_salesrank DESC ) AS Rank FROM product WHERE product_salesrank > 0) rs WHERE Rank <= 10;"

# SELECT product.product_title, product.product_group, product.product_salesrank
# FROM product
# WHERE product.product_salesrank IN (
#     SELECT product_salesrank
#     FROM product as p
#     WHERE p.product_group = product.product_group
#     ORDER BY p.product_salesrank DESC
#     LIMIT 10
# )

PRODUTOS_MELHORES_AVALIACOES_SQL = "SELECT t2.product_id,t2.product_title,t2.product_group,t2.avg_helpful,t2.n_rank FROM (SELECT product.product_id,product.product_title,product.product_group, t1.avg_helpful, ROW_NUMBER() OVER (PARTITION BY product.product_group ORDER BY  t1.avg_helpful DESC) AS n_rank FROM product JOIN (SELECT review.product_id, ROUND(AVG(review.review_helpful),2) AS avg_helpful FROM review WHERE review.review_helpful > 0 GROUP BY  review.product_id) t1 ON t1.product_id=product.product_id) AS t2 WHERE n_rank <= 10;"

# SELECT t2.product_id,
#         t2.product_title,
#         t2.product_group,
#         t2.avg_helpful,
#         t2.n_rank
#     FROM 
#     (SELECT product.product_id,
#         product.product_title,
#         product.product_group,
#         t1.avg_helpful,
#         ROW_NUMBER()
#         OVER (PARTITION BY product.product_group
#     ORDER BY  t1.avg_helpful DESC) AS n_rank
#         FROM product
#     JOIN 
#         (SELECT review.product_id,
#         ROUND(AVG(review.review_helpful),
#         2) AS avg_helpful
#             FROM review
#             WHERE review.review_helpful > 0
#             GROUP BY  review.product_id) t1
#             ON t1.product_id=product.product_id) AS t2
#         WHERE n_rank <= 10

CATEGORIAS_MAIOR_MEDIA_POR_PRODUTO_SQL = "SELECT category.category_description,ROUND(t_avg.avg,2) FROM category INNER JOIN (SELECT product_category.category_id, AVG(qtd_pos.count) FROM product_category INNER JOIN (SELECT review.product_id, COUNT(*) FROM review WHERE review.review_helpful > 0 GROUP BY  review.product_id) qtd_pos ON qtd_pos.product_id = product_category.product_id GROUP BY  product_category.category_id HAVING AVG(qtd_pos.count) > 0 ORDER BY  avg DESC limit 5) t_avg ON category.category_id = t_avg.category_id;"

# SELECT category.category_description,
#         ROUND(t_avg.avg,
#         2)
#     FROM category
# INNER JOIN 
#     (SELECT product_category.category_id,
#         AVG(qtd_pos.count)
#         FROM product_category
#     INNER JOIN 
#         (SELECT review.product_id,
#         COUNT(*)
#             FROM review
#             WHERE review.review_helpful > 0
#             GROUP BY  review.product_id) qtd_pos
#             ON qtd_pos.product_id = product_category.product_id
#             GROUP BY  product_category.category_id
#         HAVING AVG(qtd_pos.count) > 0
#         ORDER BY  avg DESC limit 5) t_avg
#         ON category.category_id = t_avg.category_id;

CLIENTES_MAIS_COMENTARIOS_SQL = "SELECT customer_id,n_reviews, review_rank, product_group FROM (SELECT customer_id,n_reviews,product_group, ROW_NUMBER() OVER (PARTITION BY t1.product_group ORDER BY  t1.n_reviews DESC) AS review_rank FROM (SELECT customer_id, COUNT(customer_id) AS n_reviews, product_group FROM product INNER JOIN review ON product.product_id=review.product_id GROUP BY  (product_group, customer_id)) AS t1 ORDER BY  t1.product_group ASC, t1.n_reviews DESC) AS t2 WHERE review_rank <= 10;"

# SELECT customer_id,
#         n_reviews,
#         review_rank,
#         product_group
#     FROM 
#     (SELECT customer_id,
#         n_reviews,
#         product_group,
#         ROW_NUMBER()
#         OVER (PARTITION BY t1.product_group
#     ORDER BY  t1.n_reviews DESC) AS review_rank
#         FROM 
#         (SELECT customer_id,
#         COUNT(customer_id) AS n_reviews,
#         product_group
#             FROM product
#         INNER JOIN review
#             ON product.product_id=review.product_id
#             GROUP BY  (product_group, customer_id)) AS t1
#         ORDER BY  t1.product_group ASC, t1.n_reviews DESC) AS t2
#         WHERE review_rank <= 10;

def print_table_data(header: Iterable[str], cols_size: Iterable[int], data: Iterable[Iterable[Any]]):
    """Essa função recebe um conjuto de dados (tuplas) e imprime no console num formato tabular
    Args:
        header (Iterable[str]): Nome das colunas da tabela (linha de cabeçalho).
        cols_size (Iterable[int]): Tamanho de caracteres em cada coluna da tabela. Utilizado para formatação.
        data (Iterable[Iterable[Any]]): Conjunto de dados num formato tabular a serem impressos no console.
    """
    # imprime o cabeçalho (nome das colunas) da tabela
    margin = 5
    print('-' * (sum(cols_size)+margin))
    for col_name, col_size in zip(header, cols_size):
        print(f'{col_name.ljust(col_size)}', end='')
    print('\n' + '-' * (sum(cols_size)+margin))
    # imprime o conjunto de dados
    for row in data:
        for value, col_size in zip(row, cols_size):
            print(f'{str(value).ljust(col_size)}', end='')
        print('')
    print('-' * (sum(cols_size)+margin))
    print('')

def execute_query(sql_query: str, params: Iterable = None) -> Iterable: # type: ignore
    """Função que recebe uma query SQL e seus parametros, executa a query e retorna os registros obtidos.
    Args:
        sql_query (str): A query SQL a ser executada. Os parametros são identificados como "placeholders" dentro da query.
        params (Iterable): Conjunto de parametros da query.
    Returns:
        Iterable: Conjunto de dados obtidos com a execução da query.
    """
    # estabelece uma conexão com o sgbd
    conn = DatabaseManager.get_connection(DatabaseManager.POSTGRESQL_DB)
    # obtem o objeto para executar consultas
    cursor = conn.cursor()
    # executa a consulta fornecida junto com seus parametros
    cursor.execute(sql_query, params) # type: ignore
    # retorna os dados encontrados (tuplas)
    return cursor.fetchall()

def query1():
    """Esta função exibe os 5 comentários mais úteis e com maior avaliação, e também os 5 mais úteis com menor avaliação, de um dado produto."""
    id = input('POR GENTILEZA, INFORME O ID DO PRODUTO: ')
    rows = execute_query(COMENTARIOS_UTEIS_SQL, (id, id))

    th = ['ID PRODUTO', 'ASIN', 'TITULO', 'ID CLIENTE', 'DATA', 'RATING', 'HELPFUL']
    ts = [12, 15, 60, 16, 12, 5] 
    print_table_data(th, ts, rows)

    
def query2():
    """Esta função exibe os produtos similares com maiores vendas do que ele"""
    id = input('POR GENTILEZA, INFORME O ID DO PRODUTO: ')
    rows = execute_query(PRODUTOS_SIMILARES_SQL, (id, ))

    th = ['P ID', 'P ASIN', 'P TITULO', 'P SALESRANK', 'S ID', 'S ASIN', 'S TITULO', 'S SALESRANK']
    ts = [6, 13, 35, 15, 6, 13, 35, 15] 
    print_table_data(th, ts, rows)

    
def query3():
    """Esta função exibe a evolução diária das médias de avaliação de um produto."""
    id = input('POR GENTILEZA, INFORME O ID DO PRODUTO: ')
    rows = execute_query(EVOLUCAO_AVALIACAO_SQL, (id, ))

    th = ['TITULO', 'DATA', 'AV MEDIA']
    ts = [40, 15, 8] 
    print_table_data(th, ts, rows)
    
def query4():
    """Esta função exibe uma tabela no console com os 10 produtos mais vendidos (líderes) de cada grupo de produtos."""
    rows = execute_query(LIDERES_VENDAS_POR_CATEGORIA_SQL)
    th = ['ID', 'TITULO', 'SALESRANK', 'GROUP']
    ts = [15, 120, 15, 20] 
    print_table_data(th, ts, rows)
    
def query5():
    """Esta função exibe uma tabela no console com os 10 produtos com a maior média de avaliações úteis positivas por grupo."""
    rows = execute_query(PRODUTOS_MELHORES_AVALIACOES_SQL)
    
    th = ['ID', 'TITULO', 'GROUP', 'AVG', 'RANK']
    ts = [15, 120, 15, 10, 5] 
    print_table_data(th, ts, rows)

    
def query6():
    """Esta função exibe uma tabela no console com as 5 categorias mais bem avaliadas."""
    rows = execute_query(CATEGORIAS_MAIOR_MEDIA_POR_PRODUTO_SQL)
    th = ['DESCRICAO', 'AVG HELPFUL REVIEWS']
    ts = [60, 20] 
    print_table_data(th, ts, rows)
    
def query7():
    """Esta função exibe uma tabela no console com os 10 clientes que mais fizeram comentários por grupo de produto."""
    rows = execute_query(CLIENTES_MAIS_COMENTARIOS_SQL)
    th = ['CUSTOMER ID', 'N REVIEWS', 'RANK', 'GROUP']
    ts = [16, 10, 7, 20] 
    print_table_data(th, ts, rows)

def sair():
    print("SAINDO...\n")

def switch_case(op):
    switcher = {
        0: sair,
        1: query1,
        2: query2,
        3: query3,
        4: query4,
        5: query5,
        6: query6,
        7: query7
    }
    func = switcher.get(op, lambda: print("Opção inválida!\n"))
    func()

if __name__ == '__main__':
    op = 99
    while op:
        print('[1] - LISTAR OS 5 COMENTÁRIOS MAIS ÚTEIS E COM MAIOR AVALIAÇÃO E OS 5 COMENTÁRIOS MAIS ÚTEIS E COM MENOR AVALIAÇÃO')
        print('[2] - LISTAR OS PRODUTOS SIMILARES COM MAIORES VENDAS')
        print('[3] - MOSTRAR EVOLUÇÃO DIÁRIA DAS AVALIAÇÕES')
        print('[4] - LISTAR OS 10 MAIS VENDIDOS EM CADA GRUPO DE PRODUTOS')
        print('[5] - LISTAR OS 10 PRODUTOS COM A MAIOR MÉDIA DE AVALIAÇÕES ÚTEIS POSITIVAS POR PRODUTO')
        print('[6] - LISTAR AS 5 CATEGORIAS DE PRODUTO COM A MAIOR MÉDIA DE AVALIAÇÕES ÚTEIS POSITIVAS')
        print('[7] - LISTAR OS 10 CLIENTES QUE MAIS FIZERAM COMENTÁRIOS POR GRUPO DE PRODUTO')
        print('[0] - SAIR')
        op = int(input('SELECIONE UMA DAS OPÇÕES: ').strip())
        switch_case(op)