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
from datetime import datetime

if __name__ == '__main__':
    start_time = datetime.now()
    DatabaseManager.create_database(DatabaseManager.POSTGRESQL_DB)
    # dataset_path = input('INFORME O LOCAL DO ARQUIVO DE METADADOS: ')
    # products, categories, prod_cats, similars, reviews = AmazonDatasetController().extrair(dataset_path)

    # ProductController.insert_batch(products)
    # print(f'{len(products)} PRODUCTS INSERTED INTO DATABASE')
    # CategoryController.insert_batch(categories.values())
    # print(f'{len(categories.values())} CATEGORIES INSERTED INTO DATABASE')
    # ProductCategoryController.insert_batch(prod_cats)
    # print(f'{len(prod_cats)} PRODUCT CATEGORIES INSERTED INTO DATABASE')
    # SimilarProductsController.insert_batch(similars)
    # print(f'{len(similars)} SIMILARS PRODUCTS INSERTED INTO DATABASE')
    # ReviewController.insert_batch(reviews)
    # print(f'{len(reviews)} PRODUCT REVIEWS INSERTED INTO DATABASE')
    DatabaseManager.close_connection()
    print(f'TIME ELAPSED: {datetime.now() - start_time}')
