# -*- coding: utf-8 -*-
# UNIVERSIDADE FEDERAL DO AMAZONAS
# INSTITUTO DE COMPUTAÇÃO
# BANCO DE DADOS I - 2022/02 (2023)
# PROFESSOR: ALTIGRAN SOARES DA SILVA
# ALUNOS: JOSÉ MATEUS CORDOVA RODRIGUES
# ALUNOS: GIOVANNA ANDRADE SANTOS
# ALUNOS: MARCOS AVNER PIMENTA DE LIMA
# TRABALHO PRÁTICO I
import re
from database import DatabaseManager

class AmazonDataLoader:

    __PRODUCT_REGEX_PATTERN = re.compile(r"^Id:\s(\d+)\sASIN:\s([\w\d]+)\stitle:\s(.+)\sgroup:\s([\w\s]+)\ssalesrank:\s-?(\d+)\ssimilar:\s(\d+)([\s\w\d]*)\scategories:\s(\d+)")
    __REVIEWS_RESUME_PATTERN = re.compile(r"total:\s(\d+)\sdownloaded:\s(\d+)\savg\srating:\s(\d+\.\d+|\d+)")
    __PRODUCT_DISCONTINUED_REGEX_PATTERN = re.compile(r"^Id:\s(\d+)\sASIN:\s([\w\d]+)\sdiscontinued\sproduct")
    __CATEGORY_REGEX_PATTERN = re.compile(r'(.*)\[(\d+)\]')
    __REVIEW_REGEX_PATTERN = re.compile(r'^(\d{4}-\d{1,2}-\d{1,2})\s{1,2}cutomer:\s{1,6}([\w\d]+)\s{2}rating:\s(\d+)\s{2}votes:\s{1,3}(\d+)\s{2}helpful:\s{1,3}(\d+)\s')

    @classmethod
    def extract(cls, path: str):
        with open(path, 'r', encoding='utf-8') as dataset_file:
            # pula as três primeiras linhas de cabeçalho do arquivo
            for _ in range(3):
                _ = dataset_file.readline()

            # lista que ira armazenar um conjunto as linhas do arquivo referente a um produto
            data_block = []
            product_list = []
            category_dict = dict()
            prod_cat_list = []
            prod_similars = []
            review_list = []

            # percorre todo o dataset linha por linha
            for line in dataset_file:
                # se a linha atual for vazia, significa que chegamos ao final de um bloco
                if not line.strip():
                    product = AmazonDataLoader.__extract_product(data_block)
                    product_list.append(product)

                    if not product[2].startswith('DISCON'):
                        cats = AmazonDataLoader.__extract_categories(data_block, product)
                        category_dict.update(cats)

                        for category in cats.values():
                            prod_cat_list.append((product[0], category[0]))

                        if product[5]:
                            similars = AmazonDataLoader.__extract_similars(data_block, product)
                            prod_similars.extend(similars)
                        
                        if product[7]:
                            reviews = AmazonDataLoader.__extract_reviews(data_block, product)
                            review_list.extend(reviews)
                    
                    data_block = [] # esvazia o bloco atual
                    continue # avança para a próxima linha do arquivo
            
                # se a linha atual contiver metadados ela deve ser adicionada no bloco
                data_block.append(line) 
            
            print('\nSALVANDO PRODUTOS NO BANCO DE DADOS...')
            DatabaseManager.insert_many(product_list, DatabaseManager.TABLE_PRODUCT)
            print(f'\tTOTAL REGISTROS: {len(product_list)}')
            
            print('SALVANDO CATEGORIAS NO BANCO DE DADOS...')
            category_list = list(category_dict.values())
            DatabaseManager.insert_many(category_list, DatabaseManager.TABLE_CATEGORY)
            print(f'\tTOTAL REGISTROS: {len(category_list)}')

            print('SALVANDO PRODUTOS-CATEGORIAS NO BANCO DE DADOS...')
            DatabaseManager.insert_many(prod_cat_list, DatabaseManager.TABLE_PRODUCT_CATEGORY)
            print(f'\tTOTAL REGISTROS: {len(prod_cat_list)}')

            print('SALVANDO PRODUTOS-SIMILARES NO BANCO DE DADOS...')
            DatabaseManager.insert_many(prod_similars, DatabaseManager.TABLE_PRODUCT_SIMILAR)
            print(f'\tTOTAL REGISTROS: {len(prod_similars)}')

            print('SALVANDO REVIEWS NO BANCO DE DADOS...')
            DatabaseManager.insert_many(review_list, DatabaseManager.TABLE_REVIEW, 'product_id,customer_id,review_date,review_rating,review_votes,review_helpful')
            print(f'\tTOTAL REGISTROS: {len(review_list)}')


    @staticmethod
    def __extract_product(data_block):
        str_block = ''.join(data_block)
        str_block = re.sub('\n', ' ', str_block)
        str_block = re.sub(' +', ' ', str_block)
        m = AmazonDataLoader.__PRODUCT_DISCONTINUED_REGEX_PATTERN.match(str_block)
        if m:
            row = m.groups()
            row = (int(row[0]), row[1], 'DISCONTINUED PRODUCT', None, None, None, None, None, None, None)
            print(f'PRODUTO: {row[0]} (DISCONTINUED PRODUCT)')
            return row
        
        m = AmazonDataLoader.__PRODUCT_REGEX_PATTERN.match(str_block)         
        if m:
            row = m.groups()
            row = (int(row[0]), row[1], str(row[2]).upper(), str(row[3]).upper()) + (int(row[4]), int(row[5]), int(row[7]))
            n = AmazonDataLoader.__REVIEWS_RESUME_PATTERN.search(str_block)
            row = row + n.groups()
            print(f'PRODUTO: {row[0]}')
            return row
         
        return None

    @staticmethod
    def __extract_categories(data_block, product):
        categories_dict = dict()
        n_categories = product[6]
        for i in range(7, 7 + n_categories):
            categories_str = data_block[i][:-1].split('|')[1:]
            super_ct = None
        
            for category_str in categories_str:
                category_data = AmazonDataLoader.__CATEGORY_REGEX_PATTERN.match(category_str).groups()
        
                if super_ct:
                   categories_dict[int(category_data[1])] = (int(category_data[1]), category_data[0].upper(), int(super_ct[1]))
                else:
                    categories_dict[int(category_data[1])] = (int(category_data[1]), category_data[0].upper(), None)
                super_ct = category_data
        
        return categories_dict

    @staticmethod
    def __extract_similars(data_block, product):
        similar_str = data_block[5].strip()
        assert similar_str.startswith('similar:')
        similars_ids = similar_str.split('  ')[1:]
        return [(product[1], sid) for sid in similars_ids]

    @staticmethod
    def __extract_reviews(data_block, product):
        reviews = []
        for review_str in data_block[7:]:
            m = AmazonDataLoader.__REVIEW_REGEX_PATTERN.match(review_str[4:])
            if m:
                review = m.groups()
                reviews.append((product[0], review[1], review[0], review[2], review[3], review[4]))
        return reviews
