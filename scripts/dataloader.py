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

    @classmethod
    def extract(cls, path: str):
        with open(path, 'r', encoding='utf-8') as dataset_file:
            # pula as três primeiras linhas de cabeçalho do arquivo
            for _ in range(3):
                _ = dataset_file.readline()

            # lista que ira armazenar um conjunto as linhas do arquivo referente a um produto
            data_block = []
            products = []
            # categories = set()

            # percorre todo o dataset linha por linha
            for line in dataset_file:
                # se a linha atual for vazia, significa que chegamos ao final de um bloco
                if not line.strip():
                    prod = AmazonDataLoader.__extract_product(data_block)
                    products.append(prod)

                    if not prod[2].startswith('DISCON'):
                        # cts = AmazonDataLoader.__extract_categories(data_block, prod[6])
                        # categories.update(cts)

                        #TODO vincular as 'categories' com o 'product' e salvar o relacionamento no banco de dados

                        similars = AmazonDataLoader.__extract_similars(data_block)
                        #TODO salvar os 'similars' no banco de dados[

                        reviews = AmazonDataLoader.__extract_reviews(data_block)
                        #TODO salva as 'reviews' no banco de dado]
                    
                    data_block = [] # esvazia o bloco atual
                    continue # avança para a próxima linha do arquivo
            
                # se a linha atual contiver metadados ela deve ser adicionada no bloco
                data_block.append(line) 
            
            DatabaseManager.insert_many(products, DatabaseManager.TABLE_PRODUCT)


    @staticmethod
    def __extract_product(data_block):
        str_block = ''.join(data_block)
        str_block = re.sub('\n', ' ', str_block)
        str_block = re.sub(' +', ' ', str_block)
        #print(str_block, '\n\n')
        m = AmazonDataLoader.__PRODUCT_DISCONTINUED_REGEX_PATTERN.match(str_block)
        if m:
            row = m.groups()
            row = (int(row[0]), row[1], 'DISCONTINUED PRODUCT', None, None, None, None, None, None, None)
            print(f'PRODUTO EXTRAIDO: DISCONTINUED PRODUCT ({row[0]} / {row[1]})')
            return row
        
        m = AmazonDataLoader.__PRODUCT_REGEX_PATTERN.match(str_block)         
        if m:
            row = m.groups()
            row = (int(row[0]), row[1], str(row[2]).upper(), str(row[3]).upper()) + (int(row[4]), int(row[5]), int(row[7]))
            n = AmazonDataLoader.__REVIEWS_RESUME_PATTERN.search(str_block)
            row = row + n.groups()
            print(f'PRODUTO EXTRAIDO: {row[2]} ({row[0]} / {row[1]})')
            return row
         
        return None

    @staticmethod
    def __extract_categories(data_block, n_categories):
        categories_set = set()
        
        for i in range(7, 7+n_categories):
            categories_str = data_block[i][:-1].split('|')[1:]
            super_ct = None
        
            for category_str in categories_str:
                category_data = AmazonDataLoader.__CATEGORY_REGEX_PATTERN.match(category_str).groups()
        
                if super_ct:
                    category_data = (int(category_data[1]), category_data[0].upper(), super_ct[0])
                else:
                    category_data = (int(category_data[1]), category_data[0].upper(), None)
                categories_set.add(category_data)
                super_ct = category_data
        
        return categories_set

    @staticmethod
    def __extract_similars(data_block):
        pass

    @staticmethod
    def __extract_reviews(data_block):
        pass