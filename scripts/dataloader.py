# -*- coding: utf-8 -*-
# UNIVERSIDADE FEDERAL DO AMAZONAS
# INSTITUTO DE COMPUTAÇÃO
# BANCO DE DADOS I - 2022/02 (2023)
# PROFESSOR: ALTIGRAN SOARES DA SILVA
# ALUNOS: JOSÉ MATEUS CORDOVA RODRIGUES
# ALUNOS: GIOVANNA ANDRADE SANTOS
# ALUNOS: MARCOS AVNER PIMENTA DE LIMA
# TRABALHO PRÁTICO I

class AmazonDataLoader:

    @classmethod
    def extract(cls, path: str):
        with open(path, 'r', encoding='utf-8') as dataset_file:
            # pula as três primeiras linhas de cabeçalho do arquivo
            for _ in range(3):
                _ = dataset_file.readline()

            # lista que ira armazenar um conjunto as linhas do arquivo referente a um produto
            data_block = []

            # percorre todo o dataset linha por linha
            for line in dataset_file:
                # se a linha atual for vazia, significa que chegamos ao final de um bloco
                if not line.strip():
                    product = AmazonDataLoader.__extract_product(data_block)
                    #TODO salvar 'product' no banco de dados 

                    categorias = AmazonDataLoader.__extract_categories(data_block)
                    #TODO salvar as 'categories' no banco de dados 

                    #TODO vincular as 'categories' com o 'product' e salvar o relacionamento no banco de dados

                    similars = AmazonDataLoader.__extract_similars(data_block)
                    #TODO salvar os 'similars' no banco de dados[

                    reviews = AmazonDataLoader.__extract_reviews(data_block)
                    #TODO salva as 'reviews' no banco de dado]
                    
                    data_block = [] # esvazia o bloco atual
                    continue # avança para a próxima linha do arquivo
            
                # se a linha atual contiver metadados ela deve ser adicionada no bloco
                data_block.append(line) 


    @staticmethod
    def __extract_product(data_block):
        

    @staticmethod
    def __extract_categories(data_block):
        pass

    @staticmethod
    def __extract_similars(data_block):
        pass

    @staticmethod
    def __extract_reviews(data_block):
        pass