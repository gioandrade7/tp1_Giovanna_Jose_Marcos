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
from dataloader import AmazonDataLoader
from datetime import datetime
import os

if __name__ == '__main__':
    start_time = datetime.now()
    print('AMAZON PRODUCT CO-PURCHASING DATASET LOADER v1.0\n')
    
    resp = input('DESEJA CRIAR O BANCO DE DADOS (S/N)? ')
    if resp == 'S':
        DatabaseManager.create_database(DatabaseManager.POSTGRESQL_DB)

    dataset_path = input('\nINFORME O CAMINHO PARA O DATASET: ')
    AmazonDataLoader.extract(dataset_path)
    
    DatabaseManager.close_connection()
    print(f'TIME ELAPSED: {datetime.now() - start_time}')
