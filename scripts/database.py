# -*- coding: utf-8 -*-
# UNIVERSIDADE FEDERAL DO AMAZONAS
# INSTITUTO DE COMPUTAÇÃO
# BANCO DE DADOS I - 2022/02 (2023)
# PROFESSOR: ALTIGRAN SOARES DA SILVA
# ALUNOS: JOSÉ MATEUS CORDOVA RODRIGUES
# ALUNOS: GIOVANNA ANDRADE SANTOS
# ALUNOS: MARCOS AVNER PIMENTA DE LIMA
# TRABALHO PRÁTICO I
import psycopg2
import os

from configparser import ConfigParser
from psycopg2 import extras


class DatabaseManager:
    """
    Classe responsável pela criação do banco de dados e gerenciamento das conexões com o SGBD.

    As conexões com o SGBD seguem o pattern Singleton.

    Constants:
        - POSTGRESQL_DB (str): String constante com o nome do SGBD postgresql, para uso na requisição de uma conexão.
        - DATABASE_NAME (str): String constante com o nome do banco de dados utilizado nesta aplicação.
        - DATABASE_CONFIG (str): String constante com o nome arquivo com as configurações para acesso ao banco de dados.

    """

    POSTGRESQL_DB = 'postgresql'
    DATABASE_NAME = 'amazon'
    DATABASE_CONFIG_FILENAME = 'database.ini' # nome do arquivo de configurações da conexão com o sgbd
    DATABASE_CREATE_FILENAME = 'create_db.sql' # nome do arquivo de configurações da conexão com o sgbd
    __connection = None # objeto singleton de conexão com o sgbd

    TABLE_PRODUCT = 'product'
    TABLE_CATEGORY = 'category'
    TABLE_PRODUCT_CATEGORY = 'product_category'
    TABLE_REVIEW = 'review'
    TABLE_PRODUCT_SIMILAR = 'similar_product'

    @classmethod
    def __get_connection_params(cls, sgbd_name: str):
        """
        Esta função carrega os parametros de conexão do arquivo de configurações e coloca-os num dicionário.

        Args:
            sgbd_name (str): Nome do SGDB a ser utilizado.

        Raises:
            FileNotFoundError: Caso o arquivo de configurações não seja encontrado na pasta atual.
            Exception: Caso a seção para o SGBD escolhido não sejam encontradas no arquivo de configurações.
        """
        # verifica se o arquivo de configurações existe
        filepath = os.path.join(os.getcwd(), DatabaseManager.DATABASE_CONFIG_FILENAME)
        if os.path.exists(filepath):
            # criar um objeto parser e carrega o arquivo de configurações
            parser = ConfigParser()
            parser.read(filepath)
            # se a seção escolhida (sgbd) estiver presente no arquivo, carrega os parametros para um dicionário
            if parser.has_section(sgbd_name):
                params = parser.items(sgbd_name)
                db = dict(params)
                return db
            else:
                raise Exception(f'Section {sgbd_name} not found in the file: {filepath}')
        else:
            raise FileNotFoundError(f'Config file not found: {filepath}')

    @classmethod
    def get_connection(cls, sgbd_name: str):
        """
        Esta função é responsável por fonecer o objeto Singleton de conexão com o SGBD.

        Caso o objeto de conexão não exista ou a conexão estiver fechada, abre uma nova conexão num novo objeto.

        Args:
            sgbd_name (str): Nome do SGDB a ser utilizado (POSTGRESQL_DB | MYSQL_DB).
        """
        if not DatabaseManager.__connection or DatabaseManager.__connection.closed:
            DatabaseManager.__connection = psycopg2.connect(**DatabaseManager.__get_connection_params(sgbd_name))
        return DatabaseManager.__connection

    @classmethod
    def close_connection(cls):
        """
        Esta função é responsável por fechar a conexão ativa com o SGBD.
        """
        if DatabaseManager.__connection and not DatabaseManager.__connection.closed:
            DatabaseManager.__connection.close()

    @classmethod
    def create_database(cls, sgbd_name: str):
        """
        Esta função é responsável por estabelecer a primeira conexão com o SGBD, criar o schema do banco de dados, tabelas e relacionamentos.

        A primeira conexão feita é para criar o schema do banco de dados, por isso conectamos diretamente no banco de dados padrão do sgbd.

        Args:
            sgbd_name (str): Nome do SGDB a ser utilizado (POSTGRESQL_DB | MYSQL_DB).
        """
        db_params = DatabaseManager.__get_connection_params(f'{sgbd_name}-admin')
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE {DatabaseManager.DATABASE_NAME};")
        conn.close()
        # lê e executa o script de criação das tabelas do banco de dados
        with open(DatabaseManager.DATABASE_CREATE_FILENAME, 'r') as db_file:
            sql = ''.join(db_file.readlines())
            conn = DatabaseManager.get_connection(sgbd_name)
            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.close()
            conn.commit()
            conn.close()

    @classmethod
    def insert_one(cls, row, table_name: str):
        params = ', '.join(['%s' for _ in row])
        query = f"INSERT INTO {table_name} VALUES ({params})"
        # recupera a conexão com o sgbd
        conn = DatabaseManager.get_connection(DatabaseManager.POSTGRESQL_DB)
        cursor = conn.cursor()
        # executa o comando SQL
        cursor.execute(query, row)
        conn.commit()
        return cursor.rowcount
    
    @classmethod
    def insert_many(cls, rows, table_name: str, attrs=None):
        # constroi o comando SQL para inserção
        if not attrs:
            sql_query = f'INSERT INTO {table_name} VALUES %s'
        else:
            sql_query = f'INSERT INTO {table_name} ({attrs}) VALUES %s'
        # recupera a conexão com o sgbd
        connection = DatabaseManager.get_connection(DatabaseManager.POSTGRESQL_DB)
        cursor = connection.cursor()
        # executa o comando SQL
        extras.execute_values(cursor, sql_query, rows)
        connection.commit()
        return cursor.rowcount
