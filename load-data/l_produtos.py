import psycopg2
from psycopg2 import sql
import pandas as pd
import logging
import os
from typing import Optional


def setup_logging() -> logging.Logger:
    """
    Configura e retorna um logger específico para o módulo.
    
    Returns:
        logging.Logger: Logger configurado
    """
    # Garantir que o diretório de logs existe
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Configurar logger específico
    logger = logging.getLogger('loading_produtos')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/loading_produtos_postgresql.log', mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_database_connection() -> psycopg2.extensions.connection:
    """
    Estabelece conexão com o banco de dados PostgreSQL.
    
    Returns:
        psycopg2.extensions.connection: Conexão com o banco
        
    Raises:
        psycopg2.Error: Se houver erro na conexão
    """
    connection_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'bus2',
        'user': 'bus2',
        'password': 'testebus2'
    }
    
    return psycopg2.connect(**connection_params)


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> None:
    """
    Valida se o DataFrame possui as colunas necessárias.
    
    Args:
        df: DataFrame a ser validado
        required_columns: Lista de colunas obrigatórias
        
    Raises:
        ValueError: Se alguma coluna obrigatória estiver faltando
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias não encontradas: {missing_columns}")


def create_produtos_table(cur: psycopg2.extensions.cursor) -> None:
    """
    Cria a tabela produtos no banco de dados.
    
    Args:
        cur: Cursor do banco de dados
    """
    cur.execute("""
        DROP TABLE IF EXISTS produtos;
        CREATE TABLE produtos (
            id_produto INTEGER PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            preco NUMERIC(10, 2),
            categoria VARCHAR(255)
        );
    """)


def insert_produtos_data(cur: psycopg2.extensions.cursor, df: pd.DataFrame, logger: logging.Logger) -> int:
    """
    Insere dados de produtos no banco de dados.
    
    Args:
        cur: Cursor do banco de dados
        df: DataFrame com dados de produtos
        logger: Logger para registrar informações
        
    Returns:
        int: Número de registros inseridos
    """
    inserted_count = 0
    
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO produtos (id_produto, nome, preco, categoria)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_produto) DO NOTHING;
            """, (
                int(row['id_produto']),
                str(row['nome']),
                float(row['preco']) if pd.notnull(row['preco']) and row['preco'] != '' else None,
                str(row['categoria']) if pd.notnull(row['categoria']) else None
            ))
            inserted_count += 1
        except Exception as e:
            logger.warning(f"Erro ao inserir produto ID {row['id_produto']}: {e}")
    
    return inserted_count


def loading_produtos_postgresql(df_produtos: pd.DataFrame) -> None:
    """
    Carrega dados de produtos tratados no banco de dados PostgreSQL.
    
    Args:
        df_produtos: DataFrame do pandas com os dados dos produtos já tratados
        
    Raises:
        ValueError: Se o DataFrame não possuir as colunas necessárias
        Exception: Se houver erro na operação de banco de dados
    """
    logger = setup_logging()
    logger.info("Iniciando carregamento de produtos no PostgreSQL")
    
    try:
        # Validar DataFrame de entrada
        required_columns = ['id_produto', 'nome', 'preco', 'categoria']
        validate_dataframe(df_produtos, required_columns)
        logger.info(f"DataFrame validado. Total de registros: {len(df_produtos)}")
        
        # Estabelecer conexão
        conn = get_database_connection()
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Conexão com banco de dados estabelecida")
        
        # Criar tabela
        create_produtos_table(cur)
        logger.info("Tabela 'produtos' criada/recriada")
        
        # Inserir dados
        inserted_count = insert_produtos_data(cur, df_produtos, logger)
        logger.info(f"Dados inseridos com sucesso: {inserted_count} registros")
        
        # Fechar conexões
        cur.close()
        conn.close()
        logger.info("Conexão com banco de dados fechada")
        logger.info("Carregamento de produtos concluído com sucesso")
        
    except ValueError as e:
        logger.error(f"Erro de validação: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao carregar dados no PostgreSQL: {e}")
        logger.error("Verifique se o banco de dados está acessível e as credenciais estão corretas")
        raise


if __name__ == "__main__":
    # Teste da função (requer DataFrame de produtos tratados)
    try:
        # Exemplo de uso - seria chamado pelo pipeline com dados já tratados
        print("Este módulo deve ser chamado pelo pipeline com dados já tratados")
        print("Para testar, execute: python pipeline.py")
    except Exception as e:
        print(f"Erro: {e}")
        
