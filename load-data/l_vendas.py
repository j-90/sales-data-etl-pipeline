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
    logger = logging.getLogger('loading_vendas')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/loading_vendas_postgresql.log', mode='a', encoding='utf-8')
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


def create_vendas_table(cur: psycopg2.extensions.cursor) -> None:
    """
    Cria a tabela vendas no banco de dados.
    
    Args:
        cur: Cursor do banco de dados
    """
    cur.execute("""
        DROP TABLE IF EXISTS vendas;
        CREATE TABLE vendas (
            id_venda INTEGER PRIMARY KEY,
            data DATE NOT NULL,
            id_produto INTEGER NOT NULL,
            id_empregado INTEGER,
            quantidade INTEGER NOT NULL,
            valor_unitario NUMERIC(10, 2),
            valor_total NUMERIC(10, 2)
        );
    """)


def insert_vendas_data(cur: psycopg2.extensions.cursor, df: pd.DataFrame, logger: logging.Logger) -> int:
    """
    Insere dados de vendas no banco de dados.
    
    Args:
        cur: Cursor do banco de dados
        df: DataFrame com dados de vendas
        logger: Logger para registrar informações
        
    Returns:
        int: Número de registros inseridos
    """
    inserted_count = 0
    
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO vendas (id_venda, data, id_produto, id_empregado, quantidade, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_venda) DO NOTHING;
            """, (
                int(row['id_venda']),
                row['data'],
                int(row['id_produto']),
                int(row['id_empregado']) if pd.notnull(row['id_empregado']) and row['id_empregado'] != '' else None,
                int(row['quantidade']),
                float(row['valor_unitario']) if pd.notnull(row['valor_unitario']) and row['valor_unitario'] != '' else None,
                float(row['valor_total']) if pd.notnull(row['valor_total']) and row['valor_total'] != '' else None
            ))
            inserted_count += 1
        except Exception as e:
            logger.warning(f"Erro ao inserir venda ID {row['id_venda']}: {e}")
    
    return inserted_count


def loading_vendas_postgresql(df_vendas: pd.DataFrame) -> None:
    """
    Carrega dados de vendas tratados no banco de dados PostgreSQL.
    
    Args:
        df_vendas: DataFrame do pandas com os dados das vendas já tratados
        
    Raises:
        ValueError: Se o DataFrame não possuir as colunas necessárias
        Exception: Se houver erro na operação de banco de dados
    """
    logger = setup_logging()
    logger.info("Iniciando carregamento de vendas no PostgreSQL")
    
    try:
        # Validar DataFrame de entrada
        required_columns = ['id_venda', 'data', 'id_produto', 'id_empregado', 'quantidade', 'valor_unitario', 'valor_total']
        validate_dataframe(df_vendas, required_columns)
        logger.info(f"DataFrame validado. Total de registros: {len(df_vendas)}")
        
        # Estabelecer conexão
        conn = get_database_connection()
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Conexão com banco de dados estabelecida")
        
        # Criar tabela
        create_vendas_table(cur)
        logger.info("Tabela 'vendas' criada/recriada")
        
        # Inserir dados
        inserted_count = insert_vendas_data(cur, df_vendas, logger)
        logger.info(f"Dados inseridos com sucesso: {inserted_count} registros")
        
        # Fechar conexões
        cur.close()
        conn.close()
        logger.info("Conexão com banco de dados fechada")
        logger.info("Carregamento de vendas concluído com sucesso")
        
    except ValueError as e:
        logger.error(f"Erro de validação: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao carregar dados no PostgreSQL: {e}")
        logger.error("Verifique se o banco de dados está acessível e as credenciais estão corretas")
        raise


if __name__ == "__main__":
    # Teste da função (requer DataFrame de vendas tratados)
    try:
        # Exemplo de uso - seria chamado pelo pipeline com dados já tratados
        print("Este módulo deve ser chamado pelo pipeline com dados já tratados")
        print("Para testar, execute: python pipeline.py")
    except Exception as e:
        print(f"Erro: {e}")
        
