import pandas as pd
import os
import logging
from typing import Dict, Tuple
from sqlalchemy import create_engine, text
import psycopg2


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
    logger = logging.getLogger('save_data_parquet')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/save_data_parquet.log', mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_database_connection() -> str:
    """
    Retorna a string de conexão para o banco de dados PostgreSQL.
    
    Returns:
        str: String de conexão SQLAlchemy
    """
    connection_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'bus2',
        'user': 'bus2',
        'password': 'testebus2'
    }
    
    return f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"


def create_output_directories(logger: logging.Logger) -> None:
    """
    Cria os diretórios necessários para salvar os arquivos.
    
    Args:
        logger: Logger para registrar informações
    """
    directories = ['dados_corrigidos', 'parquet-files']
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Diretório '{directory}' criado.")


def fetch_data_from_database(engine, logger: logging.Logger) -> Dict[str, pd.DataFrame]:
    """
    Busca dados das tabelas do banco de dados PostgreSQL.
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        Dict[str, pd.DataFrame]: Dicionário com os DataFrames das tabelas
        
    Raises:
        Exception: Se houver erro ao buscar dados
    """
    tables = ['produtos', 'empregados', 'vendas']
    dataframes = {}
    
    for table in tables:
        try:
            query = text(f"SELECT * FROM {table}")
            df = pd.read_sql_query(query, engine)
            dataframes[table] = df
            logger.info(f"Dados de {table} lidos do banco. Total de registros: {len(df)}")
        except Exception as e:
            logger.error(f"Erro ao ler dados da tabela {table}: {e}")
            raise
    
    return dataframes


def save_dataframes_to_parquet(dataframes: Dict[str, pd.DataFrame], logger: logging.Logger) -> None:
    """
    Salva os DataFrames em arquivos .parquet.
    
    Args:
        dataframes: Dicionário com os DataFrames a serem salvos
        logger: Logger para registrar informações
        
    Raises:
        Exception: Se houver erro ao salvar arquivos
    """
    for table_name, df in dataframes.items():
        try:
            # Nome personalizado para a tabela de vendas
            if table_name == 'vendas':
                file_name = 'resumo-vendas'
            else:
                file_name = table_name
            
            file_path = f'parquet-files/{file_name}.parquet'
            df.to_parquet(file_path, index=False)
            logger.info(f"Arquivo {file_path} salvo com sucesso ({len(df)} registros)")
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo {file_name}.parquet: {e}")
            raise


def log_summary_statistics(dataframes: Dict[str, pd.DataFrame], logger: logging.Logger) -> None:
    """
    Registra estatísticas resumidas dos dados processados.
    
    Args:
        dataframes: Dicionário com os DataFrames processados
        logger: Logger para registrar informações
    """
    logger.info("📊 RESUMO DOS DADOS PROCESSADOS:")
    total_records = 0
    
    for table_name, df in dataframes.items():
        records = len(df)
        columns = len(df.columns)
        total_records += records
        logger.info(f"  📋 {table_name.capitalize()}: {records:,} registros | {columns} colunas")
    
    logger.info(f"  📈 Total geral: {total_records:,} registros")


def save_parquet() -> None:
    """
    Lê os dados das tabelas do banco de dados PostgreSQL e salva em arquivos .parquet.
    Os logs são salvos em 'logs/save_data_parquet.log'.
    
    Raises:
        Exception: Se houver erro na operação
    """
    logger = setup_logging()
    logger.info("Iniciando processo de salvamento em Parquet")
    
    try:
        # Criar diretórios de saída
        create_output_directories(logger)
        
        # Estabelecer conexão com o banco
        connection_string = get_database_connection()
        engine = create_engine(connection_string)
        logger.info("Conexão com banco de dados estabelecida")
        
        # Buscar dados das tabelas
        dataframes = fetch_data_from_database(engine, logger)
        
        # Salvar em arquivos Parquet
        save_dataframes_to_parquet(dataframes, logger)
        
        # Registrar estatísticas finais
        log_summary_statistics(dataframes, logger)
        
        # Fechar conexão
        engine.dispose()
        logger.info("Conexão com banco de dados fechada")
        logger.info("Processo de salvamento em Parquet concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no processo de salvamento: {e}")
        logger.error("Verifique se o banco de dados está acessível e as tabelas existem")
        raise


if __name__ == "__main__":
    try:
        save_parquet()
    except Exception as e:
        print(f"Erro no salvamento de dados: {e}")
        exit(1)

