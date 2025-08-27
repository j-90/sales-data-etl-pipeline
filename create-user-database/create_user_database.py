import psycopg2
import logging
import os
from typing import Optional
from psycopg2.extensions import connection, cursor


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
    logger = logging.getLogger('create_user_database')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/create_user_db.log', mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_superuser_connection() -> connection:
    """
    Estabelece conexão com PostgreSQL como superusuário.
    
    Returns:
        connection: Conexão com o banco de dados
        
    Raises:
        psycopg2.Error: Se houver erro na conexão
    """
    connection_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': '1234',  # Altere para a senha do seu superusuário
        'database': 'postgres'
    }
    
    return psycopg2.connect(**connection_params)


def user_exists(cur: cursor, username: str) -> bool:
    """
    Verifica se um usuário existe no PostgreSQL.
    
    Args:
        cur: Cursor do banco de dados
        username: Nome do usuário a verificar
        
    Returns:
        bool: True se o usuário existe, False caso contrário
    """
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (username,))
    return cur.fetchone() is not None


def create_user(cur: cursor, username: str, password: str, logger: logging.Logger) -> None:
    """
    Cria um usuário no PostgreSQL.
    
    Args:
        cur: Cursor do banco de dados
        username: Nome do usuário
        password: Senha do usuário
        logger: Logger para registrar informações
    """
    cur.execute("CREATE USER %s WITH PASSWORD %s;", (username, password))
    logger.info(f"Usuário '{username}' criado com sucesso.")


def grant_createdb_permission(cur: cursor, username: str, logger: logging.Logger) -> None:
    """
    Concede permissão para criar banco de dados ao usuário.
    
    Args:
        cur: Cursor do banco de dados
        username: Nome do usuário
        logger: Logger para registrar informações
    """
    cur.execute("ALTER USER " + username + " CREATEDB;")
    logger.info(f"Permissão de criação de banco de dados concedida ao usuário '{username}'.")


def database_exists(cur: cursor, dbname: str) -> bool:
    """
    Verifica se um banco de dados existe no PostgreSQL.
    
    Args:
        cur: Cursor do banco de dados
        dbname: Nome do banco de dados
        
    Returns:
        bool: True se o banco existe, False caso contrário
    """
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
    return cur.fetchone() is not None


def create_database(cur: cursor, dbname: str, owner: str, logger: logging.Logger) -> None:
    """
    Cria um banco de dados no PostgreSQL.
    
    Args:
        cur: Cursor do banco de dados
        dbname: Nome do banco de dados
        owner: Proprietário do banco de dados
        logger: Logger para registrar informações
    """
    cur.execute("CREATE DATABASE " + dbname + " OWNER " + owner + ";")
    logger.info(f"Banco de dados '{dbname}' criado com sucesso.")


def create_user_db() -> None:
    """
    Cria o usuário 'visitante' com senha 'teste', concede permissão para criar banco de dados
    e cria o banco de dados 'comercial' no PostgreSQL.
    
    Raises:
        psycopg2.Error: Se houver erro na operação de banco de dados
        Exception: Se houver erro geral na operação
    """
    logger = setup_logging()
    logger.info("Iniciando criação de usuário e banco de dados")
    
    # Configurações
    usuario = "visitante"
    senha = "teste"
    database = "comercial"
    
    try:
        # Estabelecer conexão como superusuário
        conn = get_superuser_connection()
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Conexão com PostgreSQL estabelecida como superusuário")
        
        # Criar usuário, se não existir
        if not user_exists(cur, usuario):
            create_user(cur, usuario, senha, logger)
        else:
            logger.info(f"Usuário '{usuario}' já existe.")
        
        # Conceder permissão para criar banco de dados
        grant_createdb_permission(cur, usuario, logger)
        
        # Criar banco de dados, se não existir
        if not database_exists(cur, database):
            create_database(cur, database, usuario, logger)
        else:
            logger.info(f"Banco de dados '{database}' já existe.")
        
        # Fechar conexões
        cur.close()
        conn.close()
        logger.info("Conexão com banco de dados fechada")
        logger.info("Criação de usuário e banco de dados concluída com sucesso")
        
    except psycopg2.Error as e:
        logger.error(f"Erro de banco de dados: {e}")
        logger.error("Verifique se o PostgreSQL está rodando e se as credenciais do superusuário estão corretas.")
        raise
    except Exception as e:
        logger.error(f"Erro geral: {e}")
        raise


if __name__ == "__main__":
    try:
        create_user_db()
    except Exception as e:
        print(f"Erro na criação de usuário e banco de dados: {e}")
        exit(1)

