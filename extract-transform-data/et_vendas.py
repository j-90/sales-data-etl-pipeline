import pandas as pd
import logging
import psycopg2
import os
from typing import Tuple, Optional
from sqlalchemy import create_engine


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
    logger = logging.getLogger('et_vendas')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/et_vendas.log', mode='a', encoding='utf-8')
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
        'database': 'comercial',
        'user': 'visitante',
        'password': 'teste'
    }
    
    return psycopg2.connect(**connection_params)


def fetch_produtos_from_db() -> pd.DataFrame:
    """
    Busca dados tratados de produtos do banco de dados usando SQLAlchemy (sem avisos).
    
    Returns:
        pd.DataFrame: DataFrame com dados de produtos tratados
        
    Raises:
        Exception: Se houver erro na consulta
    """
    try:
        # Usar SQLAlchemy para evitar avisos do pandas
        connection_string = "postgresql://visitante:teste@localhost:5432/comercial"
        engine = create_engine(connection_string)
        
        query = "SELECT id_produto, nome, preco, categoria FROM produtos"
        df_produtos = pd.read_sql_query(query, engine)
        
        # Fechar engine
        engine.dispose()
        
        return df_produtos
    except Exception as e:
        raise Exception(f"Erro ao buscar produtos do banco: {e}")


def validate_csv_file(file_path: str) -> None:
    """
    Valida se o arquivo CSV existe e é acessível.
    
    Args:
        file_path: Caminho para o arquivo CSV
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {file_path}")


def load_vendas_csv(file_path: str) -> pd.DataFrame:
    """
    Carrega dados de vendas do arquivo CSV.
    
    Args:
        file_path: Caminho para o arquivo CSV de vendas
        
    Returns:
        pd.DataFrame: DataFrame com dados de vendas
        
    Raises:
        Exception: Se houver erro na leitura
    """
    try:
        df_vendas = pd.read_csv(file_path, sep=';', encoding='utf-8')
        return df_vendas
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo CSV de vendas: {e}")


def remove_duplicates(df: pd.DataFrame, subset: list, logger: logging.Logger) -> pd.DataFrame:
    """
    Remove registros duplicados e registra informações no log.
    
    Args:
        df: DataFrame a ser processado
        subset: Colunas para verificar duplicatas
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame sem duplicatas
    """
    ids_duplicados = df[df.duplicated(subset=subset, keep=False)][subset[0]].unique()
    
    if len(ids_duplicados) > 0:
        logger.info(f"IDs de vendas duplicados que serão removidos: {ids_duplicados}")
    else:
        logger.info("Nenhum ID de venda duplicado encontrado para remoção.")
    
    return df.drop_duplicates(subset=subset, keep='first')


def validate_and_fill_dates(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida e preenche datas ausentes usando estratégias inteligentes SEM PERDER DADOS.
    
    ✅ PRESERVAÇÃO DE DADOS:
    - Mantém todas as vendas no dataset
    - Aplica imputação inteligente para datas ausentes
    - Adiciona flag de rastreabilidade para auditoria
    - Valida e padroniza formato de datas
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com todas as vendas e datas válidas
    """
    initial_count = len(df)
    logger.info(f"Iniciando validação de datas para {initial_count} vendas")
    
    # Identificar datas ausentes ou inválidas
    datas_ausentes = df[df['data'].isnull() | (df['data'] == '')]
    
    if not datas_ausentes.empty:
        logger.info(f"Encontradas {len(datas_ausentes)} vendas com data ausente - aplicando tratamento inteligente")
        
        # Estratégia 1: Imputação por empregado
        df = _fill_missing_dates_by_employee(df, logger)
        
        # Estratégia 2: Imputação por padrão temporal
        df = _fill_remaining_dates_with_pattern(df, logger)
        
        # Estratégia 3: Data de fallback para casos extremos
        df = _handle_extreme_missing_dates(df, logger)
    else:
        logger.info("Todas as vendas possuem data válida")
    
    # Validar e padronizar formato de datas
    df = _parse_and_validate_date_format(df, logger)
    
    final_count = len(df)
    logger.info(f"✅ Validação concluída: {final_count} vendas processadas (0% de perda)")
    
    return df


def _fill_missing_dates_by_employee(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche datas ausentes usando padrão de vendas do mesmo empregado.
    """
    mask_datas_ausentes = df['data'].isnull() | (df['data'] == '')
    
    if not mask_datas_ausentes.any():
        return df
    
    vendas_com_data = df[~mask_datas_ausentes].copy()
    
    if not vendas_com_data.empty:
        # Converter datas válidas para datetime
        vendas_com_data['data_parsed'] = pd.to_datetime(
            vendas_com_data['data'], 
            format='%d/%m/%Y',
            errors='coerce'
        )
        
        # Para cada venda sem data, buscar padrão do empregado
        for idx, row in df[mask_datas_ausentes].iterrows():
            id_empregado = row['id_empregado']
            
            # Buscar datas do mesmo empregado
            datas_empregado = vendas_com_data[
                vendas_com_data['id_empregado'] == id_empregado
            ]['data_parsed'].dropna()
            
            if not datas_empregado.empty:
                # Usar mediana das datas do empregado
                data_mediana = datas_empregado.median()
                df.loc[idx, 'data'] = data_mediana.strftime('%d/%m/%Y')
                
                # Adicionar flag de rastreabilidade
                if 'data_imputada' not in df.columns:
                    df['data_imputada'] = False
                df.loc[idx, 'data_imputada'] = True
                df.loc[idx, 'metodo_imputacao'] = 'mediana_empregado'
                
                logger.info(f"Venda ID {row['id_venda']}: data preenchida com mediana do empregado {id_empregado}")
    
    return df


def _fill_remaining_dates_with_pattern(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche datas ainda ausentes usando padrões temporais globais.
    """
    mask_ainda_ausentes = df['data'].isnull() | (df['data'] == '')
    
    if not mask_ainda_ausentes.any():
        return df
    
    vendas_com_data = df[~mask_ainda_ausentes]
    
    if not vendas_com_data.empty:
        # Converter datas válidas
        datas_validas = pd.to_datetime(
            vendas_com_data['data'], 
            format='%d/%m/%Y', 
            errors='coerce'
        ).dropna()
        
        if not datas_validas.empty:
            # Usar mediana global das datas
            data_mediana_global = datas_validas.median()
            data_preenchimento = data_mediana_global.strftime('%d/%m/%Y')
            
            # Aplicar a todas as vendas ainda sem data
            if 'data_imputada' not in df.columns:
                df['data_imputada'] = False
            if 'metodo_imputacao' not in df.columns:
                df['metodo_imputacao'] = ''
            
            df.loc[mask_ainda_ausentes, 'data'] = data_preenchimento
            df.loc[mask_ainda_ausentes, 'data_imputada'] = True
            df.loc[mask_ainda_ausentes, 'metodo_imputacao'] = 'mediana_global'
            
            count_preenchidas = mask_ainda_ausentes.sum()
            logger.info(f"Preenchidas {count_preenchidas} datas com mediana global: {data_preenchimento}")
    
    return df


def _handle_extreme_missing_dates(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Trata casos extremos onde ainda restam datas ausentes.
    """
    mask_ainda_ausentes = df['data'].isnull() | (df['data'] == '')
    
    if mask_ainda_ausentes.any():
        # Usar data de hoje como último recurso
        data_fallback = pd.Timestamp.now().strftime('%d/%m/%Y')
        
        if 'data_imputada' not in df.columns:
            df['data_imputada'] = False
        if 'metodo_imputacao' not in df.columns:
            df['metodo_imputacao'] = ''
        
        df.loc[mask_ainda_ausentes, 'data'] = data_fallback
        df.loc[mask_ainda_ausentes, 'data_imputada'] = True
        df.loc[mask_ainda_ausentes, 'metodo_imputacao'] = 'data_atual'
        
        count_fallback = mask_ainda_ausentes.sum()
        logger.warning(f"Casos extremos: {count_fallback} vendas receberam data atual ({data_fallback})")
    
    return df


def _parse_and_validate_date_format(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida e padroniza formato das datas.
    """
    try:
        # Converter para datetime para validação
        df['data_parsed'] = pd.to_datetime(
            df['data'], 
            format='%d/%m/%Y',
            errors='coerce'
        )
        
        # Verificar datas inválidas
        datas_invalidas = df[df['data_parsed'].isnull()]
        
        if not datas_invalidas.empty:
            logger.warning(f"Encontradas {len(datas_invalidas)} datas com formato inválido - corrigindo")
            
            # Usar data atual para formatos inválidos
            data_correcao = pd.Timestamp.now().strftime('%d/%m/%Y')
            
            if 'data_imputada' not in df.columns:
                df['data_imputada'] = False
            if 'metodo_imputacao' not in df.columns:
                df['metodo_imputacao'] = ''
            
            for idx in datas_invalidas.index:
                df.loc[idx, 'data'] = data_correcao
                df.loc[idx, 'data_imputada'] = True
                df.loc[idx, 'metodo_imputacao'] = 'formato_invalido'
                logger.warning(f"Venda ID {df.loc[idx, 'id_venda']}: formato inválido corrigido para {data_correcao}")
        
        # Remover coluna temporária
        df = df.drop(columns=['data_parsed'])
        
        # Estatísticas finais
        if 'data_imputada' in df.columns:
            total_imputadas = df['data_imputada'].sum()
            if total_imputadas > 0:
                logger.info(f"Resumo: {total_imputadas} datas foram imputadas de {len(df)} vendas ({total_imputadas/len(df)*100:.1f}%)")
                
                # Detalhar métodos utilizados
                if 'metodo_imputacao' in df.columns:
                    metodos = df[df['data_imputada']]['metodo_imputacao'].value_counts()
                    for metodo, count in metodos.items():
                        logger.info(f"  - {metodo}: {count} vendas")
            else:
                logger.info("Todas as datas eram válidas - nenhuma imputação necessária")
        
    except Exception as e:
        logger.error(f"Erro na validação de datas: {e}")
        raise
    
    return df


def fill_missing_unit_values(df_vendas: pd.DataFrame, df_produtos: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche valores unitários faltantes usando medianas por categoria.
    
    Args:
        df_vendas: DataFrame de vendas
        df_produtos: DataFrame de produtos
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com valores unitários preenchidos
    """
    # Converter para numérico
    df_vendas['valor_unitario'] = pd.to_numeric(df_vendas['valor_unitario'], errors='coerce')
    
    # Merge com produtos para obter categorias
    df_produtos_temp = df_produtos[['id_produto', 'categoria']].copy()
    df_vendas = df_vendas.merge(df_produtos_temp, on='id_produto', how='left')
    
    # Identificar valores faltantes
    linhas_valor_branco = df_vendas[df_vendas['valor_unitario'].isnull()]
    
    if linhas_valor_branco.empty:
        logger.info("Nenhuma linha com valor_unitario em branco ou nulo encontrada.")
        return df_vendas.drop(columns=['categoria'])
    
    logger.info(f"Encontradas {len(linhas_valor_branco)} linhas com valor_unitario em branco ou nulo.")
    
    # Preencher por categoria
    categorias = df_vendas['categoria'].unique()
    for categoria in categorias:
        if pd.notna(categoria):
            valores_validos = df_vendas[
                (df_vendas['categoria'] == categoria) & 
                (~df_vendas['valor_unitario'].isnull())
            ]['valor_unitario']
            
            if not valores_validos.empty:
                mediana_categoria = round(valores_validos.median(), 2)
                cond = (df_vendas['categoria'] == categoria) & (df_vendas['valor_unitario'].isnull())
                df_vendas.loc[cond, 'valor_unitario'] = mediana_categoria
                logger.info(f"Categoria '{categoria}': preenchidos com mediana {mediana_categoria}")
            else:
                logger.warning(f"Não foi possível calcular mediana para categoria '{categoria}'")
    
    # Preencher produtos sem categoria com mediana global
    produtos_sem_categoria = df_vendas[df_vendas['categoria'].isnull() & df_vendas['valor_unitario'].isnull()]
    if not produtos_sem_categoria.empty:
        mediana_global = round(df_vendas[~df_vendas['valor_unitario'].isnull()]['valor_unitario'].median(), 2)
        cond_sem_categoria = df_vendas['categoria'].isnull() & df_vendas['valor_unitario'].isnull()
        df_vendas.loc[cond_sem_categoria, 'valor_unitario'] = mediana_global
        logger.info(f"Produtos sem categoria: preenchidos com mediana global {mediana_global}")
    
    return df_vendas.drop(columns=['categoria'])


def calculate_total_values(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Calcula valores totais para linhas onde estão vazios.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com valores totais calculados
    """
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce')
    
    cond_valor_total_vazio = df['valor_total'].isnull() | (df['valor_total'] == '')
    linhas_vazias = cond_valor_total_vazio.sum()
    
    if linhas_vazias > 0:
        df.loc[cond_valor_total_vazio, 'valor_total'] = (
            df.loc[cond_valor_total_vazio, 'quantidade'].astype(float) * 
            df.loc[cond_valor_total_vazio, 'valor_unitario'].astype(float)
        )
        logger.info(f"Valores totais calculados para {linhas_vazias} linhas")
    else:
        logger.info("Todos os valores totais já estão preenchidos")
    
    return df


def extract_treat_vendas(csv_file_path: str) -> pd.DataFrame:
    """
    Função principal para extrair e tratar dados de vendas.
    
    Args:
        csv_file_path: Caminho para o arquivo CSV de vendas
        
    Returns:
        pd.DataFrame: DataFrame de vendas tratado
        
    Raises:
        Exception: Se houver erro no processamento
    """
    logger = setup_logging()
    logger.info("Iniciando processamento de vendas")
    
    try:
        # Validar arquivo de entrada
        validate_csv_file(csv_file_path)
        
        # Carregar dados de vendas
        df_vendas = load_vendas_csv(csv_file_path)
        logger.info(f"Dados de vendas carregados: {len(df_vendas)} registros")
        
        # Buscar dados tratados de produtos do banco
        df_produtos = fetch_produtos_from_db()
        logger.info(f"Dados de produtos carregados do banco: {len(df_produtos)} registros")
        
        # Processar dados
        df_vendas = remove_duplicates(df_vendas, ['id_venda'], logger)
        df_vendas = validate_and_fill_dates(df_vendas, logger)
        df_vendas = fill_missing_unit_values(df_vendas, df_produtos, logger)
        df_vendas = calculate_total_values(df_vendas, logger)
        
        logger.info(f"Processamento concluído. Total de vendas processadas: {len(df_vendas)}")
        return df_vendas
        
    except Exception as e:
        logger.error(f"Erro no processamento de vendas: {e}")
        raise


if __name__ == "__main__":
    # Teste da função
    try:
        df_result = extract_treat_vendas('bases-de-dados/vendas.csv')
        print(f"Processamento concluído com sucesso. Registros: {len(df_result)}")
        # Exibe todas as linhas do DataFrame, independentemente do tamanho
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df_result.to_string(index=False))
    except Exception as e:
        print(f"Erro no processamento: {e}")

