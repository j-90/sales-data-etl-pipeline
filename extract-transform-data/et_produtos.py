import pandas as pd
import logging
import os
from typing import List, Tuple


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
    logger = logging.getLogger('et_produtos')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/et_produtos.log', mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


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


def load_produtos_csv(file_path: str) -> pd.DataFrame:
    """
    Carrega dados de produtos do arquivo CSV.
    
    Args:
        file_path: Caminho para o arquivo CSV de produtos
        
    Returns:
        pd.DataFrame: DataFrame com dados de produtos
        
    Raises:
        Exception: Se houver erro na leitura
    """
    try:
        df_produtos = pd.read_csv(file_path, sep=';', encoding='utf-8')
        return df_produtos
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo CSV de produtos: {e}")


def remove_duplicates(df: pd.DataFrame, subset: List[str], logger: logging.Logger) -> pd.DataFrame:
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
        logger.info(f"IDs de produtos duplicados que serão removidos: {ids_duplicados}")
    else:
        logger.info("Nenhum ID de produto duplicado encontrado para remoção.")
    
    return df.drop_duplicates(subset=subset, keep='first')


def fix_product_names(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Corrige nomes de produtos para seguir o padrão "Produto {id_produto}".
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com nomes corrigidos
    """
    inconsistentes = []
    
    for idx, row in df.iterrows():
        id_produto = str(row['id_produto'])
        nome_produto = str(row['nome'])
        nome_esperado = f'Produto {id_produto}'
        
        if nome_produto != nome_esperado:
            inconsistentes.append((id_produto, nome_produto, nome_esperado))
            df.loc[idx, 'nome'] = nome_esperado
    
    if inconsistentes:
        logger.info("Produtos com nomes inconsistentes (corrigidos):")
        for id_produto, nome_produto, nome_esperado in inconsistentes:
            logger.info(f"ID: {id_produto} | Nome original: {nome_produto} | Corrigido para: {nome_esperado}")
    else:
        logger.info("Todos os nomes de produtos estavam de acordo com o padrão.")
    
    return df


def fill_missing_categories(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche categorias faltantes com 'Desconhecida'.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com categorias preenchidas
    """
    categorias_vazias = df[df['categoria'].isnull() | (df['categoria'] == '')]
    
    if not categorias_vazias.empty:
        logger.info(f"Preenchendo {len(categorias_vazias)} categorias vazias com 'Desconhecida'")
    
    df['categoria'] = df['categoria'].replace('', 'Desconhecida')
    df['categoria'] = df['categoria'].fillna('Desconhecida')
    
    return df


def fill_missing_prices(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche preços faltantes usando mediana por categoria.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com preços preenchidos
    """
    # Converter para numérico
    df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
    precos_em_branco = df[df['preco'].isnull()]
    
    if precos_em_branco.empty:
        logger.info("Nenhum produto com preço em branco encontrado.")
        return df
    
    logger.info(f"Encontrados {len(precos_em_branco)} produtos com preço em branco")
    
    # Preencher por categoria
    for idx, row in precos_em_branco.iterrows():
        id_produto = row['id_produto']
        categoria = row['categoria']
        
        # Calcular mediana da categoria
        precos_categoria = df[
            (df['categoria'] == categoria) &
            (df['id_produto'] != id_produto) &
            (~df['preco'].isnull())
        ]['preco']
        
        if not precos_categoria.empty:
            mediana_categoria = round(precos_categoria.median(), 2)
            df.loc[df['id_produto'] == id_produto, 'preco'] = mediana_categoria
            logger.info(f"Produto ID {id_produto}: preço preenchido com mediana {mediana_categoria} da categoria '{categoria}'")
        else:
            logger.warning(f"Não foi possível calcular mediana para categoria '{categoria}' - produto ID {id_produto}")
    
    return df


def extract_treat_produtos(csv_file_path: str) -> pd.DataFrame:
    """
    Função principal para extrair e tratar dados de produtos.
    
    Args:
        csv_file_path: Caminho para o arquivo CSV de produtos
        
    Returns:
        pd.DataFrame: DataFrame de produtos tratado
        
    Raises:
        Exception: Se houver erro no processamento
    """
    logger = setup_logging()
    logger.info("Iniciando processamento de produtos")
    
    try:
        # Validar arquivo de entrada
        validate_csv_file(csv_file_path)
        
        # Carregar dados
        df_produtos = load_produtos_csv(csv_file_path)
        logger.info(f"Dados de produtos carregados: {len(df_produtos)} registros")
        
        # Processar dados
        df_produtos = remove_duplicates(df_produtos, ['id_produto'], logger)
        df_produtos = fix_product_names(df_produtos, logger)
        df_produtos = fill_missing_categories(df_produtos, logger)
        df_produtos = fill_missing_prices(df_produtos, logger)
        
        logger.info(f"Processamento concluído. Total de produtos processados: {len(df_produtos)}")
        return df_produtos
        
    except Exception as e:
        logger.error(f"Erro no processamento de produtos: {e}")
        raise


if __name__ == "__main__":
    # Teste da função
    try:
        df_result = extract_treat_produtos('bases-de-dados/produtos.csv')
        print(f"Processamento concluído com sucesso. Registros: {len(df_result)}")
        # Exibe todas as linhas do DataFrame, independentemente do tamanho
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df_result.to_string(index=False))
    except Exception as e:
        print(f"Erro no processamento: {e}")

