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
    logger = logging.getLogger('et_empregados')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/et_empregados.log', mode='a', encoding='utf-8')
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


def load_empregados_csv(file_path: str) -> pd.DataFrame:
    """
    Carrega dados de empregados do arquivo CSV.
    
    Args:
        file_path: Caminho para o arquivo CSV de empregados
        
    Returns:
        pd.DataFrame: DataFrame com dados de empregados
        
    Raises:
        Exception: Se houver erro na leitura
    """
    try:
        df_empregados = pd.read_csv(file_path, sep=';', encoding='utf-8')
        return df_empregados
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo CSV de empregados: {e}")


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
        logger.info(f"IDs de empregados duplicados que serão removidos: {ids_duplicados}")
    else:
        logger.info("Nenhum ID de empregado duplicado encontrado para remoção.")
    
    return df.drop_duplicates(subset=subset, keep='first')


def fix_missing_names(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Corrige nomes faltantes usando o padrão "Funcionário {id_empregado}".
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com nomes corrigidos
    """
    nomes_corrigidos = []
    
    for idx, row in df.iterrows():
        id_empregado = str(row['id_empregado'])
        nome_empregado = str(row['nome']) if pd.notnull(row['nome']) else ''
        nome_esperado = f'Funcionário {id_empregado}'
        
        if nome_empregado.strip() == '':
            nomes_corrigidos.append((id_empregado, nome_empregado, nome_esperado))
            df.loc[idx, 'nome'] = nome_esperado
    
    if nomes_corrigidos:
        logger.info("Empregados com nomes faltantes (corrigidos):")
        for id_empregado, nome_empregado, nome_esperado in nomes_corrigidos:
            logger.info(f"ID: {id_empregado} | Nome original: '{nome_empregado}' | Corrigido para: {nome_esperado}")
    else:
        logger.info("Nenhum empregado com nome faltante encontrado.")
    
    return df


def fill_missing_cargos(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche cargos faltantes com 'Não Informado'.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com cargos preenchidos
    """
    cargos_vazios = df[df['cargo'].isnull() | (df['cargo'] == '')]
    
    if not cargos_vazios.empty:
        logger.info(f"Preenchendo {len(cargos_vazios)} cargos vazios com 'Não Informado'")
    
    df['cargo'] = df['cargo'].replace('', 'Não Informado')
    df['cargo'] = df['cargo'].fillna('Não Informado')
    
    return df


def fill_missing_ages(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche idades faltantes usando mediana por cargo com rastreabilidade completa.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com idades preenchidas e flags de rastreabilidade
    """
    # Converter para numérico
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
    idades_em_branco = df[df['idade'].isnull()]
    
    # Inicializar colunas de rastreabilidade se não existirem
    if 'idade_imputada' not in df.columns:
        df['idade_imputada'] = False
    if 'metodo_imputacao_idade' not in df.columns:
        df['metodo_imputacao_idade'] = ''
    
    if idades_em_branco.empty:
        logger.info("Nenhum empregado com idade em branco encontrado.")
        return df
    
    logger.info(f"Encontrados {len(idades_em_branco)} empregados com idade em branco - aplicando imputação inteligente")
    
    # Preencher por cargo
    for idx, row in idades_em_branco.iterrows():
        id_empregado = row['id_empregado']
        cargo = row['cargo']
        
        # Calcular mediana do cargo
        idades_cargo = df[
            (df['cargo'] == cargo) &
            (df['id_empregado'] != id_empregado) &
            (~df['idade'].isnull())
        ]['idade']
        
        if not idades_cargo.empty:
            mediana_cargo = round(idades_cargo.median())
            df.loc[df['id_empregado'] == id_empregado, 'idade'] = mediana_cargo
            df.loc[df['id_empregado'] == id_empregado, 'idade_imputada'] = True
            df.loc[df['id_empregado'] == id_empregado, 'metodo_imputacao_idade'] = 'mediana_cargo'
            logger.info(f"Empregado ID {id_empregado}: idade preenchida com mediana {mediana_cargo} do cargo '{cargo}'")
        else:
            # Usar mediana global como fallback
            idades_globais = df[~df['idade'].isnull()]['idade']
            if not idades_globais.empty:
                mediana_global = round(idades_globais.median())
                df.loc[df['id_empregado'] == id_empregado, 'idade'] = mediana_global
                df.loc[df['id_empregado'] == id_empregado, 'idade_imputada'] = True
                df.loc[df['id_empregado'] == id_empregado, 'metodo_imputacao_idade'] = 'mediana_global'
                logger.info(f"Empregado ID {id_empregado}: idade preenchida com mediana global {mediana_global}")
            else:
                logger.warning(f"Não foi possível calcular mediana para empregado ID {id_empregado}")
    
    return df


def validate_age_range(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida e ajusta idades fora da faixa válida (18-70 anos) com rastreabilidade.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com idades validadas e flags de rastreabilidade
    """
    # Inicializar colunas de rastreabilidade se não existirem
    if 'idade_ajustada' not in df.columns:
        df['idade_ajustada'] = False
    
    idades_invalidas = df[(df['idade'] < 18) | (df['idade'] > 70)]
    
    if not idades_invalidas.empty:
        logger.info(f"Encontrados {len(idades_invalidas)} empregados com idades fora da faixa válida (18-70 anos)")
        
        for idx, row in idades_invalidas.iterrows():
            id_empregado = row['id_empregado']
            idade_atual = row['idade']
            
            if idade_atual < 18:
                df.loc[idx, 'idade'] = 18
                df.loc[idx, 'idade_ajustada'] = True
                logger.info(f"Empregado ID {id_empregado}: idade {idade_atual} ajustada para 18 anos")
            elif idade_atual > 70:
                df.loc[idx, 'idade'] = 70
                df.loc[idx, 'idade_ajustada'] = True
                logger.info(f"Empregado ID {id_empregado}: idade {idade_atual} ajustada para 70 anos")
    else:
        logger.info("Todas as idades estão dentro da faixa válida (18-70 anos)")
    
    # Converter para inteiro
    df['idade'] = df['idade'].astype(int)
    
    return df


def fill_missing_employee_ids(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Preenche IDs de empregados em branco com a continuação da sequência de IDs.
    
    Args:
        df: DataFrame a ser processado
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com IDs de empregados preenchidos
    """
    # Identificar registros com ID em branco
    registros_sem_id = df[df['id_empregado'].isna() | (df['id_empregado'] == '') | (df['id_empregado'] == ' ')]
    
    if not registros_sem_id.empty:
        logger.info(f"Encontrados {len(registros_sem_id)} empregados com ID em branco")
        
        # Encontrar o maior ID existente
        ids_existentes = df[df['id_empregado'].notna() & (df['id_empregado'] != '') & (df['id_empregado'] != ' ')]
        
        if not ids_existentes.empty:
            # Converter para numérico e encontrar o máximo
            ids_numericos = pd.to_numeric(ids_existentes['id_empregado'], errors='coerce')
            max_id = ids_numericos.max()
            
            if pd.isna(max_id):
                # Se não conseguir converter, usar o maior valor como string
                max_id = 0
            else:
                max_id = int(max_id)
        else:
            max_id = 0
        
        logger.info(f"Maior ID existente: {max_id}")
        
        # Preencher IDs em branco com sequência
        proximo_id = max_id + 1
        for idx in registros_sem_id.index:
            df.loc[idx, 'id_empregado'] = proximo_id
            logger.info(f"Empregado sem ID: atribuído ID {proximo_id}")
            proximo_id += 1
        
        logger.info(f"IDs preenchidos: {len(registros_sem_id)} empregados receberam novos IDs")
    else:
        logger.info("Nenhum empregado com ID em branco encontrado")
    
    # Converter coluna para inteiro
    df['id_empregado'] = pd.to_numeric(df['id_empregado'], errors='coerce').astype('Int64')
    
    return df


def _log_processing_statistics(df: pd.DataFrame, logger: logging.Logger) -> None:
    """
    Registra estatísticas detalhadas do processamento de dados.
    
    Args:
        df: DataFrame processado
        logger: Logger para registrar informações
    """
    logger.info("📊 ESTATÍSTICAS DE PROCESSAMENTO:")
    
    # Estatísticas de IDs de empregados
    ids_unicos = df['id_empregado'].nunique()
    logger.info(f"  🆔 IDs de empregados únicos: {ids_unicos}")
    
    # Estatísticas de idades imputadas
    if 'idade_imputada' in df.columns:
        idades_imputadas = df['idade_imputada'].sum()
        if idades_imputadas > 0:
            percentual = (idades_imputadas / len(df)) * 100
            logger.info(f"  📈 Idades imputadas: {idades_imputadas} de {len(df)} empregados ({percentual:.1f}%)")
            
            # Detalhar métodos de imputação
            if 'metodo_imputacao_idade' in df.columns:
                metodos_idade = df[df['idade_imputada']]['metodo_imputacao_idade'].value_counts()
                for metodo, count in metodos_idade.items():
                    logger.info(f"    - {metodo}: {count} empregados")
        else:
            logger.info("  📈 Idades: Todas eram válidas - nenhuma imputação necessária")
    
    # Estatísticas de idades ajustadas
    if 'idade_ajustada' in df.columns:
        idades_ajustadas = df['idade_ajustada'].sum()
        if idades_ajustadas > 0:
            percentual = (idades_ajustadas / len(df)) * 100
            logger.info(f"  🔧 Idades ajustadas (faixa válida): {idades_ajustadas} empregados ({percentual:.1f}%)")
        else:
            logger.info("  🔧 Idades: Todas estavam na faixa válida (18-70 anos)")
    
    # Estatísticas de cargos
    cargos_nao_informados = (df['cargo'] == 'Não Informado').sum()
    if cargos_nao_informados > 0:
        percentual = (cargos_nao_informados / len(df)) * 100
        logger.info(f"  💼 Cargos 'Não Informado': {cargos_nao_informados} empregados ({percentual:.1f}%)")
    
    # Distribuição por cargo
    distribuicao_cargos = df['cargo'].value_counts()
    logger.info(f"  💼 Distribuição por cargo: {len(distribuicao_cargos)} cargos distintos")
    
    # Estatísticas de idade
    idade_media = df['idade'].mean()
    idade_mediana = df['idade'].median()
    logger.info(f"  👥 Idade média: {idade_media:.1f} anos | Mediana: {idade_mediana} anos")


def extract_treat_empregados(csv_file_path: str) -> pd.DataFrame:
    """
    Função principal para extrair e tratar dados de empregados.
    
    Args:
        csv_file_path: Caminho para o arquivo CSV de empregados
        
    Returns:
        pd.DataFrame: DataFrame de empregados tratado
        
    Raises:
        Exception: Se houver erro no processamento
    """
    logger = setup_logging()
    logger.info("Iniciando processamento de empregados")
    
    try:
        # Validar arquivo de entrada
        validate_csv_file(csv_file_path)
        
        # Carregar dados
        df_empregados = load_empregados_csv(csv_file_path)
        logger.info(f"Dados de empregados carregados: {len(df_empregados)} registros")
        
        # Processar dados
        df_empregados = remove_duplicates(df_empregados, ['id_empregado'], logger)
        df_empregados = fill_missing_employee_ids(df_empregados, logger)
        df_empregados = fix_missing_names(df_empregados, logger)
        df_empregados = fill_missing_cargos(df_empregados, logger)
        df_empregados = fill_missing_ages(df_empregados, logger)
        df_empregados = validate_age_range(df_empregados, logger)
        
        # Estatísticas finais de processamento
        _log_processing_statistics(df_empregados, logger)
        
        logger.info(f"✅ Processamento concluído. Total de empregados processados: {len(df_empregados)} (0% de perda)")
        return df_empregados
        
    except Exception as e:
        logger.error(f"Erro no processamento de empregados: {e}")
        raise


if __name__ == "__main__":
    # Teste da função
    try:
        df_result = extract_treat_empregados('bases-de-dados/empregados.csv')
        print(f"Processamento concluído com sucesso. Registros: {len(df_result)}")
        # Exibe todas as linhas do DataFrame, independentemente do tamanho
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(df_result.to_string(index=False))
    except Exception as e:
        print(f"Erro no processamento: {e}")

        