import logging
import sys
import os
import importlib
import time

def main():
    # Configuração do logging
    logger = logging.getLogger("pipeline_etl")
    logger.setLevel(logging.INFO)
    # Garante diretório de logs
    if not os.path.exists('logs'):
        os.makedirs('logs')
    handler = logging.FileHandler("logs/pipeline_etl.log", mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    logger.info("Iniciando pipeline ETL...")
    pipeline_started_at = time.perf_counter()

    # Ajusta sys.path para permitir import de módulos em pastas com hífen
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extra_paths = [
        os.path.join(base_dir, 'create-user-database'),
        os.path.join(base_dir, 'extract-transform-data'),
        os.path.join(base_dir, 'load-data'),
        os.path.join(base_dir, 'save-data'),
    ]
    for p in extra_paths:
        if p not in sys.path:
            sys.path.append(p)

    # Caminhos dos CSVs de entrada
    caminho_produtos = os.path.join('bases-de-dados', 'produtos.csv')
    caminho_vendas = os.path.join('bases-de-dados', 'vendas.csv')
    caminho_empregados = os.path.join('bases-de-dados', 'empregados.csv')

    # Verificações iniciais de entrada
    for nome, caminho in {
        'produtos.csv': caminho_produtos,
        'vendas.csv': caminho_vendas,
        'empregados.csv': caminho_empregados,
    }.items():
        if not os.path.exists(caminho):
            logger.exception(f"Arquivo de entrada ausente: {caminho} ({nome})")
            sys.exit(1)

    def run_step(step_num: int, titulo: str, module_name: str, func_name: str, *args, **kwargs):
        logger.info(f"{step_num} - {titulo}")
        started_at = time.perf_counter()
        try:
            modulo = importlib.import_module(module_name)
            func = getattr(modulo, func_name)
            resultado = func(*args, **kwargs)
            elapsed = time.perf_counter() - started_at
            # Log de sucesso e, se aplicável, contagem de registros
            if hasattr(resultado, 'shape') and hasattr(resultado, 'columns'):
                try:
                    logger.info(f"{titulo} concluído em {elapsed:.2f}s | Registros: {len(resultado)} | Colunas: {len(resultado.columns)}")
                except Exception:
                    logger.info(f"{titulo} concluído em {elapsed:.2f}s")
            else:
                logger.info(f"{titulo} concluído em {elapsed:.2f}s")
            return resultado
        except Exception as exc:
            logger.exception(f"Falha em '{titulo}': {exc}")
            sys.exit(1)

    # 1 - Create user e database
    run_step(1, 'Executando create_user_db', 'create_user_database', 'create_user_db')

    # 2 - ET Produtos
    df_produtos = run_step(2, 'Executando tratamento de produtos (ET)', 'et_produtos', 'extract_treat_produtos', caminho_produtos)

    # 3 - LOAD Produtos
    run_step(3, 'Executando carga de produtos (LOAD)', 'l_produtos', 'loading_produtos_postgresql', df_produtos)

    # 4 - ET Vendas
    df_vendas = run_step(4, 'Executando tratamento de vendas (ET)', 'et_vendas', 'extract_treat_vendas', caminho_vendas)

    # 5 - LOAD Vendas
    run_step(5, 'Executando carga de vendas (LOAD)', 'l_vendas', 'loading_vendas_postgresql', df_vendas)

    # 6 - ET Empregados
    df_empregados = run_step(6, 'Executando tratamento de empregados (ET)', 'et_empregados', 'extract_treat_empregados', caminho_empregados)

    # 7 - LOAD Empregados
    run_step(7, 'Executando carga de empregados (LOAD)', 'l_empregados', 'loading_empregados_postgresql', df_empregados)

    # 8 - Exportação para Parquet
    run_step(8, 'Executando save_data_parquet', 'save_data_parquet', 'save_parquet')

    # 9 - Relatório PDF
    run_step(9, 'Executando save_data_pdf_report', 'save_data_pdf_report', 'save_pdf_report')

    total_elapsed = time.perf_counter() - pipeline_started_at
    logger.info(f"Pipeline ETL finalizado com sucesso em {total_elapsed:.2f}s.")

if __name__ == "__main__":
    main()

