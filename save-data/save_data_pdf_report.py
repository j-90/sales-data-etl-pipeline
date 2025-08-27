import pandas as pd
import os
import re
import logging
from typing import Dict, List, Tuple
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime
import numpy as np
import io


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
    logger = logging.getLogger('save_data_pdf_report')
    logger.setLevel(logging.INFO)
    
    # Evitar handlers duplicados
    if not logger.handlers:
        file_handler = logging.FileHandler('logs/relatorio_pdf.log', mode='a', encoding='utf-8')
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
        'database': 'comercial',
        'user': 'visitante',
        'password': 'teste'
    }
    
    return f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"


def fetch_sales_by_employee(engine, logger: logging.Logger) -> pd.DataFrame:
    """
    Busca total de vendas por funcionário.
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com vendas por funcionário
    """
    query = text("""
        SELECT 
            e.id_empregado,
            e.nome,
            COALESCE(SUM(v.valor_total), 0) AS valor_total
        FROM vendas v
        INNER JOIN empregados e
            ON v.id_empregado = e.id_empregado
        GROUP BY e.id_empregado, e.nome
        ORDER BY valor_total DESC
    """)
    
    df = pd.read_sql_query(query, engine)
    logger.info(f"Dados de vendas por funcionário carregados: {len(df)} registros")
    return df


def fetch_average_ticket_by_product(engine, logger: logging.Logger) -> pd.DataFrame:
    """
    Busca ticket médio por produto.
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com ticket médio por produto
    """
    query = text("""
        SELECT 
            p.id_produto,
            p.nome,
            COALESCE(SUM(v.valor_total) / COUNT(v.id_venda), 0) AS ticket_medio
        FROM vendas v
        INNER JOIN produtos p
            ON v.id_produto = p.id_produto
        GROUP BY p.id_produto, p.nome
    """)
    
    df = pd.read_sql_query(query, engine)
    
    # Ordenar pelo número do produto extraído do nome
    df['numero_produto'] = df['nome'].apply(extract_product_number)
    df = df.sort_values(by='numero_produto').reset_index(drop=True)
    df = df.drop(columns=['numero_produto'])
    
    logger.info(f"Dados de ticket médio por produto carregados: {len(df)} registros")
    return df


def extract_product_number(product_name: str) -> int:
    """
    Extrai o número do produto do nome.
    
    Args:
        product_name: Nome do produto
        
    Returns:
        int: Número do produto ou infinito se não encontrar
    """
    match = re.search(r'(\d+)', str(product_name))
    if match:
        return int(match.group(1))
    else:
        return float('inf')  # Se não encontrar número, coloca no final


def fetch_sales_by_category(engine, logger: logging.Logger) -> pd.DataFrame:
    """
    Busca quantidade de vendas por categoria de produto.
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com vendas por categoria
    """
    query = text("""
        SELECT
            p.categoria,
            COUNT(v.id_venda) AS quantidade_vendas
        FROM vendas v
        INNER JOIN produtos p 
            ON v.id_produto = p.id_produto
        GROUP BY p.categoria
        ORDER BY quantidade_vendas DESC
    """)
    
    df = pd.read_sql_query(query, engine)
    logger.info(f"Dados de vendas por categoria carregados: {len(df)} registros")
    return df


def fetch_top5_employees(engine, logger: logging.Logger) -> pd.DataFrame:
    """
    Busca top 5 funcionários com maior volume de vendas.
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com top 5 funcionários
    """
    query = text("""
        SELECT 
            e.id_empregado,
            e.nome,
            COALESCE(SUM(v.valor_total), 0) AS valor_total
        FROM empregados e
        INNER JOIN vendas v 
            ON v.id_empregado = e.id_empregado
        GROUP BY e.id_empregado, e.nome
        ORDER BY valor_total DESC
        LIMIT 5
    """)
    
    df = pd.read_sql_query(query, engine)
    logger.info(f"Dados de top 5 funcionários carregados: {len(df)} registros")
    return df


def fetch_sales_by_period(engine, logger: logging.Logger) -> pd.DataFrame:
    """
    Busca quantidade de vendas por período (mês).
    
    Args:
        engine: Engine SQLAlchemy para conexão com o banco
        logger: Logger para registrar informações
        
    Returns:
        pd.DataFrame: DataFrame com vendas por período
    """
    query = text("""
        SELECT 
            TO_CHAR(v.data, 'YYYY-MM') AS periodo,
            COUNT(v.id_venda) AS quantidade_vendas,
            SUM(v.valor_total) AS valor_total
        FROM vendas v
        WHERE v.data IS NOT NULL
        GROUP BY TO_CHAR(v.data, 'YYYY-MM')
        ORDER BY periodo
    """)
    
    df = pd.read_sql_query(query, engine)
    logger.info(f"Dados de vendas por período carregados: {len(df)} registros")
    return df


def create_horizontal_bar_chart(data: pd.DataFrame, title: str, logger: logging.Logger) -> plt.Figure:
    """
    Cria gráfico de barras horizontais.
    
    Args:
        data: DataFrame com os dados
        title: Título do gráfico
        logger: Logger para registrar informações
        
    Returns:
        plt.Figure: Figura do matplotlib
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Criar gráfico de barras horizontais
    bars = ax.barh(range(len(data)), data.iloc[:, 1], color='lightcoral', edgecolor='darkred', alpha=0.8)
    
    # Configurar eixos
    ax.set_xlabel('Quantidade de Vendas', fontsize=12)
    ax.set_ylabel('Categorias', fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Configurar labels do eixo Y
    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(data.iloc[:, 0])
    
    # Adicionar valores nas barras
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                f'{width}', ha='left', va='center', fontweight='bold')
    
    # Inverter eixo Y para mostrar maior valor no topo
    ax.invert_yaxis()
    
    plt.tight_layout()
    
    logger.info(f"Gráfico de barras horizontais criado: {title}")
    return fig


def create_bar_chart(data: pd.DataFrame, title: str, logger: logging.Logger) -> plt.Figure:
    """
    Cria gráfico de barras.
    
    Args:
        data: DataFrame com os dados
        title: Título do gráfico
        logger: Logger para registrar informações
        
    Returns:
        plt.Figure: Figura do matplotlib
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Criar gráfico de barras
    bars = ax.bar(range(len(data)), data.iloc[:, 1], color='skyblue', edgecolor='navy', alpha=0.7)
    
    # Configurar eixos
    ax.set_xlabel('Funcionários', fontsize=12)
    ax.set_ylabel('Valor Total (R$)', fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Configurar labels do eixo X
    ax.set_xticks(range(len(data)))
    ax.set_xticklabels(data.iloc[:, 0], rotation=45, ha='right')
    
    # Adicionar valores nas barras
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'R$ {height:,.0f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    logger.info(f"Gráfico de barras criado: {title}")
    return fig


def create_line_chart(data: pd.DataFrame, title: str, logger: logging.Logger) -> plt.Figure:
    """
    Cria gráfico de linha para vendas por período.
    
    Args:
        data: DataFrame com os dados
        title: Título do gráfico
        logger: Logger para registrar informações
        
    Returns:
        plt.Figure: Figura do matplotlib
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Criar gráfico de linha
    ax.plot(range(len(data)), data['quantidade_vendas'], marker='o', linewidth=2, markersize=8, color='green')
    
    # Configurar eixos
    ax.set_xlabel('Período', fontsize=12)
    ax.set_ylabel('Quantidade de Vendas', fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Configurar labels do eixo X
    ax.set_xticks(range(len(data)))
    ax.set_xticklabels(data['periodo'], rotation=45, ha='right')
    
    # Calcular limites do eixo Y para garantir que todos os valores fiquem visíveis
    y_values = data['quantidade_vendas']
    y_min = y_values.min()
    y_max = y_values.max()
    y_range = y_max - y_min
    
    # Adicionar margem maior acima (25%) e menor abaixo (10%) para garantir visibilidade
    y_margin_top = y_range * 0.25  # Margem maior no topo
    y_margin_bottom = y_range * 0.10  # Margem menor embaixo
    ax.set_ylim(y_min - y_margin_bottom, y_max + y_margin_top)
    
    # Adicionar valores nos pontos com posicionamento dinâmico
    for i, (x, y) in enumerate(zip(range(len(data)), data['quantidade_vendas'])):
        # Determinar se o texto deve ficar acima ou abaixo do ponto
        if i == len(data) - 1:  # Último ponto (maio)
            # Para o último ponto, colocar o texto abaixo
            ax.text(x, y - y*0.08, str(y), ha='center', va='top', fontweight='bold')
        else:
            # Para os outros pontos, colocar o texto acima
            ax.text(x, y + y*0.05, str(y), ha='center', va='bottom', fontweight='bold')
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    logger.info(f"Gráfico de linha criado: {title}")
    return fig


def figure_to_reportlab_image(fig: plt.Figure, width: float = 6*inch, height: float = 4*inch) -> Image:
    """
    Converte uma figura matplotlib em um objeto Image do ReportLab.
    
    Args:
        fig: Figura do matplotlib
        width: Largura da imagem no PDF
        height: Altura da imagem no PDF
        
    Returns:
        Image: Objeto Image do ReportLab
    """
    # Salvar figura em buffer de memória
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    img_buffer.seek(0)
    
    # Criar objeto Image do ReportLab
    img = Image(img_buffer, width=width, height=height)
    
    # Fechar figura para liberar memória
    plt.close(fig)
    
    return img


def setup_unicode_fonts():
    """
    Configura fontes Unicode para suporte a caracteres especiais.
    """
    # Lista de fontes Unicode para tentar
    unicode_fonts = [
        # Fontes DejaVu (Linux)
        ('DejaVu', 'DejaVuSansCondensed.ttf'),
        ('DejaVu', '/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf'),
        ('DejaVu', '/System/Library/Fonts/DejaVuSansCondensed.ttf'),
        
        # Fontes Arial Unicode (Windows)
        ('ArialUnicode', 'arial.ttf'),
        ('ArialUnicode', 'C:/Windows/Fonts/arial.ttf'),
        ('ArialUnicode', 'C:/Windows/Fonts/ARIAL.TTF'),
        
        # Fontes Helvetica com suporte Unicode
        ('Helvetica', None),  # Fonte padrão do ReportLab
    ]
    
    for font_name, font_path in unicode_fonts:
        try:
            if font_path:
                pdfmetrics.registerFont(TTFont(font_name, font_path))
            return font_name
        except:
            continue
    
    # Fallback para fonte padrão
    return 'Helvetica'


def create_unicode_styles():
    """
    Cria estilos Unicode para o relatório.
    
    Returns:
        dict: Dicionário com estilos configurados
    """
    font_name = setup_unicode_fonts()
    
    styles = getSampleStyleSheet()
    
    # Estilo para título principal
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontName=font_name,
        fontSize=18,
        spaceAfter=8,  # Reduzido de 20 para 8
        alignment=1,  # Centralizado
        textColor=colors.white,
        backColor=colors.HexColor('#2980b9')
    )
    
    # Estilo para títulos de seção
    section_style = ParagraphStyle(
        'CustomSection',
        parent=styles['Heading2'],
        fontName=font_name,
        fontSize=14,
        spaceAfter=6,  # Reduzido de 10 para 6
        textColor=colors.HexColor('#2980b9'),
        backColor=colors.HexColor('#e6e6fa')
    )
    
    # Estilo para resumos
    summary_style = ParagraphStyle(
        'CustomSummary',
        parent=styles['Normal'],
        fontName=font_name,
        fontSize=10,
        spaceAfter=6,  # Reduzido de 10 para 6
        textColor=colors.grey,
        fontNameItalic=font_name
    )
    
    # Estilo para observações importantes
    warning_style = ParagraphStyle(
        'CustomWarning',
        parent=styles['Normal'],
        fontName=font_name,
        fontSize=10,
        spaceAfter=10,
        textColor=colors.red,
        fontNameBold=font_name
    )
    
    return {
        'title': title_style,
        'section': section_style,
        'summary': summary_style,
        'warning': warning_style,
        'font_name': font_name
    }

def create_sales_report(dataframes: Dict[str, pd.DataFrame], logger: logging.Logger) -> None:
    """
    Cria o relatório PDF com os dados fornecidos usando ReportLab.
    
    Args:
        dataframes: Dicionário com os DataFrames dos dados
        logger: Logger para registrar informações
    """
    # Configurar estilos Unicode
    styles = create_unicode_styles()
    
    # Criar diretório se não existir
    if not os.path.exists('pdf-files'):
        os.makedirs('pdf-files')
    
    output_path = "pdf-files/relatorio-final.pdf"
    doc = SimpleDocTemplate(output_path, pagesize=A4)
    
    # Lista para armazenar elementos do PDF
    story = []
    
    # Título principal
    logger.info("Gerando título principal")
    title = Paragraph("Relatório de Vendas", styles['title'])
    story.append(title)
    story.append(Spacer(1, 3))  # Reduzido de 5 para 3
    
    # Total de vendas por funcionário
    logger.info("Gerando seção: Total de vendas por funcionário")
    section_title = Paragraph("Total de vendas por funcionário", styles['section'])
    story.append(section_title)
    
    summary_vendas_funcionario = (
        "Este indicador apresenta o volume total de vendas realizadas por cada funcionário, "
        "permitindo identificar os colaboradores com maior contribuição para a receita da empresa. "
        "Os valores são calculados somando todas as vendas associadas a cada funcionário."
    )
    summary = Paragraph(summary_vendas_funcionario, styles['summary'])
    story.append(summary)
    
    # Tabela de vendas por funcionário
    table_data = [["Nome do Funcionário", "Total de Vendas (R$)"]]
    for _, row in dataframes['vendas_por_funcionario'].iterrows():
        table_data.append([row['nome'], f"{row['valor_total']:.2f}"])
    
    table = Table(table_data, colWidths=[4*inch, 2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8ddf2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2980b9')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), styles['font_name']),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # Reduzido de 12 para 6
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), styles['font_name']),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 1), (-1, -1), 6),  # Adicionado padding superior reduzido
    ]))
    story.append(table)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12

    # Ticket médio por produto
    logger.info("Gerando seção: Ticket médio por produto")
    section_title = Paragraph("Ticket médio por produto", styles['section'])
    story.append(section_title)
    
    summary_ticket_medio = (
        "O ticket médio por produto representa o valor médio de cada venda para cada produto, "
        "calculado dividindo o total de vendas pelo número de transações. "
        "Este indicador ajuda a identificar produtos com maior valor agregado."
    )
    summary = Paragraph(summary_ticket_medio, styles['summary'])
    story.append(summary)
    
    # Tabela de ticket médio
    table_data = [["Nome do Produto", "Ticket Médio (R$)"]]
    for _, row in dataframes['ticket_medio_produto'].iterrows():
        table_data.append([row['nome'], f"{row['ticket_medio']:.2f}"])
    
    table = Table(table_data, colWidths=[4*inch, 2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8ddf2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2980b9')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), styles['font_name']),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # Reduzido de 12 para 6
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), styles['font_name']),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 1), (-1, -1), 6),  # Adicionado padding superior reduzido
    ]))
    story.append(table)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12

    # Nova página para vendas por categoria
    story.append(PageBreak())
    
    # Quantidade de vendas por categoria de produto
    logger.info("Gerando seção: Quantidade de vendas por categoria")
    section_title = Paragraph("Quantidade de vendas por categoria de produto", styles['section'])
    story.append(section_title)
    
    summary_vendas_categoria = (
        "Este indicador mostra a distribuição das vendas por categoria de produto, "
        "permitindo identificar quais categorias são mais populares entre os clientes. "
        "A análise ajuda no planejamento de estoque e estratégias de marketing."
    )
    summary = Paragraph(summary_vendas_categoria, styles['summary'])
    story.append(summary)
    
    # Tabela de vendas por categoria
    table_data = [["Categoria", "Qtd. Vendas"]]
    for _, row in dataframes['vendas_por_categoria'].iterrows():
        table_data.append([row['categoria'], str(row['quantidade_vendas'])])
    
    table = Table(table_data, colWidths=[4*inch, 2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8ddf2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2980b9')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), styles['font_name']),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # Reduzido de 12 para 6
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), styles['font_name']),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 1), (-1, -1), 6),  # Adicionado padding superior reduzido
    ]))
    story.append(table)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12
    
    # Gráfico de barras horizontais para vendas por categoria
    logger.info("Criando gráfico de barras horizontais para vendas por categoria")
    horizontal_bar_fig = create_horizontal_bar_chart(
        dataframes['vendas_por_categoria'], 
        "Vendas por Categoria de Produto", 
        logger
    )
    img = figure_to_reportlab_image(horizontal_bar_fig, width=6*inch, height=4*inch)
    story.append(img)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12

    # Nova página para top 5 funcionários
    story.append(PageBreak())
    
    # Top 5 funcionários com maior volume de vendas
    logger.info("Gerando seção: Top 5 funcionários")
    section_title = Paragraph("Top 5 funcionários com maior volume de vendas", styles['section'])
    story.append(section_title)
    
    summary_top5 = (
        "Apresenta os 5 funcionários com maior volume total de vendas, destacando os "
        "colaboradores de alto desempenho. Esta análise é útil para reconhecimento, "
        "treinamento e definição de metas de vendas."
    )
    summary = Paragraph(summary_top5, styles['summary'])
    story.append(summary)
    
    # Tabela de top 5 funcionários
    table_data = [["Nome do Funcionário", "Total de Vendas (R$)"]]
    for _, row in dataframes['top5_funcionarios'].iterrows():
        table_data.append([row['nome'], f"{row['valor_total']:.2f}"])
    
    table = Table(table_data, colWidths=[4*inch, 2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8ddf2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2980b9')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), styles['font_name']),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # Reduzido de 12 para 6
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), styles['font_name']),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 1), (-1, -1), 6),  # Adicionado padding superior reduzido
    ]))
    story.append(table)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12
    
    # Gráfico de barras para top 5 funcionários
    logger.info("Criando gráfico de barras para top 5 funcionários")
    bar_chart_fig = create_bar_chart(
        dataframes['top5_funcionarios'], 
        "Top 5 Funcionários - Volume de Vendas", 
        logger
    )
    img = figure_to_reportlab_image(bar_chart_fig, width=6*inch, height=4*inch)
    story.append(img)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12
    
    # Nova página para vendas por período
    story.append(PageBreak())
    
    # Quantidade de vendas por período
    logger.info("Gerando seção: Vendas por período")
    section_title = Paragraph("Quantidade de vendas por período", styles['section'])
    story.append(section_title)
    
    summary_vendas_periodo = (
        "Este indicador apresenta a evolução das vendas ao longo do tempo, "
        "permitindo identificar tendências sazonais e padrões de crescimento. "
        "A análise temporal ajuda no planejamento estratégico e previsões."
    )
    summary = Paragraph(summary_vendas_periodo, styles['summary'])
    story.append(summary)
    
    # Observação sobre tratamento de datas
    warning_text = (
        "⚠️ OBSERVAÇÃO IMPORTANTE: Devido ao tratamento aplicado nas datas da tabela de vendas "
        "(imputação de datas ausentes), pode haver variação de até 15% nos dados apresentados. "
        "As datas foram tratadas usando mediana por funcionário e global para garantir "
        "a integridade dos dados sem perda de informações."
    )
    warning = Paragraph(warning_text, styles['warning'])
    story.append(warning)
    story.append(Spacer(1, 6))  # Reduzido de 10 para 6
    
    # Tabela de vendas por período
    table_data = [["Período", "Qtd. Vendas", "Valor Total (R$)"]]
    for _, row in dataframes['vendas_por_periodo'].iterrows():
        table_data.append([
            row['periodo'], 
            str(row['quantidade_vendas']), 
            f"{row['valor_total']:.2f}"
        ])
    
    table = Table(table_data, colWidths=[2*inch, 2*inch, 2.5*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8ddf2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2980b9')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), styles['font_name']),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),  # Reduzido de 12 para 6
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), styles['font_name']),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 1), (-1, -1), 6),  # Adicionado padding superior reduzido
    ]))
    story.append(table)
    story.append(Spacer(1, 12))  # Reduzido de 20 para 12
    
    # Gráfico de linha para vendas por período
    logger.info("Criando gráfico de linha para vendas por período")
    line_chart_fig = create_line_chart(
        dataframes['vendas_por_periodo'], 
        "Evolução de Vendas por Período", 
        logger
    )
    img = figure_to_reportlab_image(line_chart_fig, width=6*inch, height=4*inch)
    story.append(img)

    # Gerar o PDF
    doc.build(story)
    logger.info(f"Relatório PDF gerado com sucesso: {output_path}")


def save_pdf_report() -> None:
    """
    Gera um relatório em PDF com:
    - Total de vendas por funcionário
    - Ticket médio por produto
    - Quantidade de vendas por categoria de produto (com gráfico de barras horizontais)
    - Top 5 funcionários com maior volume de vendas (com gráfico de barras)
    - Quantidade de vendas por período (com gráfico de linha)
    
    Raises:
        Exception: Se houver erro na operação
    """
    logger = setup_logging()
    logger.info("Iniciando geração do relatório PDF")
    
    try:
        # Estabelecer conexão com o banco
        connection_string = get_database_connection()
        engine = create_engine(connection_string)
        logger.info("Conexão com banco de dados estabelecida")
        
        # Buscar dados das tabelas
        dataframes = {
            'vendas_por_funcionario': fetch_sales_by_employee(engine, logger),
            'ticket_medio_produto': fetch_average_ticket_by_product(engine, logger),
            'vendas_por_categoria': fetch_sales_by_category(engine, logger),
            'top5_funcionarios': fetch_top5_employees(engine, logger),
            'vendas_por_periodo': fetch_sales_by_period(engine, logger)
        }
        
        # Criar relatório PDF
        create_sales_report(dataframes, logger)
        
        # Fechar conexão
        engine.dispose()
        logger.info("Conexão com banco de dados fechada")
        logger.info("Geração do relatório PDF concluída com sucesso")
        
    except Exception as e:
        logger.error(f"Erro na geração do relatório PDF: {e}")
        logger.error("Verifique se o banco de dados está acessível e as tabelas existem")
        raise


if __name__ == "__main__":
    try:
        save_pdf_report()
    except Exception as e:
        print(f"Erro na geração do relatório PDF: {e}")
        exit(1)

