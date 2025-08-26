# 📊 Pipeline ETL - Análise de Dados de Vendas

## 📋 Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) completo para análise de dados de vendas, incluindo processamento de produtos, vendas e empregados, com geração de relatórios em PDF e exportação para formato Parquet.

## 🏗️ Arquitetura do Sistema

### Estrutura de Diretórios
```
arquivos_teste_dados_bus2/
├── bases-de-dados/           # Arquivos CSV de entrada
│   ├── produtos.csv
│   ├── vendas.csv
│   └── empregados.csv
├── create-user-database/     # Criação de usuário e banco
│   └── create_user_database.py
├── extract-transform-data/   # Processamento ETL
│   ├── et_produtos.py
│   ├── et_vendas.py
│   └── et_empregados.py
├── load-data/               # Carregamento no banco
│   ├── l_produtos.py
│   ├── l_vendas.py
│   └── l_empregados.py
├── save-data/               # Exportação e relatórios
│   ├── save_data_parquet.py
│   └── save_data_pdf_report.py
├── parquet-files/           # Arquivos Parquet gerados
├── pdf-files/               # Relatórios PDF
├── logs/                    # Logs de execução
└── pipeline.py              # Pipeline principal
```

## 🚀 Como Executar

### Pré-requisitos
- Python 3.8+
- PostgreSQL 12+
- Bibliotecas Python (instalar via `pip install -r requirements.txt`)

### Execução Completa
```bash
python pipeline.py
```

### Execução Individual
```bash
# Criar usuário e banco
python create-user-database/create_user_database.py

# Processar produtos
python extract-transform-data/et_produtos.py

# Processar vendas
python extract-transform-data/et_vendas.py

# Processar empregados
python extract-transform-data/et_empregados.py

# Gerar relatório PDF
python save-data/save_data_pdf_report.py

# Exportar para Parquet
python save-data/save_data_parquet.py
```

## 📊 Especificações dos Dados

### 1. Produtos (produtos.csv)
**Formato:** CSV com separador `;` e encoding UTF-8

**Colunas:**
- `id_produto`: Identificador único do produto
- `nome`: Nome do produto
- `preco`: Preço unitário (decimal)
- `categoria`: Categoria do produto

**Critérios de Tratamento:**
- ✅ **Remoção de duplicatas** por `id_produto`
- ✅ **Padronização de nomes** para formato "Produto {id_produto}"
- ✅ **Validação de preços** (deve ser > 0)
- ✅ **Categorização automática** baseada no nome do produto
- ✅ **Tratamento de valores nulos** com valores padrão

### 2. Vendas (vendas.csv)
**Formato:** CSV com separador `;` e encoding UTF-8

**Colunas:**
- `id_venda`: Identificador único da venda
- `id_produto`: Referência ao produto
- `id_empregado`: Referência ao empregado
- `quantidade`: Quantidade vendida
- `valor_unitario`: Valor unitário da venda
- `valor_total`: Valor total da venda
- `data`: Data da venda (formato DD/MM/YYYY)

**Critérios de Tratamento:**
- ✅ **Validação de integridade referencial** com produtos e empregados
- ✅ **Tratamento inteligente de datas ausentes** (imputação por mediana)
- ✅ **Cálculo automático de valores** (quantidade × valor_unitario)
- ✅ **Remoção de vendas inválidas** (valores negativos ou zero)
- ✅ **Flags de rastreabilidade** para dados imputados
- ✅ **Tratamento de valores inconsistentes**

### 3. Empregados (empregados.csv)
**Formato:** CSV com separador `;` e encoding UTF-8

**Colunas:**
- `id_empregado`: Identificador único do empregado
- `nome`: Nome do empregado
- `idade`: Idade do empregado
- `cargo`: Cargo/função do empregado

**Critérios de Tratamento:**
- ✅ **Remoção de duplicatas** por `id_empregado`
- ✅ **Correção de nomes ausentes** com padrão "Funcionário {id}"
- ✅ **Imputação de idades ausentes** usando cascata de estratégias
- ✅ **Validação de idades** (18-70 anos)
- ✅ **Preenchimento de IDs ausentes** com sequência numérica
- ✅ **Padronização de cargos**

## 🔧 Critérios de Qualidade de Dados

### 1. Integridade Referencial
- **Vendas → Produtos:** Todas as vendas devem referenciar produtos válidos
- **Vendas → Empregados:** Todas as vendas devem referenciar empregados válidos
- **Chaves primárias:** Validação de unicidade em todas as tabelas

### 2. Tratamento de Valores Ausentes
- **Datas:** Imputação inteligente usando mediana por funcionário e global
- **Nomes:** Padronização com padrão "Entidade {ID}"
- **Idades:** Cascata de estratégias (mediana por cargo → mediana global → valor padrão)
- **IDs:** Preenchimento sequencial para manter integridade

### 3. Validação de Dados
- **Preços:** Deve ser > 0
- **Quantidades:** Deve ser > 0
- **Idades:** Deve estar entre 18-70 anos
- **Datas:** Formato DD/MM/YYYY válido
- **Valores totais:** Consistência com quantidade × valor_unitario

### 4. Rastreabilidade
- **Flags de imputação:** Identificação de dados tratados
- **Logs detalhados:** Registro de todas as operações
- **Estatísticas:** Contagem de registros processados e tratados

## 🗄️ Banco de Dados

### Configuração PostgreSQL
- **Host:** localhost
- **Porta:** 5432
- **Usuário:** bus2
- **Senha:** testebus2
- **Banco:** bus2

### Tabelas Criadas
1. **produtos** (id_produto, nome, preco, categoria)
2. **empregados** (id_empregado, nome, idade, cargo)
3. **vendas** (id_venda, id_produto, id_empregado, quantidade, valor_unitario, valor_total, data)

## 📈 Relatórios Gerados

### 1. Relatório PDF (relatorio-final.pdf)
**Seções incluídas:**
- **Total de vendas por funcionário** (tabela + resumo)
- **Ticket médio por produto** (tabela + resumo)
- **Vendas por categoria de produto** (tabela + gráfico de barras horizontais)
- **Top 5 funcionários** (tabela + gráfico de barras)
- **Evolução de vendas por período** (tabela + gráfico de linha + observação sobre imputação)

**Características técnicas:**
- **Biblioteca:** ReportLab (suporte Unicode completo)
- **Fontes:** DejaVu/Arial Unicode com fallback para Helvetica
- **Gráficos:** Matplotlib integrado (sem arquivos PNG intermediários)
- **Layout:** Páginas organizadas com espaçamentos otimizados

### 2. Arquivos Parquet
- **resumo-vendas.parquet:** Dados de vendas tratados
- **produtos.parquet:** Dados de produtos tratados
- **empregados.parquet:** Dados de empregados tratados

## 📝 Logs e Monitoramento

### Sistema de Logs
- **Arquivo:** `logs/pipeline_etl.log`
- **Formato:** Timestamp - Nível - Mensagem
- **Encoding:** UTF-8
- **Rotação:** Append mode (acumula execuções)

### Métricas Registradas
- **Tempo de execução** por etapa
- **Contagem de registros** processados
- **Dados tratados** (duplicatas, valores ausentes, etc.)
- **Erros e exceções** com stack trace completo

## 🔍 Análises Realizadas

### 1. Indicadores de Vendas
- **Volume total por funcionário**
- **Ticket médio por produto**
- **Distribuição por categoria**
- **Ranking de funcionários**
- **Evolução temporal**

### 2. Qualidade dos Dados
- **Taxa de completude** por campo
- **Identificação de outliers**
- **Consistência entre tabelas**
- **Impacto das imputações**

### 3. Performance
- **Tempo de processamento** por etapa
- **Uso de memória** otimizado
- **Eficiência de consultas** SQL

## ⚠️ Observações Importantes

### 1. Tratamento de Datas
- **Variação potencial:** Até 15% nos dados temporais devido à imputação
- **Estratégia:** Mediana por funcionário → mediana global → data padrão
- **Rastreabilidade:** Flags indicam dados imputados

### 2. Limitações Conhecidas
- **Dependência do PostgreSQL** para execução completa
- **Arquivos CSV** devem estar no formato especificado
- **Credenciais** do banco devem estar corretas

### 3. Melhorias Implementadas
- **SQLAlchemy** para evitar warnings do pandas
- **Processamento em memória** para gráficos (sem arquivos PNG)
- **Tratamento robusto** de caracteres especiais
- **Modularização** completa do código

## 🛠️ Tecnologias Utilizadas

### Backend
- **Python 3.8+**
- **Pandas** (manipulação de dados)
- **SQLAlchemy** (ORM e conexões)
- **Psycopg2** (driver PostgreSQL)

### Visualização
- **Matplotlib** (criação de gráficos)
- **ReportLab** (geração de PDFs)

### Banco de Dados
- **PostgreSQL 12+**
- **SQL** (consultas e DDL)

### Formato de Dados
- **CSV** (entrada)
- **Parquet** (saída otimizada)
- **PDF** (relatórios)

## 📞 Suporte

Para dúvidas ou problemas:
1. Verificar logs em `logs/pipeline_etl.log`
2. Confirmar conectividade com PostgreSQL
3. Validar formato dos arquivos CSV de entrada
4. Verificar permissões de escrita nos diretórios de saída

## Contato

- **Autor**: [Jeferson Andrade | j-90](https://github.com/j-90)
- **LinkedIn**: https://www.linkedin.com/in/jeferson-andrade90/
- **Email**: jandrademelo90@gmail.com
- **Data de Criação**: 26/08/2025 às 17:33 PM -03

## Histórico de Versões

- **v1.0.0** (26/08/2025): Versão inicial com pipeline ETL funcional.

## Notas Adicionais

- Certifique-se de que o arquivo `empregados.csv`, `produtos.csv` e `vendas.csv` contém as colunas esperadas.

## Links Úteis

- [pandas](https://pandas.pydata.org/)

---

**Desenvolvido com foco em qualidade de dados, rastreabilidade e boas práticas de ETL.**

