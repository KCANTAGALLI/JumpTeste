import pandas as pd
import time
from memory_profiler import memory_usage

# Defina o caminho para o arquivo CSV na sua maquina para o teste de execução da avaliação
file_path = "C:/Users/Usuario/Downloads/Sysmanager/projeto-rs 1/projeto-rs/config/vendas.csv"

# Função para processar os dados em pedaços
def process_vendas(file_path):
    # Inicializando variáveis para armazenar resultados
    produto_mais_vendido = None
    max_vendas = 0
    total_vendas_por_pais_regiao = {}
    total_vendas_por_produto = {}
    total_meses = 0

    # Lê o arquivo em chunks de 10000 linhas
    for chunk in pd.read_csv(file_path, chunksize=10000):
        # Convertendo a coluna Order Date para datetime e extraindo o mês
        chunk['Order Date'] = pd.to_datetime(chunk['Order Date'])
        chunk['Mes'] = chunk['Order Date'].dt.to_period('M')

        # Analisando o produto mais vendido (em unidades)
        vendas_por_produto = chunk.groupby('Item Type')['Units Sold'].sum()
        if vendas_por_produto.max() > max_vendas:
            max_vendas = vendas_por_produto.max()
            produto_mais_vendido = vendas_por_produto.idxmax()

        # Analisando vendas por país e região
        vendas_por_pais_regiao = chunk.groupby(['Country', 'Region'])['Total Revenue'].sum()
        for (pais, regiao), valor in vendas_por_pais_regiao.items():
            if (pais, regiao) in total_vendas_por_pais_regiao:
                total_vendas_por_pais_regiao[(pais, regiao)] += valor
            else:
                total_vendas_por_pais_regiao[(pais, regiao)] = valor

        # Armazenando vendas por produto
        for produto, quantidade in chunk.groupby('Item Type')['Units Sold'].sum().items():
            if produto in total_vendas_por_produto:
                total_vendas_por_produto[produto] += quantidade
            else:
                total_vendas_por_produto[produto] = quantidade

        # Contabilizando o número de meses
        total_meses += chunk['Mes'].nunique()

    # Encontrando o país e região com maior volume de vendas
    max_vendas_pais_regiao = max(total_vendas_por_pais_regiao.items(), key=lambda x: x[1])

    # Calculando a média mensal de vendas por produto
    media_vendas_mensais = {produto: quantidade / total_meses for produto, quantidade in total_vendas_por_produto.items()}

    return {
        'produto_mais_vendido': produto_mais_vendido,
        'max_vendas_pais_regiao': max_vendas_pais_regiao,
        'media_vendas_mensais': media_vendas_mensais
    }

if __name__ == '__main__':
    # Medindo o tempo de execução e consumo de memória
    start_time = time.time()
    mem_usage = memory_usage((process_vendas, (file_path,)), max_usage=True)

    # Executa a função e imprime os resultados
    resultados = process_vendas(file_path)
    print("Produto mais vendido:", resultados['produto_mais_vendido'])
    print("Maior volume de vendas (País, Região):", resultados['max_vendas_pais_regiao'])
    print("Média de vendas mensais por produto:", resultados['media_vendas_mensais'])

    # Exibe tempo de execução e uso de memória
    end_time = time.time()
    print("Tempo de execução:", end_time - start_time, "segundos")
    print("Uso máximo de memória:", mem_usage[0], "MB")