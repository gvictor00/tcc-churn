import json
import os

import pandas as pd


def load_data():
    # Carregar as tabelas do diretório 'tabelas'
    df_available_data = pd.read_csv('tabelas/available_data.tsv', sep='\t')
    df_products = pd.read_csv('tabelas/df_dummies_products.csv', sep=';')
    df_products_old = pd.read_csv('tabelas/df_dummies_products_old.csv', sep=';')

    # print(df_products.head())

    # Split the column 'mes' in 'ano' and 'mes'
    df_products['ano'] = df_products['mes'].apply(lambda x: int(x.split('-')[0]))
    df_products['mes'] = df_products['mes'].apply(lambda x: int(x.split('-')[1]))

    return df_available_data, df_products, df_products_old


def run(min_age=12):
    # Carregar as tabelas
    df_available_data, df_products, df_products_old = load_data()

    unique_ids_from_main_data = df_available_data['rec_est_id'].unique()  # Lista de ids únicos da tabela principal
    concatenated_rows = []  # Lista de dicionários para armazenar as linhas concatenadas
    ids_not_in_products = set()  # Set de ids que não estão na tabela de produtos
    old_users = set()  # Set de ids de usuários antigos

    # Cria uma estrutura para armazenar os tamanhos dos dataframes de cada usuário
    # {tamanho: quantidade de usuários}
    sizes = {}

    for unique_id in unique_ids_from_main_data:
        df_id = df_available_data[df_available_data['rec_est_id'] == unique_id]

        if unique_id in df_products['tx_est_id'].unique():
            df_products_id = df_products[df_products['tx_est_id'] == unique_id].sort_values(by=['ano', 'mes'])

            sizes[len(df_products_id)] = sizes.get(len(df_products_id), 0) + 1

            if len(df_products_id) >= min_age:
                old_users.add(unique_id)

            for _, row1 in df_products_id.iterrows():
                month = row1['mes']
                year = row1['ano']

                # print(f'ID: {unique_id} - Month: {month} - Year: {year}')

                # Filtrar a tabela 2 com base no mês e no ano correspondentes
                filtered_rows = df_id[(df_id['rec_month_part'] == month) & (df_id['rec_year_part'] == year)]

                # Concatenar as linhas correspondentes da
                for _, row2 in filtered_rows.iterrows():
                    concatenated_row = {**row2, **row1}
                    concatenated_rows.append(concatenated_row)

        elif unique_id in df_products_old['tx_est_id'].unique():
            df_products_old_id = df_products_old[df_products_old['tx_est_id'] == unique_id].sort_values(
                by=['ano', 'mes'])

            sizes[len(df_products_old_id)] = sizes.get(len(df_products_old_id), 0) + 1

            if len(df_products_old_id) >= min_age:
                old_users.add(unique_id)

            for _, row1 in df_products_old_id.iterrows():
                month = row1['mes']
                year = row1['ano']

                # print(f'ID: {unique_id} - Month: {month} - Year: {year}')

                # Filtrar a tabela 2 com base no mês e no ano correspondentes
                filtered_rows = df_id[(df_id['rec_month_part'] == month) & (df_id['rec_year_part'] == year)]

                # Concatenar as linhas correspondentes da
                for _, row2 in filtered_rows.iterrows():
                    concatenated_row = {**row2, **row1}
                    concatenated_rows.append(concatenated_row)
        else:
            ids_not_in_products.add(unique_id)

    df_concatenated = pd.DataFrame(concatenated_rows)  # Dataframe com as linhas concatenadas

    # df_concatenated.drop(columns=['Unnamed: 0'], inplace=True)

    # Adiciona a coluna 'qtd_produtos' ao dataframe, com a quantidade de produtos que o usuário possui
    products_columns = ['emission_bill', 'linkou', 'linkou_pix', 'payment_bill_card', 'pos', 'pos_pix', 'tef']
    df_concatenated['qtd_produtos'] = df_concatenated[products_columns].sum(axis=1)

    # Mantém somente as colunas com delta
    # Remove as colunas 'rec_month_mf', 'rec_previous_month_mf', 'rec_month_qtd_mov', 'rec_previous_month_qtd_mov'
    df_concatenated.drop(columns=['rec_month_mf', 'rec_previous_month_mf',
                                  'rec_month_qtd_mov', 'rec_previous_month_qtd_mov',
                                  'mes', 'ano', 'tx_est_id', 'rec_month'],
                         inplace=True)

    # Move a coluna rec_monthly_category para o final do dataframe
    df_concatenated = df_concatenated[
        [col for col in df_concatenated.columns if col != 'rec_monthly_category'] + ['rec_monthly_category']]

    # Salva o dataframe concatenado em um arquivo .csv
    df_concatenated.to_csv(f'tabelas/df_concatenated_new_{min_age}.csv', sep=';', index=False)

    print(50 * '-')
    # Ordenar o dicionário de tamanhos
    sizes = {k: v for k, v in sorted(sizes.items(), key=lambda item: item[0])}

    unique_ids_from_concatenated = df_concatenated['rec_est_id'].unique()
    # Verifica se o valor na "última linha" da coluna 'rec_monthly_category' do dataframe é igual a '2'
    # Se for, o usuário é churner, adiciona o id à lista de churners
    churners = [unique_id for unique_id in unique_ids_from_concatenated if
                df_concatenated[df_concatenated['rec_est_id'] == unique_id]['rec_monthly_category'].iloc[-1] == 2]

    mapeamento = {'Nunca Mov': 1, 'Churn': 2, 'Recorrente': 3, 'Perda': 4, 'Recuperação': 5, 'Novo': 6}
    count_map = {mapeamento[k]: 0 for k, v in mapeamento.items()}
    # Contar a quantidade de usuários em cada categoria, considerando a última linha de cada usuários únicos e o
    # dado da coluna 'rec_monthly_category'
    for current_id in unique_ids_from_concatenated:
        # ordena o dataframe por 'rec_month_part' e 'rec_year_part' e pega a linha mais recente
        category = df_concatenated[df_concatenated['rec_est_id'] == current_id].sort_values(
            by=['rec_year_part', 'rec_month_part']).iloc[-1]['rec_monthly_category']

        # O identificador da categoria é o valor do dicionário 'mapeamento' correspondente à categoria
        count_map[category] += 1

        # Imprime a linha mais recente do usuário com categoria 'Churn'
        # if category == 2:
        #     print(df_concatenated[df_concatenated['rec_est_id'] == current_id].sort_values(
        #         by=['rec_year_part', 'rec_month_part']).iloc[-1])

    for k, v in mapeamento.items():
        # Renomeia as chaves do dicionário 'count_map' para os nomes das categorias
        count_map[k] = count_map.pop(v)

    print(count_map)

    count_map_old = {mapeamento[k]: 0 for k, v in mapeamento.items()}
    churners_old = []

    # Análise das categorias para os usuários em old_users (com mais de min_age meses de dados)
    for us in old_users:
        # Pega a categoria do usuário na última linha
        category = df_concatenated[df_concatenated['rec_est_id'] == us].sort_values(
            by=['rec_year_part', 'rec_month_part']).iloc[-1]['rec_monthly_category']

        # Preenche o dicionário count_map_old com a quantidade de usuários em cada categoria
        count_map_old[category] += 1

        # Se o usuário for churner, adiciona o id à lista de churners
        if category == 2:
            churners_old.append(us)

    # Renomeia as chaves do dicionário 'count_map' para os nomes das categorias
    for k, v in mapeamento.items():
        count_map_old[k] = count_map_old.pop(v)

    # Cria método para printar textos no console e no arquivo de log
    def print_and_log(text, file_name):
        print(text)
        with open(file_name, 'a') as f:
            f.write(text + '\n')

    # Sumário
    filename = f'sumarios/summary_{min_age}_months.txt'

    # Remove o arquivo de log se ele já existir
    if os.path.exists(filename):
        os.remove(filename)

    print_and_log(50 * '-', filename)
    print_and_log(f'Dataframe concatenado criado com sucesso! {len(df_concatenated)} linhas', filename)
    print_and_log(f'Número de linhas duplicadas: {len(df_concatenated[df_concatenated.duplicated()])}', filename)
    print_and_log(f'Número de ids únicos: {len(df_concatenated["rec_est_id"].unique())}', filename)
    print_and_log(f'Número de ids que não estão na tabela de produtos: {len(ids_not_in_products)}', filename)
    print_and_log(f'Tamanhos dos dataframes: {sizes}', filename)

    print_and_log(50 * '-', filename)
    print_and_log(f'Número de churners: {len(churners)}', filename)
    print_and_log(f'Lista de churners: {churners}', filename)
    # Percentual de churners em relação ao total de usuários
    print_and_log(f'Percentual de churners: {round(len(churners) / len(unique_ids_from_concatenated), 4)}%', filename)

    print_and_log(50 * '-', filename)
    print_and_log(f'Existem {len(old_users)} usuários antigos, com mais de {min_age} mes(es) de dados', filename)
    print_and_log(count_map_old, filename)
    print_and_log(f'Número de churners com {min_age} mes(es): {len(churners_old)}', filename)
    print_and_log(f'Lista de churners: {churners_old}', filename)
    # Percentual de churners em relação ao total de usuários
    print_and_log(f'Percentual de churners: {round(len(churners_old) / len(old_users), 4)}%', filename)

    print_and_log(50 * '-' + '\n', filename)
    #
    # # Salva os dados impressos em um arquivo txt
    # with open(f'tabelas/summary_{min_age}.txt', 'w') as f:
    #     f.write(f'Dataframe concatenado criado com sucesso! {len(df_concatenated)} linhas\n')
    #     f.write(f'Número de linhas duplicadas: {len(df_concatenated[df_concatenated.duplicated()])}\n')
    #     f.write(f'Número de ids únicos: {len(df_concatenated["rec_est_id"].unique())}\n')
    #     f.write(f'Número de ids que não estão na tabela de produtos: {len(ids_not_in_products)}\n')
    #     f.write(f'Tamanhos dos dataframes: {sizes}\n')
    #
    #     f.write(50 * '-' + '\n')
    #     f.write(f'Número de churners: {len(churners)}\n')
    #     f.write(f'Lista de churners: {churners}\n')
    #     f.write(f'Percentual de churners: {round(len(churners) / len(unique_ids_from_concatenated), 4)}%\n')
    #
    #     f.write(50 * '-' + '\n')
    #     f.write(f'Existem {len(old_users)} usuários antigos, com mais de {min_age} mes(es) de dados\n')
    #     f.write(json.dumps(count_map_old))
    #     f.write(f'Número de churners com {min_age} mes(es): {len(churners_old)}\n')
    #     f.write(f'Lista de churners: {churners_old}\n')
    #     f.write(f'Percentual de churners: {round(len(churners_old) / len(old_users), 4)}%\n')
    #
    #     f.write(50 * '-' + '\n')


def analyze_data():
    df_concatenated = pd.read_csv('tabelas/df_concatenated.csv', sep=';')

    ages = []
    unique_ids_from_main_data = df_concatenated['tx_est_id'].unique()
    for unique_id in unique_ids_from_main_data:
        df_id = df_concatenated[df_concatenated['tx_est_id'] == unique_id]
        ages.append(len(df_id))

    df_concatenated.drop(columns=['rec_month', 'rec_month_part', 'rec_year_part', 'tx_est_id'], inplace=True)
    df_concatenated.to_csv('tabelas/df_concatenated_adjusted.csv', sep=';', index=False)


# método para carregar um arquivo csv e aplicar o one hot encoding na coluna tx_product
def load_xlsx(path):
    # carregar o arquivo csv
    df = pd.read_csv(path)
    df = pd.get_dummies(df, columns=['tx_product'])

    # Adiciona as colunas que não existem no arquivo
    columns = ['emission_bill', 'linkou_pix', 'pos_pix', 'tef']
    for column in columns:
        if column not in df.columns:
            df[column] = 0

    # salvar o dataframe em um arquivo csv
    df.to_csv('tabelas/df_dummies_products.csv', sep=';', index=False)


if __name__ == '__main__':
    print('Iniciando o programa...')
    print("Executando para 6 meses...")
    run(6)
    # print("Executando para 7 meses...")
    # run(7)
    # print("Executando para 8 meses...")
    # run(8)
    # print("Executando para 9 meses...")
    # run(9)
    # print("Executando para 10 meses...")
    # run(10)
    # print("Executando para 11 meses...")
    # run(11)
    # print("Executando para 12 meses...")
    # run(12)
    print("Fim do programa!")

    # load_xlsx("C:/Users/gvalv/Downloads/produtos_2021_lojistas_faltantes.xlsx - tx_products.csv")
    # analyze_data()
