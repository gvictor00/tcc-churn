import pandas as pd

# if main is not defined, define it
if __name__ == "__main__":
    # load the data
    df = pd.read_csv('tabelas/df_concatenated_new_6.csv', sep=';')

    unique_ids = df['rec_est_id'].unique()  # Lista de ids únicos da tabela principal

    # Cria dataframe limpo para receber os dados
    df_clean = pd.DataFrame(columns=df.columns)

    # Adiciona as colunas 'already_churn' e 'churn_count' ao dataframe limpo
    df_clean['category_change'] = 0
    df_clean['already_churn'] = 0
    df_clean['churn_count'] = 0
    df_clean['up_to_leave'] = 0


    mapeamento = {'Nunca Mov': 1, 'Churn': 2, 'Recorrente': 3, 'Perda': 4, 'Recuperação': 5, 'Novo': 6}

    for current_id in unique_ids:
        print(f'Atualizando id {current_id} ...')
        # Filtra a tabela principal pelo id
        df_id = df[df['rec_est_id'] == current_id]

        # Cria um dataframe temporário
        df_temp = df_id.copy()
        # Cria coluna para verificar se a coluna 'rec_monthly_category' é diferente da linha anterior
        df_temp['category_change'] = df_id['rec_monthly_category'].diff()
        # Evita NaN na coluna 'category_change'
        df_temp['category_change'] = df_temp['category_change'].fillna(0)

        # Se for igual a 2 ou 4, a coluna 'new_rec_monthly_category' recebe 1 (churn).
        # Se for igual a 3 ou 5, a coluna 'new_rec_monthly_category' recebe 0 (recorrente).
        df_temp['up_to_leave'] = df_id['rec_monthly_category'].apply(
            lambda x: 1 if x == 2 or x == 4 else 0
        )

        # Cria coluna para verificar se a coluna 'rec_monthly_category' é igual a 5 (churn)
        df_temp['already_churn'] = df_id['rec_monthly_category'].apply(lambda x: 1 if x == 2 else 0)
        # Cria coluna com a contagem de churns
        df_temp['churn_count'] = df_temp['already_churn'].cumsum()

        # Atualiza o dataframe limpo com os dados do dataframe temporário
        df_clean = pd.concat([df_clean, df_temp], ignore_index=True)

    # Move a coluna rec_monthly_category para o final do dataframe
    df_clean = df_clean[
        [col for col in df_clean.columns if col != 'rec_monthly_category'] + ['rec_monthly_category']]

    # Confirma que o df_clean tem o mesmo número de linhas que o df
    if df_clean.shape[0] == df.shape[0]:
        print('Número de linhas confirmado!')
        print('Salvando tabela...')
        # Salva a tabela principal, depois de atualizada
        #df_clean.to_csv('tabelas/df_concatenated_new_6_with_counter_new_class.csv', sep=';', index=False)

    # Itera por todos os ids únicos da tabela df_clean de modo a contar a quantidade de dados de cada classe da
    # coluna 'up_to_leave'

    def count_up_to_leave_by_months_age(months_count):
        up_to_leave_dict = {'0': 0, '1': 0}
        users_count = 0
        for current_id in unique_ids:
            # Filtra a tabela principal pelo id
            df_id = df_clean[df_clean['rec_est_id'] == current_id]

            if len(df_id) >= months_count:
                users_count += 1
                # Verifica se a coluna 'up_to_leave' é igual a 1, se sim, adiciona 1 ao dicionário
                if df_id['up_to_leave'].iloc[-1] == 1:
                    up_to_leave_dict['1'] += 1
                else:
                    up_to_leave_dict['0'] += 1

        print(f'Quantidade de ids com {months_count} meses de idade: {users_count}')
        print(up_to_leave_dict)
        # Apresenta os resultados percentuais
        print(f'Percentual de 0: {round((up_to_leave_dict["0"] / users_count)*100,3)}')
        print(f'Percentual de 1: {round((up_to_leave_dict["1"] / users_count)*100,3)}')
        print('------------------------')

    # conta de 4 a 12 meses
    for i in range(4, 13):
        count_up_to_leave_by_months_age(i)
