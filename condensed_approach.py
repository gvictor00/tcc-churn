import pandas as pd


def load_data():
    df = pd.read_csv('tabelas/df_concatenated_new.csv', sep=';')
    return df


def save_data(df):
    # Move a coluna 'rec_registered_month' para o final do dataframe
    df_data = df[
        [col for col in df.columns if col != 'rec_monthly_category'] + ['rec_monthly_category']]

    # Salvando tabela
    df_data.to_csv('tabelas/df_simplified_merged.csv', sep=';', index=False)


def merge_monthly_data(df):
    unique_ids = df['rec_est_id'].unique()

    # Cria dataframe limpo para receber os dados
    df_new = pd.DataFrame(columns=df.columns)

    for current_id in unique_ids:
        df_id = df[df['rec_est_id'] == current_id]
        if len(df_id) > 2:
            # Cria linha temporária
            df_temp = df_id.copy()

            # Realizando a soma acumulada das colunas desejadas
            columns_to_accumulate = ['rec_month_mf_antecipated', 'rec_month_mf_not_antecipated', 'rec_month_qtd_mov']
            for col in columns_to_accumulate:
                df_temp[col] = df_temp[col].shift(1) + df_temp[col]

            # Remove a primeira linha do dataframe temporário
            df_temp = df_temp.iloc[1:]

            # Adiciona o dataframe temporário ao dataframe final
            df_new = pd.concat([df_new, df_temp])

    return df_new


def run():
    df_data = load_data()
    _DATA_TO_KEEP = ['rec_est_id', 'rec_month_part', 'rec_year_part', 'rec_month_mf_antecipated',
                     'rec_month_mf_not_antecipated', 'rec_month_qtd_mov', 'rec_registered_month', 'emission_bill',
                     'linkou', 'linkou_pix', 'payment_bill_card', 'pos', 'pos_pix', 'tef', 'rec_monthly_category']
    df_data = df_data[_DATA_TO_KEEP]

    # Cria coluna qtd_produtos, que representa a quantidade de produtos que o cliente possui
    df_data['qtd_produtos'] = df_data['emission_bill'] + df_data['linkou'] + df_data['linkou_pix'] + \
                              df_data['payment_bill_card'] + df_data['pos'] + df_data['pos_pix'] + df_data['tef']

    # Junta os dados mensais (2 meses) em um único mês
    df_data = merge_monthly_data(df_data)

    # Salva a tabela
    save_data(df_data)


if __name__ == "__main__":
    run()
