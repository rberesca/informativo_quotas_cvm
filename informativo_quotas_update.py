import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup

# Parameters
UPDATE_QUOTAS = 1
INSERT_FUNDS = 2
FUNDS_SELECTION = 3

# Define module to run
module = UPDATE_QUOTAS
# source
url_informativo_diario = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

# connection variable
host = "my_AWS_RDS_url"
port = 3306
dbname= "my_dbname"
user = "my_user"
password = "passwd"

def get_informativo_lnks(csv_url):
    # create response object
    r = requests.get(csv_url)
    # create beautiful-soup object
    soup = BeautifulSoup(r.content, 'html5lib')
    # find all links on web-page
    links = soup.findAll('a')
    # filter the link sending with .mp4
    csv_links = [csv_url + link['href'] for link in links if link['href'].endswith('csv')]
    return csv_links


def update_database(module, update_parameter):
    try:
        # connect databse
        conn = pymysql.connect(host, user=user, port=port, passwd=password, db=dbname)
        cursor = conn.cursor()

        if module == UPDATE_QUOTAS:

            # get list of funds to update
            funds_to_update = pd.read_sql("SELECT DISTINCT CNPJ_FUNDO FROM table1", conn)
            conn.commit()
            # convert result to a numpy array
            funds_list = funds_to_update['CNPJ_FUNDO'].to_numpy()

            files_to_Update = get_informativo_lnks(update_parameter)
            for file_to_Update in files_to_Update:

                # read the source file quotas
                df = pd.read_csv(file_to_Update, sep=';', decimal='.', encoding="ISO-8859-1")
                # filter quotas to use only results of funds_list
                df = df[df['CNPJ_FUNDO'].isin(funds_list)]
                df = df.reset_index(drop=True)
                df = df.astype(str)
                total_records_entered =  len(df)
                print("\nFile:", file_to_Update.split('/')[-1])
                print("Records being entered:", total_records_entered)

                # check existing records
                df_ = pd.read_sql(f"""SELECT * FROM table2 WHERE DT_COMPTC BETWEEN
                                    \'{df['DT_COMPTC'].min()}\' AND \'{df['DT_COMPTC'].max()}\'""", conn)
                df_ = df_.astype(str)
                pre_existing_records = len(df_)
                print("Existing records:", pre_existing_records)
                # merge tables
                df_to_update = df_.merge(df, indicator='i', how='outer').query('i == "right_only"').drop('i', 1)
                df_to_update = df_to_update.astype(str)
                print("Records to insert:", len(df_to_update))

                for i, row in df_to_update.iterrows():
                    # print(tuple(row))
                    query = """INSERT INTO table2 (CNPJ_FUNDO, DT_COMPTC, VL_TOTAL, VL_QUOTA,
                            VL_PATRIM_LIQ, CAPTC_DIA, RESG_DIA, NR_COTST) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(query, tuple(row))
                    conn.commit()

        else:
                pass

    except Exception as e:
        print("Exeception occured:{}".format(e))
    finally:
        conn.close()
    return


def main():
    if module in (INSERT_FUNDS, FUNDS_SELECTION):
        update_database(module, 0)
    else:
        update_database(module, url_informativo_diario)
    return


if __name__ == "__main__":
    main()
