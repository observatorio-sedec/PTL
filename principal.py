import datetime
import pandas as pd
import requests as rq 
import pprint
from localidades import nacional, estadual
import ssl
# import gspread
# from Google import Create_Service
# from googleapiclient.http import MediaFileUpload
import openpyxl
from ajustar_planilha import ajustar_colunas, ajustar_bordas

tabela1086 = 1086
tabela6830 = 6830

class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def requisitando_dados(api):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_api = s.get(api, verify=True)

    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")

    try:
        dados_brutos = dados_brutos_api.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da API: {str(e)}")


    if len(dados_brutos) < 4:
        dados_brutos_282 = None
        dados_brutos_283 = None
        dados_brutos_171 = None
        dados_brutos_2522 = None
        return dados_brutos_282, dados_brutos_283, dados_brutos_171, dados_brutos_2522
    
    if dados_brutos_api.status_code == 500:
        raise Exception(f"Os dados passou de 100.0000 por isso o codigo de: {dados_brutos_api.status_code}")

    dados_brutos_282 = dados_brutos[0]
    dados_brutos_283 = dados_brutos[1]
    dados_brutos_171 = dados_brutos[2]
    dados_brutos_2522 = dados_brutos[3]
    

    return dados_brutos_282, dados_brutos_283, dados_brutos_171, dados_brutos_2522

def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)

    if dados_brutos:
        if tabela_id == tabela1086:
            variavel282 = dados_brutos[0]
            variavel283 = dados_brutos[1]
            variavel171 = dados_brutos[2]
            variavel2522 = dados_brutos[3]
            return variavel282, variavel283,variavel171,variavel2522
        elif tabela_id == tabela6830:
            variavel282 = dados_brutos[0]
            variavel283 = dados_brutos[1]
            return variavel282, variavel283
    else:
        pass

def tratando_dados1086(dados_brutos_282, dados_brutos_283, dados_brutos_171, dados_brutos_2522):
    dados_limpos_282 = []
    dados_limpos_283 = []
    dados_limpos_2522= []
    dados_limpos_151 = []

    variaveis = [dados_brutos_282, dados_brutos_283, dados_brutos_171, dados_brutos_2522]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']

            referencia_tempo = []
            tipo_inspecao = []

            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():
            
                    if nome_produto in ['Federal', 'Estadual', 'Municipal']:
                        tipo_inspecao.append(nome_produto)

                chave_referencia_tempo = next(iter(dados_id_produto.values()))

                if chave_referencia_tempo == 'Total do trimestre' and chave_referencia_tempo not in referencia_tempo:
                    referencia_tempo.append(chave_referencia_tempo)

            for iv in dados_producao:
                id = iv['localidade']['id']
                nome = iv['localidade']['nome'].replace(' (MT)', '')
                dados_ano_producao = iv['serie']

                for ano, producao in dados_ano_producao.items():
                    partes = ano.split("/")
                    ano_sem_trimestre = int(partes[0][:4])
                    trimestre = int(partes[0][4:6])
                    producao = producao.replace('-', '0').replace('...', '0').replace('X', '0')

                    referencia_tempo_str = ', '.join(referencia_tempo)
                    tipo_inspecao_str = ', '.join(tipo_inspecao)

                    dict = {
                        'id': id,
                        'nome': nome,
                        'id_produto': id_produto,
                        'Referencia_Tempo': referencia_tempo_str,
                        'Tipo_Inspecao': tipo_inspecao_str,
                        variavel: producao,
                        'ano': f'01/{int(trimestre) * 3}/{ano_sem_trimestre}',
                        'Trimestre': trimestre
                    }

                    if id_tabela == '282':
                        dados_limpos_282.append(dict)
                    elif id_tabela == '283':
                        dados_limpos_283.append(dict)
                    elif id_tabela == '2522':
                        dados_limpos_2522.append(dict)
                    elif id_tabela == '151':
                        dados_limpos_151.append(dict)

    return dados_limpos_282, dados_limpos_283, dados_limpos_151, dados_limpos_2522

ano_atual = datetime.datetime.now().year
def executando_funcoes():
    lista_dados_151 = [] 
    lista_dados_282 = []
    lista_dados_283 = []
    lista_dados_2522 = []
    for ano in range(2014, ano_atual):
        for tri in range(1, 5):
            api = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela1086}/periodos/{ano}0{tri}/variaveis/151|282|283|2522?localidades=N3[all]&classificacao=12716[115236]|12529[111737,111738,111739]'     
            variavel_151, variavel_282, variavel_283, variavel_2522 = extrair_dados(api, 1086)
            if variavel_151 == None and variavel_282 == None and variavel_283 == None and variavel_2522 == None:
                break
            else:
                novos_dados_151, novos_dados_282, novos_dados_283, novos_dados_2522 = tratando_dados1086(variavel_151, variavel_282, variavel_283, variavel_2522)
                lista_dados_151.extend(novos_dados_151)
                lista_dados_282.extend(novos_dados_282)
                lista_dados_283.extend(novos_dados_283)
                lista_dados_2522.extend(novos_dados_2522)
    
    return  lista_dados_151,lista_dados_282,lista_dados_283, lista_dados_2522

def gerando_dataframe1086(dados_limpos_282_estadual, dados_limpos_283_estadual,  dados_limpos_171_estadual,  dados_limpos_2522_estadual):

    
    df282_estadual = pd.DataFrame(dados_limpos_282_estadual)
    df283_estadual = pd.DataFrame(dados_limpos_283_estadual)
    df171_estadual = pd.DataFrame(dados_limpos_171_estadual)
    df2522_estadual = pd.DataFrame(dados_limpos_2522_estadual)
    df1086_estadual= pd.merge(df282_estadual, df283_estadual, on=['id', 'nome', 'id_produto','Referencia_Tempo','Tipo_Inspecao', 'ano', 'Trimestre'], how='inner')
    df1086_estadual= pd.merge(df1086_estadual, df171_estadual, on=['id', 'nome', 'id_produto','Referencia_Tempo','Tipo_Inspecao', 'ano', 'Trimestre'], how='inner')
    df1086_estadual= pd.merge(df1086_estadual, df2522_estadual, on=['id', 'nome', 'id_produto','Referencia_Tempo','Tipo_Inspecao', 'ano', 'Trimestre'], how='inner')
    

    df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'].str.replace(',', '.').astype(float)   
    df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'] * 1000
    df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'].str.replace(',', '.').astype(float)
    df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'] * 1000
    
    return df1086_estadual


# pp = pprint.PrettyPrinter(indent=4)
dados_limpos_282_estadual, dados_limpos_283_estadual,  dados_limpos_171_estadual,  dados_limpos_2522_estadual = executando_funcoes()
df1086_estadual = gerando_dataframe1086(dados_limpos_282_estadual, dados_limpos_283_estadual,  dados_limpos_171_estadual,  dados_limpos_2522_estadual)


df1086_estadual.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\planilhas tratadas\\PLT 1086 ESTADUAL.xlsx", index=False)


planilha_principal = openpyxl.Workbook()    
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\planilhas tratadas\\PTL 1086.xlsx")    
worksheet = planilha_principal.active
ajustar_bordas(planilha_principal)        
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\planilhas tratadas\\PTL 1086.xlsx")

if __name__ == '__main__':
    from sql import executar_sql 
    executar_sql()
