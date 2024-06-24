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

url1086_nacional = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela1086}/periodos/201301%7C201302%7C201303%7C201304%7C201401%7C201402%7C201403%7C201404%7C201501%7C201502%7C201503%7C201504%7C201601%7C201602%7C201603%7C201604%7C201701%7C201702%7C201703%7C201704%7C201801%7C201802%7C201803%7C201804%7C201901%7C201902%7C201903%7C201904%7C202001%7C202002%7C202003%7C202004%7C202101%7C202102%7C202103%7C202104%7C202201%7C202202%7C202203%7C202204%7C202301%7C202302%7C202303/variaveis/282%7C283?{nacional}&classificacao=12716[115236]|12529[111737,111738,111739]'
url1086_estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela1086}/periodos/201301%7C201302%7C201303%7C201304%7C201401%7C201402%7C201403%7C201404%7C201501%7C201502%7C201503%7C201504%7C201601%7C201602%7C201603%7C201604%7C201701%7C201702%7C201703%7C201704%7C201801%7C201802%7C201803%7C201804%7C201901%7C201902%7C201903%7C201904%7C202001%7C202002%7C202003%7C202004%7C202101%7C202102%7C202103%7C202104%7C202201%7C202202%7C202203%7C202204%7C202301%7C202302%7C202303/variaveis/282%7C283?{estadual}&classificacao=12716[115236]|12529[111737,111738,111739]'

url6830_nacional = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela6830}/periodos/201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303/variaveis/282|283?{nacional}&classificacao=12716[115236]'

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
    
    # Verificação se a solicitação foi bem-sucedida antes de continuar
    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")

    # Verificação se a resposta pode ser convertida para JSON
    try:
        dados_brutos = dados_brutos_api.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da API: {str(e)}")

    # Verificação se a resposta contém os dados esperados
    if len(dados_brutos) < 2:
        raise Exception("A resposta da API não contém dados suficientes.")
    
    if dados_brutos_api.status_code == 500:
        raise Exception(f"Os dados passou de 100.0000 por isso o codigo de: {dados_brutos_api.status_code}")

    dados_brutos_282 = dados_brutos[0]
    dados_brutos_283 = dados_brutos[1]


    return dados_brutos_282, dados_brutos_283

def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)

    if dados_brutos:
        if tabela_id == tabela1086:
            variavel282 = dados_brutos[0]
            variavel283 = dados_brutos[1]
            return variavel282, variavel283
        elif tabela_id == tabela6830:
            variavel282 = dados_brutos[0]
            variavel283 = dados_brutos[1]
            return variavel282, variavel283
    else:
        pass

def tratando_dados1086(dados_brutos_282, dados_brutos_283):
    dados_limpos_282 = []
    dados_limpos_283 = []

    variaveis = [dados_brutos_282, dados_brutos_283]

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
                        'ano': f'01/01/{ano_sem_trimestre}',
                        'Trimestre': trimestre
                    }

                    if id_tabela == '282':
                        dados_limpos_282.append(dict)
                    elif id_tabela == '283':
                        dados_limpos_283.append(dict)

    return dados_limpos_282, dados_limpos_283

def tratando_dados6830(dados_brutos_282, dados_brutos_283):
    dados_limpos_282 = []
    dados_limpos_283 = []
    
    variaveis = [dados_brutos_282, dados_brutos_283]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']
        
        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            
            referencia_tempo = set()

            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                
                chave_referencia_tempo = next(iter(dados_id_produto.values()))

                if chave_referencia_tempo == 'Total do trimestre' and chave_referencia_tempo not in referencia_tempo:
                    referencia_tempo.add(chave_referencia_tempo)

                for id_produto, nome_produto in dados_id_produto.items():
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

                            
                            dict = {
                                'id': id,
                                'nome': nome,
                                'id_produto': id_produto,
                                'Referencia_Tempo': referencia_tempo_str,
                                variavel: producao,
                                'ano': f'01/01/{ano_sem_trimestre}',
                                'Trimestre': trimestre
                            }

                            if id_tabela == '282':
                                dados_limpos_282.append(dict)
                            elif id_tabela == '283':
                                dados_limpos_283.append(dict)

    return dados_limpos_282, dados_limpos_283

def executando_funcoes():
    variavel282nacional, variavel283nacional = extrair_dados(url1086_nacional, tabela1086)
    variavel282estadual, variavel283estadual = extrair_dados(url1086_estadual, tabela1086)
    
    variavel6830nacional, variavel6831nacional = extrair_dados(url6830_nacional, tabela6830)
    
    dados_limpos_6830_nacional, dados_limpos_6831_nacional = tratando_dados6830(variavel6830nacional, variavel6831nacional)

    dados_limpos_282_nacional, dados_limpos_283_nacional = tratando_dados1086(variavel282nacional, variavel283nacional)
    dados_limpos_282_estadual, dados_limpos_283_estadual = tratando_dados1086(variavel282estadual, variavel283estadual)
    
    return dados_limpos_282_nacional, dados_limpos_283_nacional, dados_limpos_282_estadual, dados_limpos_283_estadual, dados_limpos_6830_nacional, dados_limpos_6831_nacional

def gerando_dataframe1086(dados_limpos_282_nacional, dados_limpos_283_nacional, dados_limpos_282_estadual, dados_limpos_283_estadual):
    df282_nacional = pd.DataFrame(dados_limpos_282_nacional)
    df283_nacional = pd.DataFrame(dados_limpos_283_nacional)
    df1086_nacional = pd.merge(df282_nacional, df283_nacional, on=['id', 'nome', 'id_produto','Referencia_Tempo','Tipo_Inspecao', 'ano', 'Trimestre'], how='inner')
    
    df282_estadual = pd.DataFrame(dados_limpos_282_estadual)
    df283_estadual = pd.DataFrame(dados_limpos_283_estadual)
    df1086_estadual= pd.merge(df282_estadual, df283_estadual, on=['id', 'nome', 'id_produto','Referencia_Tempo','Tipo_Inspecao', 'ano', 'Trimestre'], how='inner')
    
    df1086_nacional['Quantidade de leite cru, resfriado ou não, adquirido'] = df1086_nacional['Quantidade de leite cru, resfriado ou não, adquirido'].str.replace(',', '.').astype(float)
    df1086_nacional['Quantidade de leite cru, resfriado ou não, industrializado'] = df1086_nacional['Quantidade de leite cru, resfriado ou não, industrializado'].str.replace(',', '.').astype(float)
    df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, adquirido'].str.replace(',', '.').astype(float)
    df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'] = df1086_estadual['Quantidade de leite cru, resfriado ou não, industrializado'].str.replace(',', '.').astype(float)
    
    return df1086_nacional, df1086_estadual

def gerando_dataframe6830(dados_limpos_6830_nacional, dados_limpos_6831_nacional):
    df6832_nacional = pd.DataFrame(dados_limpos_6830_nacional)
    df6831_nacional = pd.DataFrame(dados_limpos_6831_nacional)
    df6830 = pd.merge(df6832_nacional, df6831_nacional, on=['id', 'nome', 'id_produto','Referencia_Tempo', 'ano', 'Trimestre'], how='inner')
    df6830['Quantidade de leite cru, resfriado ou não, adquirido'] = df6830['Quantidade de leite cru, resfriado ou não, adquirido'].str.replace(',', '.').astype(float)
    df6830['Quantidade de leite cru, resfriado ou não, industrializado'] = df6830['Quantidade de leite cru, resfriado ou não, industrializado'].str.replace(',', '.').astype(float)
    return df6830

# pp = pprint.PrettyPrinter(indent=4)
dados_limpos_282_nacional, dados_limpos_283_nacional, dados_limpos_282_estadual, dados_limpos_283_estadual, dados_limpos_6830_nacional, dados_limpos_6831_nacional = executando_funcoes()
df1086_nacional, df1086_estadual = gerando_dataframe1086(dados_limpos_282_nacional, dados_limpos_283_nacional, dados_limpos_282_estadual, dados_limpos_283_estadual)


df1086_nacional.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 1086 NACIONAL.xlsx", index=False)
df1086_estadual.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 1086 ESTADUAL.xlsx", index=False)

df6830 = gerando_dataframe6830(dados_limpos_6830_nacional, dados_limpos_6831_nacional)
df6830.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 6830 NACIONAL.xlsx", index=False)

wb_6930nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 6830 NACIONAL.xlsx")
ws6930_nacional = wb_6930nacional.active

ajustar_colunas(ws6930_nacional)
ajustar_bordas(wb_6930nacional)
wb_6930nacional.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 6830 NACIONAL.xlsx")
##############################
planilha_principal = openpyxl.Workbook()

wb_1086nacional = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 1086 NACIONAL.xlsx")
wb_1086estadual = openpyxl.load_workbook("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PLT 1086 ESTADUAL.xlsx")

aba1086_nacional = planilha_principal.create_sheet('NACIONAL 1086')
aba1086_estadual = planilha_principal.create_sheet('ESTADUAL 1086')

for linha in wb_1086nacional.active.iter_rows(values_only=True):
    aba1086_nacional.append(linha)

for linha in wb_1086estadual.active.iter_rows(values_only=True):
    aba1086_estadual.append(linha)
    
for aba in planilha_principal.sheetnames:
    if aba not in ["NACIONAL 1086", "ESTADUAL 1086"]:
        del planilha_principal[aba]

lista_abas = [aba1086_nacional, aba1086_estadual]
for abas in lista_abas:
    ajustar_colunas(abas)
    
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PTL 1086.xlsx")    
############################    
worksheet = planilha_principal.active
ajustar_bordas(planilha_principal)        
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\PTL\\PTL 1086.xlsx")

if __name__ == '__main__':
    from sql import executar_sql 
    executar_sql()
