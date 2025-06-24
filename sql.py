import psycopg2
from principal import df1086_estadual
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    cur.execute('SET search_path TO ptl, public')
    verificando_existencia_1086_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'ptl' AND table_type='BASE TABLE' AND table_name='ptl_1086_estadual';
    '''
    
    ptl_1086_ESTADUAL = \
    '''
    CREATE TABLE IF NOT EXISTS ptl.PTL_1086_ESTADUAL (
        id_ptl_1086_estadual SERIAL PRIMARY KEY ,
        id INTEGER NOT NULL,
        nome TEXT,
        id_produto INTEGER,
        referencia_tempo TEXT, 
        tipo_inspecao TEXT,
        quantidade_leite_adquir NUMERIC,
        trimestre INTEGER,
        quantidade_leite_indust NUMERIC,
        numero_informantes INTEGER,
        preco_medio NUMERIC,
        ano DATE); 
    '''

    cur.execute(ptl_1086_ESTADUAL)
    cur.execute(verificando_existencia_1086_estadual)
    resultado_1086_estadual = cur.fetchone()
    if resultado_1086_estadual[0] == 1:
        dropando_tabela_1086_estadual = '''
        TRUNCATE TABLE ptl.PTL_1086_ESTADUAL;
        '''
        cur.execute(dropando_tabela_1086_estadual)

    inserindo_ptl_1086_municipal = \
    '''
    INSERT INTO ptl.PTL_1086_ESTADUAL(id, nome, id_produto, referencia_tempo, tipo_inspecao, quantidade_leite_adquir, trimestre, quantidade_leite_indust, numero_informantes, preco_medio, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df1086_estadual.iterrows():
            dados = (
                i['id'], 
                i['nome'], 
                i['id_produto'], 
                i['Referencia_Tempo'], 
                i['Tipo_Inspecao'], 
                i['Quantidade de leite cru, resfriado ou não, adquirido'], 
                i['Trimestre'],
                i['Quantidade de leite cru, resfriado ou não, industrializado'],
                i['Número de informantes'],
                i['Preço médio'],
                i['ano']
            )
            cur.execute(inserindo_ptl_1086_municipal, dados)
    except psycopg2.Error as e:
      
        print(f"Erro ao inserir dados estaduais: {e}")
    conexao.commit()
    conexao.close()