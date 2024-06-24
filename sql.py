import psycopg2
from principal import df1086_estadual, df1086_nacional, df6830
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO ptl, public')
    # Verifica a existência das tabelas e retorna 1

    verificando_existencia_1086_nacional = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'ptl' AND table_type='BASE TABLE' AND table_name='ptl_1086_nacional';
    '''
    verificando_existencia_1086_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'ptl' AND table_type='BASE TABLE' AND table_name='ptl_1086_estadual';
    '''
    
    verificando_existencia_6830_nacional = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'ptl' AND table_type='BASE TABLE' AND table_name='ptl_6830_nacional';
    '''
    
    
    ptl_1086_NACIONAL = \
    '''
    CREATE TABLE IF NOT EXISTS ptl.PTL_1086_NACIONAL (
        id_ptl_1086_nacional SERIAL PRIMARY KEY,
        id INTEGER NOT NULL,
        nome TEXT,
        id_produto INTEGER,
        referencia_tempo TEXT, 
        tipo_inspecao TEXT,
        quantidade_leite_adquir INTEGER,
        trimestre INTEGER,
        quantidade_leite_indust INTEGER,
        ano DATE); 
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
        quantidade_leite_adquir INTEGER,
        trimestre INTEGER,
        quantidade_leite_indust INTEGER,
        ano DATE); 
    '''
    ptl_6830_NACIONAL = \
    '''
    CREATE TABLE IF NOT EXISTS ptl.PTL_6830_NACIONAL (
        id_ptl_6830_NACIONAL SERIAL PRIMARY KEY ,
        id INTEGER NOT NULL,
        nome TEXT,
        id_produto INTEGER,
        referencia_tempo TEXT, 
        quantidade_leite_adquir INTEGER,
        trimestre INTEGER,
        quantidade_leite_indust INTEGER,
        ano DATE); 
    '''

    cur.execute(ptl_1086_NACIONAL)
    cur.execute(ptl_1086_ESTADUAL)
    cur.execute(ptl_6830_NACIONAL)


    # Execute as consultas de verificação
    cur.execute(verificando_existencia_1086_nacional)
    resultado_1086_nacional = cur.fetchone()
    cur.execute(verificando_existencia_1086_estadual)
    resultado_1086_estadual = cur.fetchone()

    cur.execute(verificando_existencia_6830_nacional)
    resultado_6830_nacional = cur.fetchone()

    # Verifique se as tabelas existem e exclua, se necessário
    if resultado_1086_nacional[0] == 1:
        dropando_tabela_1086_nacional = '''
        TRUNCATE TABLE ptl.PTL_1086_NACIONAL
        '''
        cur.execute(dropando_tabela_1086_nacional)
    else:
        pass

    if resultado_1086_estadual[0] == 1:
        dropando_tabela_1086_estadual = '''
        TRUNCATE TABLE ptl.PTL_1086_ESTADUAL;
        '''
        cur.execute(dropando_tabela_1086_estadual)
    else:
        pass

    if resultado_6830_nacional[0] == 1:
        dropando_tabela_6830_nacional = '''
        TRUNCATE TABLE ptl.PTL_6830_NACIONAL;
        '''
        cur.execute(dropando_tabela_6830_nacional)
    else:
        pass    

    #INSERINDO DADOS
    inserindo_ptl_1086_nacional = \
    '''
    INSERT INTO ptl.PTL_1086_NACIONAL(id, nome, id_produto, referencia_tempo, tipo_inspecao, quantidade_leite_adquir, trimestre, quantidade_leite_indust, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df1086_nacional.iterrows():
            dados = (
                i['id'], 
                i['nome'], 
                i['id_produto'], 
                i['Referencia_Tempo'], 
                i['Tipo_Inspecao'], 
                i['Quantidade de leite cru, resfriado ou não, adquirido'], 
                i['Trimestre'],
                i['Quantidade de leite cru, resfriado ou não, industrializado'],
                i['ano']
            )
            cur.execute(inserindo_ptl_1086_nacional, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    inserindo_ptl_1086_municipal = \
    '''
    INSERT INTO ptl.PTL_1086_ESTADUAL(id, nome, id_produto, referencia_tempo, tipo_inspecao, quantidade_leite_adquir, trimestre, quantidade_leite_indust, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) 
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
                i['ano']
            )
            cur.execute(inserindo_ptl_1086_municipal, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")


    inserindo_ptl_6830_nacional= \
    '''
    INSERT INTO ptl.PTL_6830_NACIONAL(id, nome, id_produto, referencia_tempo, quantidade_leite_adquir, trimestre, quantidade_leite_indust, ano)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df6830.iterrows():
            dados = (
                i['id'], 
                i['nome'], 
                i['id_produto'], 
                i['Referencia_Tempo'], 
                i['Quantidade de leite cru, resfriado ou não, adquirido'], 
                i['Trimestre'],
                i['Quantidade de leite cru, resfriado ou não, industrializado'],
                i['ano']
            )
            cur.execute(inserindo_ptl_6830_nacional, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    conexao.commit()
    conexao.close()