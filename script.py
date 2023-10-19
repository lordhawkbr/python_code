import sqlalchemy as sa

metadata = sa.MetaData()

dm_agentes = sa.Table('dm_agentes', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('Agente_Causador_Acidente', sa.String(50, collation="utf8mb4")),
)
dm_profissoes = sa.Table('dm_profissoes', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('CBO', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)
dm_doencas = sa.Table('dm_doencas', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('CID_10', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)
dm_empregadores = sa.Table('dm_empregadores', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('CNAE2_0_Empregador', sa.String(50, collation="utf8mb4")),
  sa.Column('CNAE2_0_Empregador_1', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)
dm_localidades = sa.Table('dm_localidades', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('Munic_Empr', sa.String(50, collation="utf8mb4")),
  sa.Column('UF_Munic__Empregador', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)
dm_tipo_lesao = sa.Table('dm_tipo_lesao', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('Natureza_da_Lesao', sa.String(50, collation="utf8mb4")),
  sa.Column('Parte_Corpo_Atingida', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)
dm_acidentes = sa.Table('dm_acidentes', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('UF_Munic__Acidente' , sa.String(50, collation="utf8mb4")),
    sa.Column('Data_Acidente' , sa.String(10, collation="utf8mb4")),
    sa.Column('Emitente_CAT' , sa.String(50, collation="utf8mb4")),
    sa.Column('Especie_do_beneficio' , sa.String(50, collation="utf8mb4")),
    sa.Column('Filiacao_Segurado' , sa.String(50, collation="utf8mb4")),
    sa.Column('Indica_obito_Acidente', sa.String(3), collation="utf8mb4" ),
    sa.Column('Origem_de_Cadastramento_CAT' , sa.String(50, collation="utf8mb4")),
    sa.Column('Sexo' , sa.String(13, collation="utf8mb4")),
    sa.Column('Tipo_do_Acidente' , sa.String(50, collation="utf8mb4")),
    sa.Column('Data_Afastamento' , sa.String(10, collation="utf8mb4")),
    sa.Column('Data_Despacho_Beneficio' , sa.String(10, collation="utf8mb4")),
    sa.Column('Data_Nascimento' , sa.String(10, collation="utf8mb4")),
    sa.Column('Data_Emissao_CAT' , sa.String(10, collation="utf8mb4")),
  # extend_existing=True
)
ft_cats = sa.Table('ft_cats', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('id_agente', sa.Integer, sa.ForeignKey('dm_agentes.id')),
  sa.Column('id_doenca', sa.Integer, sa.ForeignKey('dm_doencas.id')),
  sa.Column('id_empregador', sa.Integer, sa.ForeignKey('dm_empregadores.id')),
  sa.Column('id_acidente', sa.Integer, sa.ForeignKey('dm_acidentes.id')),
  sa.Column('id_tipo_lesao', sa.Integer, sa.ForeignKey('dm_tipo_lesao.id')),
  sa.Column('id_local', sa.Integer, sa.ForeignKey('dm_localidades.id')),
  sa.Column('id_profissao', sa.Integer, sa.ForeignKey('dm_profissoes.id')),
  # extend_existing=True
)
ft_temp_cats = sa.Table('ft_temp_cats', metadata,
  sa.Column('id', sa.Integer, primary_key=True),
  sa.Column('Agente_Causador_Acidente', sa.String(50, collation="utf8mb4")),
  sa.Column('Data_Acidente', sa.String(50, collation="utf8mb4")),
  sa.Column('CBO', sa.String(50, collation="utf8mb4")),
  sa.Column('CID_10', sa.String(50, collation="utf8mb4")),
  sa.Column('CNAE2_0_Empregador', sa.String(50, collation="utf8mb4")),
  sa.Column('CNAE2_0_Empregador_1', sa.String(50, collation="utf8mb4")),
  sa.Column('Emitente_CAT', sa.String(50, collation="utf8mb4")),
  sa.Column('Especie_do_beneficio', sa.String(50, collation="utf8mb4")),
  sa.Column('Filiacao_Segurado', sa.String(50, collation="utf8mb4")),
  sa.Column('Indica_obito_Acidente', sa.String(50, collation="utf8mb4")),
  sa.Column('Munic_Empr', sa.String(50, collation="utf8mb4")),
  sa.Column('Natureza_da_Lesao', sa.String(50, collation="utf8mb4")),
  sa.Column('Origem_de_Cadastramento_CAT', sa.String(50, collation="utf8mb4")),
  sa.Column('Parte_Corpo_Atingida', sa.String(50, collation="utf8mb4")),
  sa.Column('Sexo', sa.String(50, collation="utf8mb4")),
  sa.Column('Tipo_do_Acidente', sa.String(50, collation="utf8mb4")),
  sa.Column('UF_Munic__Acidente', sa.String(50, collation="utf8mb4")),
  sa.Column('UF_Munic__Empregador', sa.String(50, collation="utf8mb4")),
  sa.Column('Data_Afastamento', sa.String(50, collation="utf8mb4")),
  sa.Column('Data_Despacho_Beneficio', sa.String(50, collation="utf8mb4")),
  sa.Column('Data_Nascimento', sa.String(50, collation="utf8mb4")),
  sa.Column('Data_Emissao_CAT', sa.String(50, collation="utf8mb4")),
  # extend_existing=True
)