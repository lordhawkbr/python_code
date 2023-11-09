useCols = ["Agente_Causador_Acidente","Data_Acidente","CBO","CID_10","CNAE2_0_Empregador","CNAE2_0_Empregador_1","Indica_obito_Acidente","Munic_Empr","Natureza_da_Lesao","Parte_Corpo_Atingida","Sexo","Tipo_do_Acidente","UF_Munic_Acidente","UF_Munic_Empregador","Data_Afastamento","Data_Nascimento"]
selectTBTempSQL = "INSERT INTO ft_cats(id_empregador,id_doenca,id_profissao,id_local) SELECT e.id,d.id,p.id,l.id FROM ft_temp_cats ftc join dm_empregadores e on e.CNAE2_0_Empregador = ftc.CNAE2_0_Empregador join dm_doencas d on d.CID_10 = ftc.CID_10 join dm_profissoes p on p.CBO = ftc.CBO join dm_localidades l on l.CIM = ftc.CIM"
# selectTBTempSQL = "SELECT acdnt.id id_acidente,agn.id id_agente,cid.id id_doenca,emp.id id_empregador,lcl.id id_local,cbo.id id_profissao,les.id id_tipo_lesao FROM ft_temp_cats temp  JOIN dm_acidentes acdnt  ON acdnt.UF_Munic_Acidente = temp.UF_Munic_Acidente AND acdnt.Data_Acidente = temp.Data_Acidente AND acdnt.Emitente_CAT = temp.Emitente_CAT AND acdnt.Especie_do_beneficio = temp.Especie_do_beneficio AND acdnt.Filiacao_Segurado = temp.Filiacao_Segurado AND acdnt.Indica_obito_Acidente = temp.Indica_obito_Acidente AND acdnt.Origem_de_Cadastramento_CAT = temp.Origem_de_Cadastramento_CAT AND acdnt.Sexo = temp.Sexo AND acdnt.Tipo_do_Acidente = temp.Tipo_do_Acidente AND acdnt.Data_Afastamento = temp.Data_Afastamento AND acdnt.Data_Despacho_Beneficio = temp.Data_Despacho_Beneficio AND acdnt.Data_Nascimento = temp.Data_Nascimento AND acdnt.Data_Emissao_CAT = temp.Data_Emissao_CAT  JOIN dm_agentes agn  ON agn.Agente_Causador_Acidente = temp.Agente_Causador_Acidente  JOIN dm_doencas cid  ON cid.CID_10 = temp.CID_10  JOIN dm_empregadores emp  ON emp.CNAE2_0_Empregador = temp.CNAE2_0_Empregador AND emp.CNAE2_0_Empregador_1 = temp.CNAE2_0_Empregador_1  JOIN dm_localidades lcl  ON lcl.Munic_Empr = temp.Munic_Empr  AND lcl.UF_Munic__Empregador = temp.UF_Munic__Empregador  JOIN dm_profissoes cbo  ON cbo.CBO = temp.CBO  JOIN dm_tipo_lesao les  ON les.Natureza_da_Lesao = temp.Natureza_da_Lesao  AND les.Parte_Corpo_Atingida = temp.Parte_Corpo_Atingida limit 1000 offset 0"
countRowsSQL = "SELECT COUNT(*) FROM ft_cats"
create_tables = "CREATE TABLE IF NOT EXISTS `dm_acidentes` (`id` int NOT NULL, `UF_Munic_Acidente` varchar(50) DEFAULT NULL, `Data_Acidente` varchar(15) DEFAULT NULL, `Indica_obito_Acidente` varchar(20) DEFAULT NULL, `Sexo` varchar(20) DEFAULT NULL, `Tipo_do_Acidente` varchar(50) DEFAULT NULL, `Data_Nascimento` varchar(10) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_agentes` (`id` int NOT NULL, `Agente_Causador_Acidente` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_doencas` (`id` int NOT NULL, `CID_10` varchar(10) DEFAULT NULL, `Doenca` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_empregadores` (`id` int NOT NULL, `CNAE2_0_Empregador` int DEFAULT NULL, `CNAE2_0_Empregador_1` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_localidades` (`id` int NOT NULL, `Munic_Empr` varchar(50) DEFAULT NULL, `CIM` int DEFAULT NULL, `UF_Munic_Empregador` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_profissoes` (`id` int NOT NULL, `CBO` int DEFAULT NULL, `Ocupacao` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_tipo_lesao` (`id` int NOT NULL, `Natureza_da_Lesao` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `dm_partescorpo` (`id` INT NOT NULL, `Parte_Corpo_Atingida` VARCHAR(50) NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `ft_logs` (`id` int NOT NULL AUTO_INCREMENT, `data_evento` varchar(15) DEFAULT NULL, `hora_evento` varchar(15) DEFAULT NULL, `evento` varchar(100) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `ft_temp_cats` (`id` int NOT NULL AUTO_INCREMENT, `Agente_Causador_Acidente` varchar(50) DEFAULT NULL, `Data_Acidente` varchar(15) DEFAULT NULL, `CBO` int DEFAULT NULL, `Ocupacao` varchar(50) DEFAULT NULL, `CID_10` varchar(10) DEFAULT NULL, `Doenca` varchar(50) DEFAULT NULL, `CNAE2_0_Empregador` int DEFAULT NULL, `CNAE2_0_Empregador_1` varchar(50) DEFAULT NULL, `Indica_obito_Acidente` varchar(50) DEFAULT NULL, `Munic_Empr` varchar(50) DEFAULT NULL, `CIM` int DEFAULT NULL, `Natureza_da_Lesao` varchar(50) DEFAULT NULL, `Parte_Corpo_Atingida` varchar(50) DEFAULT NULL, `Sexo` varchar(20) DEFAULT NULL, `Tipo_do_Acidente` varchar(50) DEFAULT NULL, `UF_Munic_Acidente` varchar(50) DEFAULT NULL, `UF_Munic_Empregador` varchar(50) DEFAULT NULL, `Data_Afastamento` varchar(7) DEFAULT NULL, `Data_Nascimento` varchar(10) DEFAULT NULL, PRIMARY KEY (`id`));CREATE TABLE IF NOT EXISTS `ft_cats` (`id_doencas` INT NULL, `id_agentes` INT NULL, `id_profissoes` INT NULL, `id_empregadores` INT NULL, `id_localidades` INT NULL, `id_tipo_lesao` INT NULL,`id_partescorpo` INT NULL, `id_acidentes` INT NULL);"