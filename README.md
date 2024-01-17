# Case Engenheiro de dados JR Nuvem Tecnologia
## Repositório com a resolução do case, na pasta do repositório contém os prints da carga no BD.


## Case:
Foram entregues 3 arquivos para serem carregados no banco de dados PostgreSQL.
Os arquivos entregues foram:

  * arq.01.csv
  * arq.02.csv
  * regiao.json

As solicitações estão nos arquivos arq.01.csv e arq.02.csv tendo a estrutura de colunas (id, solicitante, soliciado_em, executado_em, intervalo_em_seg, valor).
As regiões estão no arquivo regiao.json.

Devido a falta de tempo não foi garantido a qualidade dos dados dos arquivos csv, porém o cliente informou que temos como validar da seguinte forma:
  * os valores numéricos são formatados no padrão americano de casa decimal;
  * o atributo intervalo_em_seg é a subtração entre as colunas executado_em e solicitado_em;

Desta forma podemos efetuar o tratamento das informações do período e o intervalo caso estejam errados.
Executar o arquivo estrutura.sql para montar as tabelas de trabalho, carregar os dados nas tabelas. Após concluir:
  * gerar um dump das duas tabelas criadas com os dados carregados;
  * gerar um arquivo PARQUET particionado por regiao;



