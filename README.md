# ExemploTableAPIFlinkCEP

Projeto de teste para implementar uma pipeline simples com alguma funcionalidade do Flink CEP. Implementa uma "sink" para o banco de dados Postgres. Utiliza [Table API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/tableApi.html).

## Como funciona
[TableAPIPipeline](https://github.com/jlucartc/ExemploTableAPIFlinkCEP/blob/master/src/main/scala/FlinkCEPClasses/TableAPIPipeline.scala) é a classe
que implementa o código de leitura
e inserção de dados no banco
via TableAPI.

* Linhas 14 até 17 inicializam a pipeline
* Linha 19 lê as linhas do arquivo 'lines'
* Linha 22 transforma linhas em tuplas (String,Timestamp,Double,Double)
* Linhas 25 a 27 inicializam o *StreamTableEnvironment* e a sink
responsável por fazer o acesso ao banco de dados

* O código de configuração de acesso ao banco é bastante similar
ao que é utilizado em diversas linguagens. É necessário informar dados como
host, porta, nome da database, usuário e senha para acessar o banco corretamente.

* O driver do Postgres é importado no projeto pelo arquivo [build.sbt](https://github.com/jlucartc/ExemploTableAPIFlinkCEP/blob/master/build.sbt), assim como as demais
libs.

* Linhas 45-49: Em seguida, é necessário registrar a tabela no ambiente do TableAPI. Para isso,
é necessário informar os nomes dos campos, os tipos de dados e a saida de dados, que
nesse caso é a variável tableSink

* Linha 51: o ambiente de TableAPI acessa a tabela registrada e guarda na variável table.
* Linha 52: o comando de inserção é executado para cada linha lida do arquivo.

## Testando a aplicação

Para testar a aplicação, basta executar `sbt run` na pasta do projeto.










