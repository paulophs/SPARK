# SPARK


**Qual o objetivo do comando cache em Spark?**

O cache permite fazer a persistência de um RDD em memória onde cada nó armazena qualquer partição dele podendo assim reutilizar
em ações futuras no mesmo conjunto de dados ou em conjuntos derivados com velocidae amplificada em mais de 10x 

**O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?**

Spark suporta compartilhamento de dados em memória, processamento parcial e o recurso de cache
tornando ações futuras em um conjunto de dados mais rápidas.
Permite que diferentes jobs possam trabalhar com os mesmos dados e possibilita desenvolver processos complexos
enquanto o MapReduce precisa que cada etapa em fluxo tenha apenas uma fase de Map e uma fase Reduce
tendo os dados de cada etapa armazenados em disco.


**Qual é a função do SparkContext?**

É o client que conecta o Spark ao programa que está sendo desenvolvido. 
Ele pode ser acessado como uma variável em um programa  para utilizar os recursos e definir configurações dos clusters, execuçoes de jobs etc.


**Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**

É o DataSet do Spark, armazenado de forma distribuida e tolerante a falha, é imutável onde as transformações sempre 
ocorrem gerando um novo conjunto de dados.


**GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

O GroupBykey É um group by (alusão ao SQL) onde primeiramente é realizado todo uma agrupamento de dados
e somente em seguida efetuado o processamento desejado nos executores tendo assim um alto custo de performance,
enquanto o reduceByKey já realiza o processamneto de uma agregaçao em todas ocorrências das chaves de uma partição em memória
(processamento parcial)  


## Explique o que o código Scala abaixo faz

**val textFile = sc.textFile("hdfs://...")**
>Lê o textFile usando variável de contexto "sc" apontando o caminho para o arquivo desejado

**val counts = textFile.flatMap(line => line.split(" "))**
>Separa o conteudo do arquivo em linhas utilizando " "como divisor

**.map(word => (word, 1))**
>Mapeamento de palavras em tupla (chave, valor)  sendo cada palavra a chave , 1 corespondendo uma ocorrência da palavra 

**.reduceByKey(_ + _)**
>agregação para as ocorrências de cada chave (realizando um 'sum' por palavra)

**counts.saveAsTextFile("hdfs://...")**
>salva conjunto de dados (RDD)  no apontamento do diretorio desejado 
