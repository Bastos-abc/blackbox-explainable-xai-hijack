# Abrindo a Caixa-Preta  -- Aplicando IA Explicável para Aprimorar a Detecção de Sequestros de Prefixo

Os códigos aqui disponibilizados foram baseados nos desenvolvidos pelos pesquisadores do artigo "A System to Detect Forged-Origin BGP Hijacks" disponível em https://dfoh.uclouvain.be/

O código disponibilizado foi ajustado para análise das features com técnicas de XAI, com o uso da ferramenta Trustee (https://trusteeml.github.io/index.html), e os resultados obtidos estarão disponíveis no artigo "Abrindo a Caixa-Preta - Aplicando IA Explicável para Aprimorar a Detecção de Sequestros de Prefixo" que será apresentado no 24º Simpósio Brasileiro em Segurança da Informação e de Sistemas Computacionais (SBSeg).

## Ambiente de execução

O código disponibilizado foi executado em servidor Linux (Ubuntu 20.04) com Python 3.10.12 com as seguintes bibliotecas instaladas:

- networkx~=3.3
- colorama~=0.4.6
- click~=8.1.7
- pandas~=2.2.2
- scikit-learn~=1.5.1
- numpy~=2.0.1
- scipy~=1.14.0
- requests~=2.32.3
- graphviz~=0.20.3
- pdfplumber~=0.11.3
- trustee~=1.1.6

A raiz de execução do código está ajustada no código para "/home/dfoh_nv"

## Criar dataset para execução

O código "prepare_environment.py" é o responsável por preparar os dados necessários para execução inicial do sistema. Sua execução é demorada devido ao volume de informação que tem que ser obtida de diversas fontes (repositórios).

O período a ser analisado deve ser ajustado nas linhas 208 e 209 e a raiz de execução do sistema na linha 225.

## Rodando o sistema para um período

A execução completa do sistema está ajustada no arquivo "run_test_for_period.py", devendo-se ajustar o período de execução na linha 37, assim como o caminho dos diretórios que armazenam os arquivos que foram criados durante a execução do código "prepare_environment.py" na linha 40. O padrão dos diretórios devem ser mantidos, finalizando com db_m1, db_m4 e db_m5, da forma em que se encontram no arquivo.

## Fazendo a análise com a ferramenta Trustee

O código "trustee_dfoh.py" está com todos os ajustes realizados para obtenção dos valores apresentados no artigo para a avaliação com XAI. A execução do código necessita da passagem de parâmetros.

Ex. 
```sh
python3 trustee_dfoh.py --date 2023-12-01 --db_dir /home/dfoh_nv/db_m1 --n_threads 5 --outfolder /home/dfoh_nv/trustee
```



