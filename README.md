# SistemasDistribuidosProjeto
Sistema Distribuído de Mensagens

Projeto de um sistema distribuído de mensagens desenvolvido em Python, C++ e Java, com comunicação entre diferentes clientes e servidores.

O sistema permite que usuários criem canais, publiquem mensagens e recebam publicações de outros clientes. A comunicação é realizada utilizando ZeroMQ e MessagePack, enquanto o Docker Compose é utilizado para executar e integrar os serviços.

O ambiente conta com seis servidores e seis clientes, distribuídos igualmente entre as três linguagens, além de um broker, um proxy e um serviço de referência.
