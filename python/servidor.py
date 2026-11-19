from datetime import datetime, timezone
import time
import msgpack

relogio = 0
ajusteHora = 0

rank = 0
coordenador = None
rankCoordenador = 0

contadorMensagens = 0

def dataHora():
    return datetime.fromtimestamp(time.time() + ajusteHora,timezone.utc).isoformat(timespec="seconds")

def horaAtual():
    return time.time() + ajusteHora

def enviaMsg(mensagem):
    return msgpack.packb(mensagem, use_bin_type=True)

def recebeMsg(mensagem):
    return msgpack.unpackb(mensagem, raw=False)

import json
import os
import zmq
import uuid

nome = os.getenv("nome_servidor", "servidor1")
arquivoDados = os.getenv("dados", "dados.json")

def carregar_dados():
    if os.path.exists(arquivoDados):
        with open(arquivoDados, "r", encoding="utf-8") as arquivo:
            conteudo = json.load(arquivo)
    else:
        conteudo = {"logins": [], "canais": []}

    if "publicacoes" not in conteudo:
        conteudo["publicacoes"] = []

    return conteudo

def publicarEstado():
    global relogio

    relogio += 1

    mensagem = {"servidor": nome, "dados": dados, "relogio": relogio}

    pub.send_multipart([b"replica|", enviaMsg(mensagem)])

def mesclarEstado(dadosRecebidos):
    alterou = False

    for tipo in ["logins", "canais", "publicacoes"]:
        registros = dadosRecebidos.get(tipo, [])
        existentes = set()

        for registro in dados[tipo]:
            if tipo == "canais":
                chave = registro["nome"]
            else:
                chave = registro.get("id")

                if chave is None:
                    chave = json.dumps(registro, sort_keys=True, ensure_ascii=False)

            existentes.add(chave)

        for registro in registros:
            if tipo == "canais":
                chave = registro["nome"]
            else:
                chave = registro.get("id")

                if chave is None:
                    chave = json.dumps(registro, sort_keys=True, ensure_ascii=False)

            if chave not in existentes:
                dados[tipo].append(registro)
                existentes.add(chave)
                alterou = True

    if alterou:
        salvar_dados(dados, replicar=False)
        print(f"{nome} recebeu dados", flush=True)

def salvar_dados(dadosSalvar, replicar=True):
    pasta = os.path.dirname(arquivoDados)

    if pasta:
        os.makedirs(pasta, exist_ok=True)

    with open(arquivoDados, "w", encoding="utf-8") as arquivo:
        json.dump(dadosSalvar, arquivo, ensure_ascii=False, indent=2)

    if replicar:
        publicarEstado()

def referencia(metodo):
    global relogio

    relogio += 1

    mensagem = {"metodo": metodo,"nome": nome,"dataHora": dataHora(),"relogio": relogio}

    ref.send(enviaMsg(mensagem))
    resposta = recebeMsg(ref.recv())

    relogio = max(relogio, resposta.get("relogio", 0)) + 1

    print(f"{nome} referencia recebeu: {resposta}",flush=True)

    return resposta


def requisitarServ(nomeServidor, mensagem):
    global relogio

    newReq = contexto.socket(zmq.REQ)
    newReq.setsockopt(zmq.LINGER, 0)
    newReq.connect(f"tcp://{nomeServidor}:5560")

    try:
        relogio += 1
        mensagem["relogio"] = relogio
        mensagem["dataHora"] = dataHora()

        newReq.send(enviaMsg(mensagem))

        if newReq.poll(1500):
            resposta = recebeMsg(newReq.recv())
            relogio = max(relogio,resposta.get("relogio", 0)) + 1
            return resposta
        
        return None

    finally:
        newReq.close()

def anunciarCoordenador():
    global relogio, coordenador, rankCoordenador

    coordenador = nome
    rankCoordenador = rank

    relogio += 1

    mensagem = {"metodo": "coordenador",
        "coordenador": nome,
        "rank": rank,
        "dataHora": dataHora(),
        "relogio": relogio}

    pub.send_multipart([b"servers|", enviaMsg(mensagem)])

    print(f"{nome} coordenador: rank {rank}",flush=True)

def eleicao():
    global coordenador, rankCoordenador

    resposta = referencia("listaServidores")

    if resposta["status"] != "ok":
        return

    servidores = resposta["servidores"]

    servidores.sort(key=lambda servidor: servidor["rank"], reverse=True)

    for servidor in servidores:
        if servidor["rank"] > rank:
            mensagem = {"metodo": "eleicao","nome": nome,"rank": rank}

            respostaEleicao = requisitarServ(servidor["nome"], mensagem)

            if (respostaEleicao is not None and respostaEleicao.get("status") == "ok"):
                coordenador = servidor["nome"]
                rankCoordenador = servidor["rank"]

                print(f"{coordenador} coordenador de {nome}",flush=True)
                return

    if coordenador != nome:
        anunciarCoordenador()


def receberAnuncio():
    global relogio, coordenador, rankCoordenador

    while sub.poll(0):
        quadros = sub.recv_multipart()
        mensagem = recebeMsg(quadros[1])

        relogio = max(relogio, mensagem.get("relogio", 0)) + 1

        if quadros[0] == b"replica|":
            if mensagem["servidor"] != nome:
                mesclarEstado(mensagem["dados"])

        elif quadros[0] == b"servers|":
            novoRank = mensagem["rank"]

            if novoRank >= rankCoordenador:
                coordenador = mensagem["coordenador"]
                rankCoordenador = novoRank

                print(f"{nome} recebeu coordenador: {coordenador}", flush=True)


def sincronizar():
    global ajusteHora

    if coordenador is None:
        eleicao()

    if coordenador == nome:
        return

    inicio = time.time()

    mensagem = {"metodo": "sincronizar","nome": nome,"hora": horaAtual()}

    resposta = requisitarServ(coordenador, mensagem)
    fim = time.time()

    if resposta is None:
        eleicao()
        return

    if resposta.get("status") != "ok":
        eleicao()
        return

    horaLocal = (inicio + fim) / 2

    ajusteHora = resposta["horaCorreta"] - horaLocal

    print(f"{nome} sincronizou com {coordenador} Ajuste={ajusteHora:.6f}s Hora={dataHora()}",flush=True)

def atenderServidor():
    global relogio, ajusteHora

    pacote = peer.recv()
    requisicao = recebeMsg(pacote)

    relogio = max(relogio, requisicao.get("relogio", 0)) + 1

    metodo = requisicao.get("metodo")

    resposta = {"metodo": metodo, "status": "ok"}

    anunciar = False

    if metodo == "eleicao":
        if rank > requisicao.get("rank", 0):
            resposta["mensagem"] = "OK"

            if coordenador != nome:
                anunciar = True
        else:
            resposta["status"] = "erro"
            resposta["mensagem"] = "rank menor"

    elif metodo == "sincronizar":
        if coordenador != nome:
            resposta["status"] = "erro"
            resposta["mensagem"] = "servidor nao eh coordenador"

        else:
            horaCoordenador = horaAtual()
            horaServidor = requisicao["hora"]

            horaCorreta = (horaCoordenador + horaServidor) / 2

            ajusteHora += horaCorreta - horaCoordenador

            resposta["horaCorreta"] = horaCorreta

            print(f"{nome}: hora local={horaCoordenador:.3f} hora recebida={horaServidor:.3f} media={horaCorreta:.3f}",flush=True)

    else:
        resposta["status"] = "erro"
        resposta["mensagem"] = "metodo desconhecido"

    relogio += 1
    resposta["relogio"] = relogio
    resposta["dataHora"] = dataHora()

    peer.send(enviaMsg(resposta))

    if anunciar:
        anunciarCoordenador()

def recebeRequisicao(requisicao, dados):
    global relogio

    metodo = requisicao.get("metodo")
    resposta = {"metodo": metodo, "status": "ok"}

    if metodo == "login":
        usuario = requisicao.get("usuario")
        dados["logins"].append({"id": uuid.uuid4().hex, "usuario": usuario, "dataHora": requisicao["dataHora"]})
        salvar_dados(dados)
        resposta["mensagem"] = "Login"

    elif metodo == "criaCanal":
        nomeCanal = requisicao.get("nomeCanal")
        for canal in dados["canais"]:
            if nomeCanal == canal["nome"]:
                return {"metodo": metodo, "status": "erro", "mensagem": "Canal existente", "dataHora": dataHora()}
        dados["canais"].append({"id": uuid.uuid4().hex, "nome": nomeCanal, "usuario": requisicao.get("usuario"), "dataHora": requisicao["dataHora"]})
        salvar_dados(dados)
        resposta["mensagem"] = "Canal criado"

    elif metodo == "listaCanal":
        canais = []
        for canal in dados["canais"]:
            canais.append(canal["nome"])
        resposta["canais"] = canais

    elif metodo == "publicar":
        nomeCanal = requisicao.get("nomeCanal")
        mensagem = requisicao.get("mensagem")
        usuario = requisicao.get("usuario")

        canalExistente = False
        for canal in dados["canais"]:
            if nomeCanal == canal["nome"]:
                canalExistente = True
                break

        if not canalExistente:
            resposta["status"] = "erro"
            resposta["mensagem"] = "o canal nao existe"

        else:
            relogio += 1

            publicacao = {"id": uuid.uuid4().hex, "nomeCanal": nomeCanal, "usuario": usuario, "mensagem": mensagem, "dataHora": dataHora(), "relogio": relogio}
            dados["publicacoes"].append(publicacao)
            salvar_dados(dados)

            print(f"{nome} publicou: {publicacao}", flush=True)

            pub.send_multipart([(nomeCanal + "|").encode("utf-8"),enviaMsg(publicacao)])

            resposta["mensagem"] = "Publicacao feita"

    else:
        resposta["status"] = "erro"
        resposta["mensagem"] = "Acao desconhecida"

    return resposta

dados = carregar_dados()
contexto = zmq.Context()

rep = contexto.socket(zmq.REP)
rep.connect(f"tcp://broker:5556")

pub = contexto.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

ref = contexto.socket(zmq.REQ)
ref.connect("tcp://referencia:5559")

peer = contexto.socket(zmq.REP)
peer.bind("tcp://*:5560")

sub = contexto.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "servers|")
sub.setsockopt_string(zmq.SUBSCRIBE, "replica|")

resposta = referencia("rank")

if resposta["status"] == "ok":
    rank = resposta["rank"]
    print(f"{nome}: rank {rank}", flush=True)

eleicao()

poller = zmq.Poller()
poller.register(rep, zmq.POLLIN)
poller.register(peer, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

print(f"{nome} iniciado", flush=True)

referencia("listaServidores")

ultimaReplicacao = 0

while True:
    eventos = dict(poller.poll(1000))

    if time.monotonic() - ultimaReplicacao >= 2:
        publicarEstado()
        ultimaReplicacao = time.monotonic()

    if peer in eventos:
        atenderServidor()

    if sub in eventos:
        receberAnuncio()

    if rep in eventos:
        pacote = rep.recv()

        contadorMensagens += 1

        try:
            requisicao = recebeMsg(pacote)
            
            relogio = max(relogio, requisicao.get("relogio", 0)) + 1

            print(f"{nome} recebeu: {requisicao}", flush=True)
            resposta = recebeRequisicao(requisicao, dados)
        except Exception as exc:
            resposta = {"metodo": "desconhecido", "status": "erro", "mensagem": str(exc), "dataHora": dataHora()}

        relogio += 1

        resposta["relogio"] = relogio
        resposta["dataHora"] = dataHora()

        print(f"{nome} enviou: {resposta}", flush=True)
        rep.send(enviaMsg(resposta))

        if contadorMensagens % 10 == 0:
            resposta = referencia("heartbeat")

            if resposta["status"] != "ok":
                resposta = referencia("rank")

                if resposta["status"] == "ok":
                    rank = resposta["rank"]
                    print(f"novo rank de {nome}: {rank}", flush=True)

            eleicao()

        if contadorMensagens % 15 == 0:
            sincronizar()