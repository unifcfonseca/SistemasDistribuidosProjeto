from datetime import datetime, timezone
import msgpack

def dataHora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def enviaMsg(mensagem):
    return msgpack.packb(mensagem, use_bin_type=True)

def recebeMsg(mensagem):
    return msgpack.unpackb(mensagem, raw=False)

import time
import zmq

relogio = 0
proximoRank = 1
servidores = {}

timeout = 60

def recebeRequisicao(requisicao):
    global relogio, proximoRank

    relogio = max(relogio, requisicao.get("relogio", 0)) + 1

    metodo = requisicao.get("metodo")
    nome = requisicao.get("nome")

    resposta = {"metodo": metodo, "status": "ok"}

    if metodo == "rank":
        if nome not in servidores:
            servidores[nome] = {"rank": proximoRank, "ultimoHeartbeat": time.monotonic()}
            proximoRank += 1
        else:
            servidores[nome]["ultimoHeartbeat"] = time.monotonic()

        resposta["rank"] = servidores[nome]["rank"]
        print(f"{nome} registrado", flush=True)

    elif metodo == "listaServidores":
        lista = []

        for nomeServidor in servidores:
            lista.append({"nome": nomeServidor,"rank": servidores[nomeServidor]["rank"]})

        resposta["servidores"] = lista

    elif metodo == "heartbeat":
        if nome in servidores:
            servidores[nome]["ultimoHeartbeat"] = time.monotonic()
            resposta["mensagem"] = "Heartbeat recebido"
        else:
            resposta["status"] = "erro"
            resposta["mensagem"] = "servidor nao cadastrado"

    else:
        resposta["status"] = "erro"
        resposta["mensagem"] = "metodo desconhecido"

    relogio += 1
    resposta["relogio"] = relogio
    resposta["dataHora"] = dataHora()

    return resposta


contexto = zmq.Context()

rep = contexto.socket(zmq.REP)
rep.bind("tcp://*:5559")

while True:
    if rep.poll(1000):
        pacote = rep.recv()

        try:
            requisicao = recebeMsg(pacote)
            print(f"Referencia recebeu: {requisicao}", flush=True)

            resposta = recebeRequisicao(requisicao)

        except Exception as exc:
            relogio += 1
            resposta = {"metodo": "desconhecido",
                "status": "erro",
                "mensagem": str(exc),
                "dataHora": dataHora(),
                "relogio": relogio}

        print(f"Referencia enviou: {resposta}", flush=True)
        rep.send(enviaMsg(resposta))

    agora = time.monotonic()

    for nomeServidor in list(servidores):
        ultimo = servidores[nomeServidor]["ultimoHeartbeat"]

        if agora - ultimo > timeout:
            print(f"Servidor removido por timeout: {nomeServidor}", flush=True)
            del servidores[nomeServidor]