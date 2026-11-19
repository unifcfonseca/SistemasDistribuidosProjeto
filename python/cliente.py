from datetime import datetime, timezone
import msgpack

def dataHora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def enviaMsg(mensagem):
    return msgpack.packb(mensagem, use_bin_type=True)

def recebeMsg(mensagem):
    return msgpack.unpackb(mensagem, raw=False)

import os
import time
import zmq

nome = os.getenv("nome_bot", "bot1")

relogio = 0

contexto = zmq.Context()
req = contexto.socket(zmq.REQ)
req.connect("tcp://broker:5555")

sub = contexto.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")

inscritos = []

def requisitar(mensagem):
    global relogio

    relogio += 1
    mensagem["relogio"] = relogio
    print(f"{nome} enviou: {mensagem}", flush=True)
    req.send(enviaMsg(mensagem))

    resposta = recebeMsg(req.recv())
    print(f"{nome} recebeu: {resposta}", flush=True)

    return resposta

def login(usuario):
    mensagem = {"metodo" : "login", "dataHora": dataHora(), "usuario" : usuario}
    return requisitar(mensagem)
    
def listaCanal(usuario):
    mensagem = {"metodo" : "listaCanal", "dataHora": dataHora()}
    return requisitar(mensagem)

def criaCanal(usuario, nomeCanal):
    mensagem = {"metodo" : "criaCanal", "dataHora": dataHora(), "nomeCanal" : nomeCanal, "usuario" : usuario}
    return requisitar(mensagem)

def publicar(usuario, nomeCanal, texto):
    mensagem = {"metodo" : "publicar", "dataHora": dataHora(), "nomeCanal" : nomeCanal, "usuario" : usuario, "mensagem" : texto}
    return requisitar(mensagem)

def inscrever(nomeCanal):
    if nomeCanal not in inscritos:
        sub.setsockopt_string( zmq.SUBSCRIBE,nomeCanal + "|")
        inscritos.append(nomeCanal)
        print(f"{nome} inscrito em {nomeCanal}", flush=True)

def recebePub():
    global relogio

    while sub.poll(0):
            quadros = sub.recv_multipart()
            publicacao = recebeMsg(quadros[1])

            relogio = max(relogio, publicacao.get("relogio", 0)) + 1

            print(f"{nome} recebeu: "
                f"canal={publicacao['nomeCanal']} "
                f"mensagem={publicacao['mensagem']} "
                f"enviado={publicacao['dataHora']} "
                f"recebido={dataHora()} "
                f"relogio={relogio}",
                flush=True)

#tenta logar
while True:
    resposta = login(nome)
    if resposta["status"] == "ok":
        #logado
        break

    time.sleep(2)

numeroCanal = 0
numeroMensagem = 0

#metodos de canais
while True:
    resposta = listaCanal(nome)

    if resposta["status"] != "ok":
        time.sleep(2)
        continue

    canais = resposta["canais"]

    if len(canais) < 5:
        numeroCanal += 1

        canalReq = nome + "_canal_" + str(numeroCanal)

        resposta = criaCanal(nome, canalReq)
        if resposta["status"] == "ok":
            canais.append(canalReq)

        time.sleep(2)
        

    canaisDisponiveis = []
    for canal in canais:
        if canal not in inscritos:
            canaisDisponiveis.append(canal)

    if len(inscritos) < 3 and len(canaisDisponiveis) > 0:
        inscrever(canaisDisponiveis[0])
        time.sleep(2)

    for i in range(10):
        qntCanais = len(canais)
        if qntCanais > 0:
            canalPublicar = canais[i%qntCanais]
            numeroMensagem += 1
            texto = nome + " enviou mensagem numero " + str(numeroMensagem)
            publicar(nome, canalPublicar, texto)

        recebePub()
        time.sleep(1)
            



    