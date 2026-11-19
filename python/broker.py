from datetime import datetime, timezone
import msgpack

def dataHora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def enviaMsg(mensagem):
    return msgpack.packb(mensagem, use_bin_type=True)

def recebeMsg(mensagem):
    return msgpack.unpackb(mensagem, raw=False)

import zmq

contexto = zmq.Context()

router = contexto.socket(zmq.ROUTER)
router.bind("tcp://*:5555")

dealer = contexto.socket(zmq.DEALER)
dealer.bind("tcp://*:5556")

poller = zmq.Poller()
poller.register(router, zmq.POLLIN)
poller.register(dealer, zmq.POLLIN)

while True:
    mensagens = dict(poller.poll())

    if router in mensagens:
        quadros = router.recv_multipart()
        print(f"broker request: {recebeMsg(quadros[-1])}", flush=True)
        dealer.send_multipart(quadros)

    if dealer in mensagens:
        quadros = dealer.recv_multipart()
        print(f"broker reply: {recebeMsg(quadros[-1])}", flush=True)
        router.send_multipart(quadros)
