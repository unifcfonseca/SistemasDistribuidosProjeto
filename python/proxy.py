import zmq

contexto = zmq.Context()

xsub = contexto.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = contexto.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

zmq.proxy(xsub, xpub)
