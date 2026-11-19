
#include <zmq.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <ctime>
#include <algorithm>
#include <thread>
#include <chrono>
#include <cstdlib>

using namespace std;
using json = nlohmann::json;

string dataHora() {
    time_t agora = time(nullptr);
    char texto[40];
    strftime(texto, sizeof(texto), "%Y-%m-%dT%H:%M:%S+00:00", gmtime(&agora));
    return texto;
}

void enviaMsg(void* socket, json mensagem) {
    vector<uint8_t> bytes = json::to_msgpack(mensagem);
    zmq_send(socket, bytes.data(), bytes.size(), 0);
}

json recebeMsg(void* socket) {
    zmq_msg_t mensagem;
    zmq_msg_init(&mensagem);
    zmq_msg_recv(&mensagem, socket, 0);

    uint8_t* inicio = (uint8_t*)zmq_msg_data(&mensagem);
    vector<uint8_t> bytes(inicio, inicio + zmq_msg_size(&mensagem));

    zmq_msg_close(&mensagem);
    return json::from_msgpack(bytes);
}

string nome = "bot_cpp1";
int relogio = 0;

void* contexto = zmq_ctx_new();

void* req = zmq_socket(contexto, ZMQ_REQ);
void* sub = zmq_socket(contexto, ZMQ_SUB);

vector<string> inscritos;

json requisitar(json mensagem) {
    relogio++;
    mensagem["relogio"] = relogio;

    cout << nome << " enviou: " << mensagem.dump() << endl;
    enviaMsg(req, mensagem);

    json resposta = recebeMsg(req);
    cout << nome << " recebeu: " << resposta.dump() << endl;

    return resposta;
}

json login(string usuario) {
    json mensagem = {{"metodo", "login"}, {"dataHora", dataHora()}, {"usuario", usuario}};
    return requisitar(mensagem);
}

json listaCanal(string usuario) {
    json mensagem = {{"metodo", "listaCanal"}, {"dataHora", dataHora()}};
    return requisitar(mensagem);
}

json criaCanal(string usuario, string nomeCanal) {
    json mensagem = {{"metodo", "criaCanal"}, {"dataHora", dataHora()}, {"nomeCanal", nomeCanal}, {"usuario", usuario}};
    return requisitar(mensagem);
}

json publicar(string usuario, string nomeCanal, string texto) {
    json mensagem = {{"metodo", "publicar"}, {"dataHora", dataHora()}, {"nomeCanal", nomeCanal}, {"usuario", usuario}, {"mensagem", texto}};
    return requisitar(mensagem);
}

void inscrever(string nomeCanal) {
    if (find(inscritos.begin(), inscritos.end(), nomeCanal) == inscritos.end()) {
        string topico = nomeCanal + "|";
        zmq_setsockopt(sub, ZMQ_SUBSCRIBE, topico.data(), topico.size());

        inscritos.push_back(nomeCanal);
        cout << nome << " inscrito em " << nomeCanal << endl;
    }
}

void recebePub() {
    zmq_pollitem_t item = {sub, 0, ZMQ_POLLIN, 0};

    while (zmq_poll(&item, 1, 0) > 0 && (item.revents & ZMQ_POLLIN)) {
        zmq_msg_t topico;
        zmq_msg_init(&topico);
        zmq_msg_recv(&topico, sub, 0);
        zmq_msg_close(&topico);

        json publicacao = recebeMsg(sub);

        relogio = max(relogio, publicacao.value("relogio", 0)) + 1;

        cout << nome << " recebeu:  canal=" << publicacao["nomeCanal"].get<string>();
        cout << " mensagem=" << publicacao["mensagem"].get<string>();
        cout << " enviado=" << publicacao["dataHora"].get<string>();
        cout << " recebido=" << dataHora();
        cout << " relogio=" << relogio << endl;

        item.revents = 0;
    }
}

int main() {
    if (getenv("nome_bot")){
        nome = getenv("nome_bot");
    }

    zmq_connect(req, "tcp://broker:5555");
    zmq_connect(sub, "tcp://proxy:5558");

    while (true) {
        json resposta = login(nome);

        if (resposta["status"] == "ok"){
            break;
        }

        this_thread::sleep_for(chrono::seconds(2));
    }

    int numeroCanal = 0;
    int numeroMensagem = 0;

    while (true) {
        json resposta = listaCanal(nome);

        if (resposta["status"] != "ok") {
            this_thread::sleep_for(chrono::seconds(2));
            continue;
        }

        vector<string> canais = resposta["canais"].get<vector<string>>();

        if (canais.size() < 5) {
            numeroCanal++;

            string canalReq = nome + "_canal_" + to_string(numeroCanal);

            resposta = criaCanal(nome, canalReq);
            if (resposta["status"] == "ok"){
                canais.push_back(canalReq);
            }

            this_thread::sleep_for(chrono::seconds(2));
        }

        vector<string> canaisDisponiveis;

        for (string canal : canais) {
            if (find(inscritos.begin(), inscritos.end(), canal) == inscritos.end()){
                canaisDisponiveis.push_back(canal);
            }
        }

        if (inscritos.size() < 3 && canaisDisponiveis.size() > 0) {
            inscrever(canaisDisponiveis[0]);
            this_thread::sleep_for(chrono::seconds(2));
        }

        for (int i = 0; i < 10; i++) {
            int qntCanais = canais.size();

            if (qntCanais > 0) {
                string canalPublicar = canais[i % qntCanais];
                numeroMensagem++;

                string texto = nome + " enviou mensagem numero " + to_string(numeroMensagem);
                publicar(nome, canalPublicar, texto);
            }

            recebePub();
            this_thread::sleep_for(chrono::seconds(1));
        }
    }
}
