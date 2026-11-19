
#include <zmq.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <ctime>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <random>

using namespace std;
using json = nlohmann::json;

int relogio = 0;
double ajusteHora = 0;

int rankServidor = 0;
string coordenador = "";
int rankCoordenador = 0;

int contadorMensagens = 0;

double tempoAtual() {
    return chrono::duration<double>(chrono::system_clock::now().time_since_epoch()).count();
}

string dataHora() {
    time_t agora = (time_t)(tempoAtual() + ajusteHora);
    char texto[40];
    strftime(texto, sizeof(texto), "%Y-%m-%dT%H:%M:%S+00:00", gmtime(&agora));
    return texto;
}

double horaAtual() {
    return tempoAtual() + ajusteHora;
}

void enviaMsg(void* socket, json mensagem, int flags = 0) {
    vector<uint8_t> bytes = json::to_msgpack(mensagem);
    zmq_send(socket, bytes.data(), bytes.size(), flags);
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

string nome = "servidor_cpp1";
string arquivoDados = "/dados/estado.json";

void* contexto = zmq_ctx_new();
void* rep = zmq_socket(contexto, ZMQ_REP);
void* pub = zmq_socket(contexto, ZMQ_PUB);
void* socketRef = zmq_socket(contexto, ZMQ_REQ);
void* peer = zmq_socket(contexto, ZMQ_REP);
void* sub = zmq_socket(contexto, ZMQ_SUB);

json dados;

string novoId() {
    static random_device gerador;
    string caracteres = "0123456789abcdef";
    string id = "";

    for (int i = 0; i < 32; i++){
        id += caracteres[gerador() % 16];
    }

    return id;
}

json carregar_dados() {
    ifstream arquivo(arquivoDados);

    if (arquivo.is_open()) {
        json conteudo;
        arquivo >> conteudo;
        if (!conteudo.contains("publicacoes")){
            conteudo["publicacoes"] = json::array();
        }

        return conteudo;
    }

    return {{"logins", json::array()}, {"canais", json::array()}, {"publicacoes", json::array()}};
}

void publicarEstado() {
    relogio++;

    json mensagem = {{"servidor", nome}, {"dados", dados}, {"relogio", relogio}};

    string topico = "replica|";
    zmq_send(pub, topico.data(), topico.size(), ZMQ_SNDMORE);
    enviaMsg(pub, mensagem);
}

void salvar_dados(json dadosSalvar, bool replicar = true);

void mesclarEstado(json dadosRecebidos) {
    bool alterou = false;

    for (string tipo : {"logins", "canais", "publicacoes"}) {
        json registros = dadosRecebidos.value(tipo, json::array());
        vector<string> existentes;

        for (json registro : dados[tipo]) {
            string chave;

            if (tipo == "canais"){
                chave = registro["nome"].get<string>();
            }
            else if (registro.contains("id")){
                chave = registro["id"].get<string>();
            }
            else{
                chave = registro.dump();
            }

            existentes.push_back(chave);
        }

        for (json registro : registros) {
            string chave;

            if (tipo == "canais"){
                chave = registro["nome"].get<string>();
            }
            else if (registro.contains("id")){
                chave = registro["id"].get<string>();
            }
            else{
                chave = registro.dump();
            }

            if (find(existentes.begin(), existentes.end(), chave) == existentes.end()) {
                dados[tipo].push_back(registro);
                existentes.push_back(chave);
                alterou = true;
            }
        }
    }

    if (alterou) {
        salvar_dados(dados, false);
        cout << nome << " recebeu dados" << endl;
    }
}

void salvar_dados(json dadosSalvar, bool replicar) {
    ofstream arquivo(arquivoDados);
    arquivo << dadosSalvar.dump(2);
    arquivo.close();

    if (replicar){
        publicarEstado();
    }
}


json referencia(string metodo) {
    relogio++;
    json mensagem = {{"metodo", metodo}, {"nome", nome}, {"dataHora", dataHora()}, {"relogio", relogio}};

    enviaMsg(socketRef, mensagem);
    json resposta = recebeMsg(socketRef);

    relogio = max(relogio, resposta.value("relogio", 0)) + 1;

    cout << nome << " referencia recebeu: " << resposta.dump() << endl;

    return resposta;
}

json requisitarServ(string nomeServidor, json mensagem) {
    void* newReq = zmq_socket(contexto, ZMQ_REQ);

    int linger = 0;
    zmq_setsockopt(newReq, ZMQ_LINGER, &linger, sizeof(linger));

    string endereco = "tcp://" + nomeServidor + ":5560";
    zmq_connect(newReq, endereco.c_str());

    relogio++;
    mensagem["relogio"] = relogio;
    mensagem["dataHora"] = dataHora();

    enviaMsg(newReq, mensagem);

    json resposta = nullptr;
    zmq_pollitem_t item = {newReq, 0, ZMQ_POLLIN, 0};

    if (zmq_poll(&item, 1, 1500) > 0 && (item.revents & ZMQ_POLLIN)) {
        resposta = recebeMsg(newReq);
        relogio = max(relogio, resposta.value("relogio", 0)) + 1;
    }

    zmq_close(newReq);
    return resposta;
}

void anunciarCoordenador() {
    coordenador = nome;
    rankCoordenador = rankServidor;

    relogio++;

    json mensagem = {{"metodo", "coordenador"}, {"coordenador", nome}, {"rank", rankServidor}, {"dataHora", dataHora()}, {"relogio", relogio}};

    string topico = "servers|";
    zmq_send(pub, topico.data(), topico.size(), ZMQ_SNDMORE);
    enviaMsg(pub, mensagem);

    cout << nome << " coordenador: rank " << rankServidor << endl;
}

void eleicao() {
    json resposta = referencia("listaServidores");

    if (resposta["status"] != "ok"){
        return;
    }

    vector<json> servidores;

    for (json servidor : resposta["servidores"]){
        servidores.push_back(servidor);
    }

    sort(servidores.begin(), servidores.end(), [](json a, json b) { return a["rank"].get<int>() > b["rank"].get<int>();});

    for (json servidor : servidores) {
        if (servidor["rank"].get<int>() > rankServidor) {
            json mensagem = {{"metodo", "eleicao"}, {"nome", nome}, {"rank", rankServidor}};

            json respostaEleicao = requisitarServ(servidor["nome"].get<string>(), mensagem);

            if (!respostaEleicao.is_null() && respostaEleicao["status"] == "ok") {
                coordenador = servidor["nome"].get<string>();
                rankCoordenador = servidor["rank"].get<int>();

                cout << coordenador << " coordenador de " << nome << endl;
                return;
            }
        }
    }

    if (coordenador != nome){
        anunciarCoordenador();
    }
}

void receberAnuncio() {
    zmq_pollitem_t item = {sub, 0, ZMQ_POLLIN, 0};

    while (zmq_poll(&item, 1, 0) > 0 && (item.revents & ZMQ_POLLIN)) {
        zmq_msg_t quadro;
        zmq_msg_init(&quadro);
        zmq_msg_recv(&quadro, sub, 0);

        string topico((char*)zmq_msg_data(&quadro), zmq_msg_size(&quadro));

        zmq_msg_close(&quadro);

        json mensagem = recebeMsg(sub);

        relogio = max(relogio, mensagem.value("relogio", 0)) + 1;

        if (topico == "replica|") {
            if (mensagem["servidor"] != nome){
                mesclarEstado(mensagem["dados"]);
            }
        }

        else if (topico == "servers|") {
            int novoRank = mensagem["rank"];

            if (novoRank >= rankCoordenador) {
                coordenador = mensagem["coordenador"].get<string>();
                rankCoordenador = novoRank;

                cout << nome << " recebeu coordenador: " << coordenador << endl;
            }
        }

        item.revents = 0;
    }
}

void sincronizar() {
    if (coordenador.empty()){
        eleicao();
    }

    if (coordenador == nome){
        return;
    }

    double inicio = tempoAtual();

    json mensagem = {{"metodo", "sincronizar"}, {"nome", nome}, {"hora", horaAtual()}};

    json resposta = requisitarServ(coordenador, mensagem);
    double fim = tempoAtual();

    if (resposta.is_null()) {
        eleicao();
        return;
    }

    if (resposta["status"] != "ok") {
        eleicao();
        return;
    }

    double horaLocal = (inicio + fim) / 2;
    ajusteHora = resposta["horaCorreta"].get<double>() - horaLocal;

    cout << nome << " sincronizou com " << coordenador;
    cout     << " Ajuste=" << ajusteHora;
    cout     << " Hora=" << dataHora() << endl;
}

void atenderServidor() {
    json requisicao = recebeMsg(peer);

    relogio = max(relogio, requisicao.value("relogio", 0)) + 1;

    string metodo = requisicao["metodo"];
    json resposta = {{"metodo", metodo}, {"status", "ok"}};

    bool anunciar = false;

    if (metodo == "eleicao") {
        if (rankServidor > requisicao.value("rank", 0)) {
            resposta["mensagem"] = "OK";

            if (coordenador != nome){
                anunciar = true;
            }
        }

        else {
            resposta["status"] = "erro";
            resposta["mensagem"] = "rank menor";
        }
    }
    else if (metodo == "sincronizar") {
        if (coordenador != nome) {
            resposta["status"] = "erro";
            resposta["mensagem"] = "servidor nao eh coordenador";
        }

        else {
            double horaCoordenador = horaAtual();
            double horaServidor = requisicao["hora"];

            double horaCorreta = (horaCoordenador + horaServidor) / 2;

            ajusteHora += horaCorreta - horaCoordenador;
            resposta["horaCorreta"] = horaCorreta;

            cout << nome << ": hora local=" << horaCoordenador;
            cout << " hora recebida=" << horaServidor;
            cout << " media=" << horaCorreta << endl;
        }
    }

    else {
        resposta["status"] = "erro";
        resposta["mensagem"] = "metodo desconhecido";
    }

    relogio++;
    resposta["relogio"] = relogio;
    resposta["dataHora"] = dataHora();

    enviaMsg(peer, resposta);

    if (anunciar){
        anunciarCoordenador();
    }
}


json recebeRequisicao(json requisicao, json& dados) {
    string metodo = requisicao["metodo"];
    json resposta = {{"metodo", metodo}, {"status", "ok"}};

    if (metodo == "login") {
        string usuario = requisicao["usuario"];

        dados["logins"].push_back({{"id", novoId()}, {"usuario", usuario}, {"dataHora", requisicao["dataHora"]}});

        salvar_dados(dados);
        resposta["mensagem"] = "Login";
    }

    else if (metodo == "criaCanal") {
        string nomeCanal = requisicao["nomeCanal"];

        for (json canal : dados["canais"]) {
            if (nomeCanal == canal["nome"])
                return {{"metodo", metodo}, {"status", "erro"}, {"mensagem", "Canal existente"}, {"dataHora", dataHora()}};
        }

        dados["canais"].push_back({{"id", novoId()}, {"nome", nomeCanal}, {"usuario", requisicao["usuario"]}, {"dataHora", requisicao["dataHora"]}});

        salvar_dados(dados);
        resposta["mensagem"] = "Canal criado";
    }

    else if (metodo == "listaCanal") {
        json canais = json::array();

        for (json canal : dados["canais"]){
            canais.push_back(canal["nome"]);
        }

        resposta["canais"] = canais;
    }

    else if (metodo == "publicar") {
        string nomeCanal = requisicao["nomeCanal"];
        string mensagem = requisicao["mensagem"];
        string usuario = requisicao["usuario"];

        bool canalExistente = false;

        for (json canal : dados["canais"]) {
            if (nomeCanal == canal["nome"]) {
                canalExistente = true;
                break;
            }
        }

        if (!canalExistente) {
            resposta["status"] = "erro";
            resposta["mensagem"] = "o canal nao existe";
        }
        
        else {
            relogio++;

            json publicacao = {{"id", novoId()}, {"nomeCanal", nomeCanal}, {"usuario", usuario}, {"mensagem", mensagem}, {"dataHora", dataHora()}, {"relogio", relogio}};

            dados["publicacoes"].push_back(publicacao);
            salvar_dados(dados);

            cout << nome << " publicou: " << publicacao.dump() << endl;

            string topico = nomeCanal + "|";
            zmq_send(pub, topico.data(), topico.size(), ZMQ_SNDMORE);
            enviaMsg(pub, publicacao);

            resposta["mensagem"] = "Publicacao feita";
        }
    }

    else {
        resposta["status"] = "erro";
        resposta["mensagem"] = "Acao desconhecida";
    }

    return resposta;
}

int main() {
    if (getenv("nome_servidor")){
        nome = getenv("nome_servidor");
    }

    if (getenv("dados")){
        arquivoDados = getenv("dados");
    }

    dados = carregar_dados();

    zmq_connect(rep, "tcp://broker:5556");
    zmq_connect(pub, "tcp://proxy:5557");
    zmq_connect(socketRef, "tcp://referencia:5559");
    zmq_bind(peer, "tcp://*:5560");
    zmq_connect(sub, "tcp://proxy:5558");

    string topicoServers = "servers|";
    string topicoReplica = "replica|";

    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, topicoServers.data(), topicoServers.size());

    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, topicoReplica.data(), topicoReplica.size());

    json resposta = referencia("rank");

    if (resposta["status"] == "ok") {
        rankServidor = resposta["rank"];
        cout << nome << ": rank " << rankServidor << endl;
    }

    eleicao();

    zmq_pollitem_t itens[] = {{rep, 0, ZMQ_POLLIN, 0}, {peer, 0, ZMQ_POLLIN, 0}, {sub, 0, ZMQ_POLLIN, 0}};

    cout << nome << " iniciado" << endl;

    referencia("listaServidores");

    auto ultimaReplicacao = chrono::steady_clock::now() - chrono::seconds(2);

    while (true) {
        zmq_poll(itens, 3, 1000);

        auto agora = chrono::steady_clock::now();

        if (agora - ultimaReplicacao >= chrono::seconds(2)) {
            publicarEstado();
            ultimaReplicacao = agora;
        }

        if (itens[1].revents & ZMQ_POLLIN){
            atenderServidor();
        }

        if (itens[2].revents & ZMQ_POLLIN){
            receberAnuncio();
        }

        if (itens[0].revents & ZMQ_POLLIN) {
            json requisicao = recebeMsg(rep);
            contadorMensagens++;

            relogio = max(relogio, requisicao.value("relogio", 0)) + 1;

            cout << nome << " recebeu: " << requisicao.dump() << endl;

            json resposta = recebeRequisicao(requisicao, dados);

            relogio++;
            resposta["relogio"] = relogio;
            resposta["dataHora"] = dataHora();

            cout << nome << " enviou: " << resposta.dump() << endl;
            enviaMsg(rep, resposta);

            if (contadorMensagens % 10 == 0) {
                json respostaRef = referencia("heartbeat");

                if (respostaRef["status"] != "ok") {
                    respostaRef = referencia("rank");

                    if (respostaRef["status"] == "ok") {
                        rankServidor = respostaRef["rank"];
                        cout << "novo rank de " << nome << ": " << rankServidor << endl;
                    }
                }

                eleicao();
            }

            if (contadorMensagens % 15 == 0)
                sincronizar();
        }
    }
}
