
import org.zeromq.ZMQ;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.msgpack.core.MessagePack;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import java.time.Instant;
import java.util.*;
import java.io.File;

public class Servidor {
    static ObjectMapper msgpack = new ObjectMapper(new MessagePackFactory());
    static ObjectMapper json = new ObjectMapper();
    static ObjectMapper jsonOrdenado = new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    static int relogio = 0;
    static double ajusteHora = 0;

    static int rank = 0;
    static String coordenador = null;
    static int rankCoordenador = 0;

    static int contadorMensagens = 0;

    static double tempoAtual() {
        return System.currentTimeMillis() / 1000.0;
    }

    static String dataHora() {
        return Instant.ofEpochSecond((long) horaAtual()).toString().replace("Z", "+00:00");
    }

    static double horaAtual() {
        return tempoAtual() + ajusteHora;
    }

    static Map<String, Object> map(Object... valores) {
        Map<String, Object> mensagem = new LinkedHashMap<>();
        for (int i = 0; i < valores.length; i += 2){
            mensagem.put((String) valores[i], valores[i + 1]);
        } 
        return mensagem;
    }

    static void enviaMsg(ZMQ.Socket socket, Map<String, Object> mensagem) throws Exception {
        socket.send(msgpack.writeValueAsBytes(mensagem));
    }

    static Map<String, Object> recebeMsg(ZMQ.Socket socket) throws Exception {
        byte[] bytes = socket.recv();
        String texto = MessagePack.newDefaultUnpacker(bytes).unpackValue().toJson();
        return json.readValue(texto, Map.class);
    }

    static String nome = System.getenv().getOrDefault("nome_servidor", "servidor_java1");
    static String arquivoDados = System.getenv().getOrDefault("dados", "/dados/estado.json");

    static ZMQ.Context contexto = ZMQ.context(1);
    static ZMQ.Socket rep = contexto.socket(ZMQ.REP);
    static ZMQ.Socket pub = contexto.socket(ZMQ.PUB);
    static ZMQ.Socket socketRef = contexto.socket(ZMQ.REQ);
    static ZMQ.Socket peer = contexto.socket(ZMQ.REP);
    static ZMQ.Socket sub = contexto.socket(ZMQ.SUB);

    static Map<String, Object> dados;

    static String novoId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    static Map<String, Object> carregar_dados() throws Exception {
        Map<String, Object> conteudo;

        if (new File(arquivoDados).exists()){
            conteudo = json.readValue(new File(arquivoDados), Map.class);
        }else{
            conteudo = map("logins", new ArrayList<>(), "canais", new ArrayList<>());
        }
           
        if (!conteudo.containsKey("publicacoes")){
            conteudo.put("publicacoes", new ArrayList<>());
        }
        return conteudo;
    }

    static void publicarEstado() throws Exception {
        relogio++;
        Map<String, Object> mensagem = map("servidor", nome, "dados", dados, "relogio", relogio);

        pub.sendMore("replica|");
        enviaMsg(pub, mensagem);
    }

    static void mesclarEstado(Map<String, Object> dadosRecebidos) throws Exception {
        boolean alterou = false;

        for (String tipo : List.of("logins", "canais", "publicacoes")) {
            List<Map<String, Object>> registros = (List<Map<String, Object>>) dadosRecebidos.getOrDefault(tipo, new ArrayList<>());
            List<Map<String, Object>> locais = (List<Map<String, Object>>) dados.get(tipo);
            Set<String> existentes = new HashSet<>();

            for (Map<String, Object> registro : locais) {
                String chave;
                if(tipo.equals("canais")){
                    chave = (String) registro.get("nome");
                }else if(registro.get("id") != null){
                     chave = (String) registro.get("id");
                }else{
                    chave = jsonOrdenado.writeValueAsString(registro);
                }  
                existentes.add(chave);
            }

            for (Map<String, Object> registro : registros) {
                String chave;
                if (tipo.equals("canais")){
                    chave = (String) registro.get("nome");
                }else if (registro.get("id") != null){
                    chave = (String) registro.get("id");
                }else{
                    chave = jsonOrdenado.writeValueAsString(registro);
                } 
                 
                if (!existentes.contains(chave)) {
                    locais.add(registro);
                    existentes.add(chave);
                    alterou = true;
                }
            }
        }

        if (alterou) {
            salvar_dados(dados, false);
            System.out.println(nome + " recebeu dados");
        }
    }

    static void salvar_dados(Map<String, Object> dadosSalvar) throws Exception {
        salvar_dados(dadosSalvar, true);
    }

    static void salvar_dados(Map<String, Object> dadosSalvar, boolean replicar) throws Exception {
        json.writerWithDefaultPrettyPrinter().writeValue(new File(arquivoDados), dadosSalvar);
        if (replicar) publicarEstado();
    }

    static Map<String, Object> referencia(String metodo) throws Exception {
        relogio++;
        Map<String, Object> mensagem = map("metodo", metodo, "nome", nome, "dataHora", dataHora(), "relogio", relogio);

        enviaMsg(socketRef, mensagem);
        Map<String, Object> resposta = recebeMsg(socketRef);

        relogio = Math.max(relogio, ((Number) resposta.getOrDefault("relogio", 0)).intValue()) + 1;
        System.out.println(nome + " referencia recebeu: " + resposta);
        return resposta;
    }

    static Map<String, Object> requisitarServ(String nomeServidor, Map<String, Object> mensagem) throws Exception {
        ZMQ.Socket newReq = contexto.socket(ZMQ.REQ);
        newReq.setLinger(0);
        newReq.setReceiveTimeOut(1500);
        newReq.connect("tcp://" + nomeServidor + ":5560");

        try {
            relogio++;
            mensagem.put("relogio", relogio);
            mensagem.put("dataHora", dataHora());
            enviaMsg(newReq, mensagem);

            byte[] pacote = newReq.recv();
            if (pacote == null) return null;

            Map<String, Object> resposta = json.readValue(MessagePack.newDefaultUnpacker(pacote).unpackValue().toJson(), Map.class);
            relogio = Math.max(relogio, ((Number) resposta.getOrDefault("relogio", 0)).intValue()) + 1;
            return resposta;
        }
        finally {
            newReq.close();
        }
    }

    static void anunciarCoordenador() throws Exception {
        coordenador = nome;
        rankCoordenador = rank;
        relogio++;

        Map<String, Object> mensagem = map("metodo", "coordenador", "coordenador", nome, "rank", rank, "dataHora", dataHora(), "relogio", relogio);
        pub.sendMore("servers|");
        enviaMsg(pub, mensagem);

        System.out.println(nome + " coordenador: rank " + rank);
    }

    static void eleicao() throws Exception {
        Map<String, Object> resposta = referencia("listaServidores");
        if (!resposta.get("status").equals("ok")) return;

        List<Map<String, Object>> servidores = (List<Map<String, Object>>) resposta.get("servidores");
        servidores.sort((a, b) -> Integer.compare(((Number) b.get("rank")).intValue(), ((Number) a.get("rank")).intValue()));

        for (Map<String, Object> servidor : servidores) {
            if (((Number) servidor.get("rank")).intValue() > rank) {
                Map<String, Object> mensagem = map("metodo", "eleicao", "nome", nome, "rank", rank);
                Map<String, Object> respostaEleicao = requisitarServ((String) servidor.get("nome"), mensagem);

                if (respostaEleicao != null && respostaEleicao.get("status").equals("ok")) {
                    coordenador = (String) servidor.get("nome");
                    rankCoordenador = ((Number) servidor.get("rank")).intValue();
                    System.out.println(coordenador + " coordenador de " + nome);
                    return;
                }
            }
        }

        if (!nome.equals(coordenador)){
            anunciarCoordenador();
        } 
    }

    static void receberAnuncio() throws Exception {
        while (sub.hasReceiveMore() || sub.getEvents() != 0) {
            String topico = new String(sub.recv(), java.nio.charset.StandardCharsets.UTF_8);
            Map<String, Object> mensagem = recebeMsg(sub);

            relogio = Math.max(relogio, ((Number) mensagem.getOrDefault("relogio", 0)).intValue()) + 1;

            if (topico.equals("replica|")) {
                if (!mensagem.get("servidor").equals(nome)) {
                    mesclarEstado((Map<String, Object>) mensagem.get("dados"));
                }
            }else if (topico.equals("servers|")){
                int novoRank = ((Number) mensagem.get("rank")).intValue();

                if (novoRank >= rankCoordenador) {
                    coordenador = (String) mensagem.get("coordenador");
                    rankCoordenador = novoRank;
                    System.out.println(nome + " recebeu coordenador: " + coordenador);
                }
            }

            break;
        }
    }

    static void sincronizar() throws Exception {
        if (coordenador == null){
            eleicao();
        } 
        if (nome.equals(coordenador)){
            return;
        } 

        double inicio = tempoAtual();
        Map<String, Object> mensagem = map("metodo", "sincronizar", "nome", nome, "hora", horaAtual());

        Map<String, Object> resposta = requisitarServ(coordenador, mensagem);
        double fim = tempoAtual();

        if (resposta == null) {
            eleicao();
            return;
        }

        if (!resposta.get("status").equals("ok")) {
            eleicao();
            return;
        }

        double horaLocal = (inicio + fim) / 2;
        ajusteHora = ((Number) resposta.get("horaCorreta")).doubleValue() - horaLocal;

        System.out.println(nome + " sincronizou com " + coordenador + " Ajuste=" + ajusteHora + " Hora=" + dataHora());
    }

    static void atenderServidor() throws Exception {
        Map<String, Object> requisicao = recebeMsg(peer);
        relogio = Math.max(relogio, ((Number) requisicao.getOrDefault("relogio", 0)).intValue()) + 1;

        String metodo = (String) requisicao.get("metodo");
        Map<String, Object> resposta = map("metodo", metodo, "status", "ok");
        boolean anunciar = false;

        if (metodo.equals("eleicao")) {
            if (rank > ((Number) requisicao.getOrDefault("rank", 0)).intValue()) {
                resposta.put("mensagem", "OK");
                if (!nome.equals(coordenador)){
                    anunciar = true;
                } 
            }
            else {
                resposta.put("status", "erro");
                resposta.put("mensagem", "rank menor");
            }
        }
        else if (metodo.equals("sincronizar")) {
            if (!nome.equals(coordenador)) {
                resposta.put("status", "erro");
                resposta.put("mensagem", "servidor nao eh coordenador");
            }else {
                double horaCoordenador = horaAtual();
                double horaServidor = ((Number) requisicao.get("hora")).doubleValue();
                double horaCorreta = (horaCoordenador + horaServidor) / 2;

                ajusteHora += horaCorreta - horaCoordenador;
                resposta.put("horaCorreta", horaCorreta);

                System.out.printf("%s: hora local=%.3f hora recebida=%.3f media=%.3f%n", nome, horaCoordenador, horaServidor, horaCorreta);
            }
        }
        else {
            resposta.put("status", "erro");
            resposta.put("mensagem", "metodo desconhecido");
        }

        relogio++;
        resposta.put("relogio", relogio);
        resposta.put("dataHora", dataHora());
        enviaMsg(peer, resposta);

        if (anunciar){
            anunciarCoordenador();
        } 
    }

    static Map<String, Object> recebeRequisicao(Map<String, Object> requisicao, Map<String, Object> dados) throws Exception {
        String metodo = (String) requisicao.get("metodo");
        Map<String, Object> resposta = map("metodo", metodo, "status", "ok");

        if (metodo.equals("login")) {
            String usuario = (String) requisicao.get("usuario");

            ((List<Object>) dados.get("logins")).add(map("id", novoId(), "usuario", usuario, "dataHora", requisicao.get("dataHora")));
            salvar_dados(dados);
            resposta.put("mensagem", "Login");
        }

        else if (metodo.equals("criaCanal")) {
            String nomeCanal = (String) requisicao.get("nomeCanal");

            for (Map<String, Object> canal : (List<Map<String, Object>>) dados.get("canais")) {
                if (nomeCanal.equals(canal.get("nome"))){
                    return map("metodo", metodo, "status", "erro", "mensagem", "Canal existente", "dataHora", dataHora());
                }
            }

            ((List<Object>) dados.get("canais")).add(map("id", novoId(), "nome", nomeCanal, "usuario", requisicao.get("usuario"), "dataHora", requisicao.get("dataHora")));
            salvar_dados(dados);
            resposta.put("mensagem", "Canal criado");
        }

        else if (metodo.equals("listaCanal")) {
            List<String> canais = new ArrayList<>();

            for (Map<String, Object> canal : (List<Map<String, Object>>) dados.get("canais")){
                canais.add((String) canal.get("nome"));
            }

            resposta.put("canais", canais);
        }

        else if (metodo.equals("publicar")) {
            String nomeCanal = (String) requisicao.get("nomeCanal");
            String mensagem = (String) requisicao.get("mensagem");
            String usuario = (String) requisicao.get("usuario");

            boolean canalExistente = false;
            for (Map<String, Object> canal : (List<Map<String, Object>>) dados.get("canais")) {
                if (nomeCanal.equals(canal.get("nome"))) {
                    canalExistente = true;
                    break;
                }
            }

            if (!canalExistente) {
                resposta.put("status", "erro");
                resposta.put("mensagem", "o canal nao existe");
            }
            else {
                relogio++;

                Map<String, Object> publicacao = map("id", novoId(), "nomeCanal", nomeCanal, "usuario", usuario, "mensagem", mensagem, "dataHora", dataHora(), "relogio", relogio);
                ((List<Object>) dados.get("publicacoes")).add(publicacao);
                salvar_dados(dados);

                System.out.println(nome + " publicou: " + publicacao);

                pub.sendMore(nomeCanal + "|");
                enviaMsg(pub, publicacao);

                resposta.put("mensagem", "Publicacao feita");
            }
        }

        else {
            resposta.put("status", "erro");
            resposta.put("mensagem", "Acao desconhecida");
        }

        return resposta;
    }

    public static void main(String[] args) throws Exception {
        dados = carregar_dados();

        rep.connect("tcp://broker:5556");
        pub.connect("tcp://proxy:5557");
        socketRef.connect("tcp://referencia:5559");
        peer.bind("tcp://*:5560");
        sub.connect("tcp://proxy:5558");
        sub.subscribe("servers|".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        sub.subscribe("replica|".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Map<String, Object> resposta = referencia("rank");

        if (resposta.get("status").equals("ok")) {
            rank = ((Number) resposta.get("rank")).intValue();
            System.out.println(nome + ": rank " + rank);
        }

        eleicao();

        ZMQ.Poller poller = contexto.poller(3);
        poller.register(rep, ZMQ.Poller.POLLIN);
        poller.register(peer, ZMQ.Poller.POLLIN);
        poller.register(sub, ZMQ.Poller.POLLIN);

        System.out.println(nome + " iniciado");
        referencia("listaServidores");

        long ultimaReplicacao = 0;

        while (true) {
            poller.poll(1000);

            if (System.currentTimeMillis() - ultimaReplicacao >= 2000) {
                publicarEstado();
                ultimaReplicacao = System.currentTimeMillis();
            }

            if (poller.pollin(1)){
                atenderServidor();
            } 
            if (poller.pollin(2)){
                receberAnuncio();
            } 

            if (poller.pollin(0)) {
                byte[] pacote = rep.recv();
                contadorMensagens++;

                try {
                    Map<String, Object> requisicao = json.readValue(MessagePack.newDefaultUnpacker(pacote).unpackValue().toJson(), Map.class);
                    relogio = Math.max(relogio, ((Number) requisicao.getOrDefault("relogio", 0)).intValue()) + 1;

                    System.out.println(nome + " recebeu: " + requisicao);
                    resposta = recebeRequisicao(requisicao, dados);
                }catch (Exception exc) {
                    resposta = map("metodo", "desconhecido", "status", "erro", "mensagem", exc.toString(), "dataHora", dataHora());
                }

                relogio++;
                resposta.put("relogio", relogio);
                resposta.put("dataHora", dataHora());

                System.out.println(nome + " enviou: " + resposta);
                enviaMsg(rep, resposta);

                if (contadorMensagens % 10 == 0) {
                    resposta = referencia("heartbeat");

                    if (!resposta.get("status").equals("ok")) {
                        resposta = referencia("rank");

                        if (resposta.get("status").equals("ok")) {
                            rank = ((Number) resposta.get("rank")).intValue();
                            System.out.println("novo rank de " + nome + ": " + rank);
                        }
                    }

                    eleicao();
                }

                if (contadorMensagens % 15 == 0) sincronizar();
            }
        }
    }
}
