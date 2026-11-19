import org.zeromq.ZMQ;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.core.MessagePack;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Cliente {
    static ObjectMapper msgpack = new ObjectMapper(new MessagePackFactory());
    static ObjectMapper json = new ObjectMapper();

    static String dataHora() {
        return Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replace("Z", "+00:00");
    }

    static Map<String, Object> map(Object... valores) {
        Map<String, Object> mensagem = new LinkedHashMap<>();
        for (int i = 0; i < valores.length; i += 2) mensagem.put((String) valores[i], valores[i + 1]);
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

    static String nome = System.getenv().getOrDefault("nome_bot", "bot_java1");
    static int relogio = 0;

    static ZMQ.Context contexto = ZMQ.context(1);
    static ZMQ.Socket req = contexto.socket(ZMQ.REQ);
    static ZMQ.Socket sub = contexto.socket(ZMQ.SUB);

    static List<String> inscritos = new ArrayList<>();

    static Map<String, Object> requisitar(Map<String, Object> mensagem) throws Exception {
        relogio++;
        mensagem.put("relogio", relogio);

        System.out.println(nome + " enviou: " + mensagem);
        enviaMsg(req, mensagem);

        Map<String, Object> resposta = recebeMsg(req);
        System.out.println(nome + " recebeu: " + resposta);

        return resposta;
    }

    static Map<String, Object> login(String usuario) throws Exception {
        Map<String, Object> mensagem = map("metodo", "login", "dataHora", dataHora(), "usuario", usuario);
        return requisitar(mensagem);
    }

    static Map<String, Object> listaCanal(String usuario) throws Exception {
        Map<String, Object> mensagem = map("metodo", "listaCanal", "dataHora", dataHora());
        return requisitar(mensagem);
    }

    static Map<String, Object> criaCanal(String usuario, String nomeCanal) throws Exception {
        Map<String, Object> mensagem = map("metodo", "criaCanal", "dataHora", dataHora(), "nomeCanal", nomeCanal, "usuario", usuario);
        return requisitar(mensagem);
    }

    static Map<String, Object> publicar(String usuario, String nomeCanal, String texto) throws Exception {
        Map<String, Object> mensagem = map("metodo", "publicar", "dataHora", dataHora(), "nomeCanal", nomeCanal, "usuario", usuario, "mensagem", texto);
        return requisitar(mensagem);
    }

    static void inscrever(String nomeCanal) {
        if (!inscritos.contains(nomeCanal)) {
            sub.subscribe((nomeCanal + "|").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            inscritos.add(nomeCanal);
            System.out.println(nome + " inscrito em " + nomeCanal);
        }
    }

    static void recebePub() throws Exception {
        ZMQ.Poller poller = contexto.poller(1);
        poller.register(sub, ZMQ.Poller.POLLIN);

        while (poller.poll(0) > 0) {
            sub.recv();
            Map<String, Object> publicacao = recebeMsg(sub);

            relogio = Math.max(relogio, ((Number) publicacao.getOrDefault("relogio", 0)).intValue()) + 1;

            System.out.print(nome + " recebeu: canal=" + publicacao.get("nomeCanal"));
            System.out.print(" mensagem=" + publicacao.get("mensagem"));
            System.out.print(" enviado=" + publicacao.get("dataHora"));
            System.out.println(" recebido=" + dataHora() + " relogio=" + relogio);
        }
    }

    public static void main(String[] args) throws Exception {
        req.connect("tcp://broker:5555");
        sub.connect("tcp://proxy:5558");

        while (true) {
            Map<String, Object> resposta = login(nome);
            if (resposta.get("status").equals("ok")){
                break;
            } 
            Thread.sleep(2000);
        }

        int numeroCanal = 0;
        int numeroMensagem = 0;

        while (true) {
            Map<String, Object> resposta = listaCanal(nome);

            if (!resposta.get("status").equals("ok")) {
                Thread.sleep(2000);
                continue;
            }

            List<String> canais = new ArrayList<>((List<String>) resposta.get("canais"));

            if (canais.size() < 5) {
                numeroCanal++;
                String canalReq = nome + "_canal_" + numeroCanal;

                resposta = criaCanal(nome, canalReq);
                if (resposta.get("status").equals("ok")){
                    canais.add(canalReq);
                } 

                Thread.sleep(2000);
            }

            List<String> canaisDisponiveis = new ArrayList<>();
            for (String canal : canais) {
                if (!inscritos.contains(canal)){
                    canaisDisponiveis.add(canal);
                }
            }

            if (inscritos.size() < 3 && canaisDisponiveis.size() > 0) {
                inscrever(canaisDisponiveis.get(0));
                Thread.sleep(2000);
            }

            for (int i = 0; i < 10; i++) {
                int qntCanais = canais.size();

                if (qntCanais > 0) {
                    String canalPublicar = canais.get(i % qntCanais);
                    numeroMensagem++;
                    String texto = nome + " enviou mensagem numero " + numeroMensagem;
                    publicar(nome, canalPublicar, texto);
                }

                recebePub();
                Thread.sleep(1000);
            }
        }
    }
}
