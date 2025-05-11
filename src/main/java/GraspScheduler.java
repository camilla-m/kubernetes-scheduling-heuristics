import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Random;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;

class Pod {
    private int resourceUsage;
    private int index;

    public Pod(int resourceUsage, int index) {
        this.resourceUsage = resourceUsage;
        this.index = index;
    }

    public int getResourceUsage() {
        return resourceUsage;
    }

    public int getIndex() {
        return index;
    }
}

class Node implements Comparable<Node> {
    private int capacity;
    private List<Pod> pods;
    private int index;
    private double openingCost;
    private double allocationCost;

    public Node(int capacity, int index, double openingCost, double allocationCost) {
        this.capacity = capacity;
        this.pods = new ArrayList<>();
        this.index = index;
        this.openingCost = openingCost;
        this.allocationCost = allocationCost;
    }

    public int getCapacity() {
        return capacity;
    }

    public double getOpeningCost() {
        return openingCost;
    }

    public double getAllocationCost() {
        return allocationCost;
    }

    public boolean canAllocatePod(Pod pod) {
        int totalPodsSize = pods.stream().mapToInt(Pod::getResourceUsage).sum();
        return totalPodsSize + pod.getResourceUsage() <= capacity;
    }

    public void allocatePod(Pod pod) {
        pods.add(pod);
    }

    public List<Pod> getPods() {
        return pods;
    }

    public int getIndex() {
        return index;
    }
  
    public void clear() {
        pods.clear();
    }

    public int compareTo(Node nodeTemp) {
        if (this.index < nodeTemp.index)
            return -1;
        else if (this.index > nodeTemp.index)
            return 1;
        else
            return 0;
    }
}

/**
 * Implementação da heurística gulosa aleatorizada para o GRASP
 * Baseada no algoritmo descrito no artigo que modela o problema como CFLP
 */
class GreedyRandomized {
    private List<Node> nodes;
    private List<Pod> pods;
    private HashMap<Pod, Node> allocation;
    private TreeSet<Node> openedNodes;
    private Random random;
    private double alpha; // Parâmetro de aleatoriedade (0-1)

    public GreedyRandomized(List<Node> nodes, List<Pod> pods, double alpha, Random random) {
        this.nodes = nodes;
        this.pods = pods;
        this.allocation = new HashMap<>();
        this.openedNodes = new TreeSet<>();
        this.alpha = alpha;
        this.random = random;
    }

    /**
     * Verifica se um nó tem capacidade suficiente para acomodar um pod
     */
    private boolean temCapacidadeSuficiente(Node node, Pod pod) {
        int usedCapacity = 0;
        
        // Calcula a capacidade já utilizada pelos pods alocados a este nó
        for (Pod allocatedPod : node.getPods()) {
            usedCapacity += allocatedPod.getResourceUsage();
        }
        
        // Verifica se há espaço suficiente para o novo pod
        return (usedCapacity + pod.getResourceUsage() <= node.getCapacity());
    }
    
    /**
     * Calcula o custo de alocar um pod a um nó
     */
    private double calcularCustoAlocacao(Node node, Pod pod) {
        double cost = 0.0;
        
        // Se o nó não estiver aberto, adiciona o custo de abertura
        if (!openedNodes.contains(node)) {
            cost += node.getOpeningCost();
        }
        
        // Adiciona o custo de alocação
        cost += node.getAllocationCost();
        
        return cost;
    }
    
    /**
     * Executa a heurística construtiva gulosa aleatorizada
     * 1. Para cada pod, cria uma lista restrita de candidatos (RCL)
     * 2. Seleciona aleatoriamente um nó da RCL
     * 3. Atualiza a solução
     */
    public void execute() {
        // Limpa alocações anteriores
        for (Node node : nodes) {
            node.clear();
        }
        allocation.clear();
        openedNodes.clear();
        
        // Para cada pod
        for (Pod pod : pods) {
            // Criar a lista restrita de candidatos (RCL)
            List<NodeCost> candidatos = new ArrayList<>();
            
            // Avalia todos os nós
            for (Node node : nodes) {
                // Verifica se o nó tem capacidade para este pod
                if (temCapacidadeSuficiente(node, pod)) {
                    // Calcula o custo
                    double cost = calcularCustoAlocacao(node, pod);
                    candidatos.add(new NodeCost(node, cost));
                }
            }
            
            // Se não encontrou nenhum nó viável, pula este pod
            if (candidatos.isEmpty()) {
                continue;
            }
            
            // Ordena os candidatos por custo
            candidatos.sort(Comparator.comparingDouble(NodeCost::getCost));
            
            // Determina o tamanho da RCL usando o parâmetro alpha
            double minCost = candidatos.get(0).getCost();
            double maxCost = candidatos.get(candidatos.size() - 1).getCost();
            double threshold = minCost + alpha * (maxCost - minCost);
            
            // Cria a RCL com os nós que têm custo menor ou igual ao threshold
            List<Node> rcl = new ArrayList<>();
            for (NodeCost nc : candidatos) {
                if (nc.getCost() <= threshold) {
                    rcl.add(nc.getNode());
                }
            }
            
            // Seleciona aleatoriamente um nó da RCL
            if (!rcl.isEmpty()) {
                int randomIndex = random.nextInt(rcl.size());
                Node selectedNode = rcl.get(randomIndex);
                
                // Aloca o pod ao nó selecionado
                allocation.put(pod, selectedNode);
                openedNodes.add(selectedNode);
                selectedNode.allocatePod(pod);
            }
        }
    }
    
    /**
     * Calcula o custo total da solução
     */
    public double calculateTotalCost() {
        double totalCost = 0.0;
        
        // Soma os custos de abertura para todos os nós abertos
        for (Node node : openedNodes) {
            totalCost += node.getOpeningCost();
        }
        
        // Soma os custos de alocação
        for (Pod pod : pods) {
            Node node = allocation.get(pod);
            if (node != null) {
                totalCost += node.getAllocationCost();
            }
        }
        
        return totalCost;
    }
    
    /**
     * Retorna o número de nós abertos na solução
     */
    public int getOpenedNodesCount() {
        return openedNodes.size();
    }
    
    /**
     * Retorna todos os nós abertos na solução
     */
    public TreeSet<Node> getOpenedNodes() {
        return openedNodes;
    }
    
    /**
     * Retorna o mapeamento de alocação (pod -> nó)
     */
    public HashMap<Pod, Node> getAllocation() {
        return allocation;
    }
    
    /**
     * Classe auxiliar para armazenar um nó e seu custo associado
     */
    private class NodeCost {
        private Node node;
        private double cost;
        
        public NodeCost(Node node, double cost) {
            this.node = node;
            this.cost = cost;
        }
        
        public Node getNode() {
            return node;
        }
        
        public double getCost() {
            return cost;
        }
    }
}

/**
 * Implementação da Busca Local com Melhor Melhoria para o GRASP
 */
class LocalSearch {
    private List<Node> nodes;
    private List<Pod> pods;
    private HashMap<Pod, Node> allocation;
    private TreeSet<Node> openedNodes;
    
    public LocalSearch(List<Node> nodes, List<Pod> pods, HashMap<Pod, Node> initialAllocation, TreeSet<Node> initialOpenedNodes) {
        this.nodes = nodes;
        this.pods = pods;
        this.allocation = new HashMap<>(initialAllocation);
        this.openedNodes = new TreeSet<>(initialOpenedNodes);
        
        // Aplicar a alocação inicial aos nós
        for (Node node : nodes) {
            node.clear();
        }
        
        for (Pod pod : pods) {
            Node node = allocation.get(pod);
            if (node != null) {
                node.allocatePod(pod);
            }
        }
    }
    
    /**
     * Verifica se um nó tem capacidade suficiente para acomodar um pod
     * considerando uma possível mudança na alocação atual
     */
    private boolean temCapacidadeSuficiente(Node node, Pod pod, Node currentNode) {
        // Se o nó atual é o mesmo para onde queremos mover, não precisa verificar
        if (node.equals(currentNode)) {
            return true;
        }
        
        int usedCapacity = 0;
        
        // Calcula a capacidade já utilizada pelos pods alocados a este nó
        for (Pod allocatedPod : node.getPods()) {
            // Não contar o pod que estamos tentando mover (caso ele já esteja neste nó)
            if (!allocatedPod.equals(pod)) {
                usedCapacity += allocatedPod.getResourceUsage();
            }
        }
        
        // Verifica se há espaço suficiente para o novo pod
        return (usedCapacity + pod.getResourceUsage() <= node.getCapacity());
    }
    
    /**
     * Calcula a variação de custo ao mover um pod de um nó para outro
     */
    private double calcularVariacaoCusto(Pod pod, Node currentNode, Node newNode) {
        double deltaCost = 0.0;
        
        // Remover custo da alocação atual
        deltaCost -= currentNode.getAllocationCost();
        
        // Adicionar custo da nova alocação
        deltaCost += newNode.getAllocationCost();
        
        // Se o nó atual ficará vazio, remover seu custo de abertura
        boolean currentNodeWillBeEmpty = currentNode.getPods().size() == 1 && currentNode.getPods().contains(pod);
        if (currentNodeWillBeEmpty) {
            deltaCost -= currentNode.getOpeningCost();
        }
        
        // Se o novo nó não está aberto, adicionar seu custo de abertura
        if (!openedNodes.contains(newNode)) {
            deltaCost += newNode.getOpeningCost();
        }
        
        return deltaCost;
    }
    
    /**
     * Executa a busca local com melhor melhoria
     */
    public void execute() {
        boolean melhorou = true;
        
        while (melhorou) {
            melhorou = false;
            double melhorDeltaCusto = 0;
            Pod melhorPod = null;
            Node melhorNoDestino = null;
            Node noAtual = null;
            
            // Para cada pod
            for (Pod pod : pods) {
                Node currentNode = allocation.get(pod);
                
                // Se o pod não está alocado, continua
                if (currentNode == null) {
                    continue;
                }
                
                // Tenta mover para cada outro nó
                for (Node newNode : nodes) {
                    // Não testar o mesmo nó
                    if (newNode.equals(currentNode)) {
                        continue;
                    }
                    
                    // Verifica se o novo nó tem capacidade
                    if (temCapacidadeSuficiente(newNode, pod, currentNode)) {
                        // Calcula a variação de custo
                        double deltaCost = calcularVariacaoCusto(pod, currentNode, newNode);
                        
                        // Se encontrou uma melhoria melhor
                        if (deltaCost < melhorDeltaCusto) {
                            melhorDeltaCusto = deltaCost;
                            melhorPod = pod;
                            melhorNoDestino = newNode;
                            noAtual = currentNode;
                        }
                    }
                }
            }
            
            // Se encontrou uma melhoria, aplica a mudança
            if (melhorDeltaCusto < 0 && melhorPod != null && melhorNoDestino != null) {
                melhorou = true;
                
                // Remove o pod do nó atual
                noAtual.getPods().remove(melhorPod);
                
                // Verifica se o nó atual ficou vazio
                if (noAtual.getPods().isEmpty()) {
                    openedNodes.remove(noAtual);
                }
                
                // Adiciona o pod ao novo nó
                melhorNoDestino.allocatePod(melhorPod);
                openedNodes.add(melhorNoDestino);
                
                // Atualiza a alocação
                allocation.put(melhorPod, melhorNoDestino);
            }
        }
    }
    
    /**
     * Calcula o custo total da solução após a busca local
     */
    public double calculateTotalCost() {
        double totalCost = 0.0;
        
        // Soma os custos de abertura para todos os nós abertos
        for (Node node : openedNodes) {
            totalCost += node.getOpeningCost();
        }
        
        // Soma os custos de alocação
        for (Pod pod : pods) {
            Node node = allocation.get(pod);
            if (node != null) {
                totalCost += node.getAllocationCost();
            }
        }
        
        return totalCost;
    }
    
    /**
     * Retorna o número de nós abertos na solução
     */
    public int getOpenedNodesCount() {
        return openedNodes.size();
    }
    
    /**
     * Retorna todos os nós abertos na solução
     */
    public TreeSet<Node> getOpenedNodes() {
        return openedNodes;
    }
    
    /**
     * Retorna o mapeamento de alocação (pod -> nó)
     */
    public HashMap<Pod, Node> getAllocation() {
        return allocation;
    }
}

/**
 * Implementação do GRASP (Greedy Randomized Adaptive Search Procedure)
 * para o problema de escalonamento de contêineres em Kubernetes
 */
class Grasp {
    private List<Node> nodes;
    private List<Pod> pods;
    private HashMap<Pod, Node> bestAllocation;
    private TreeSet<Node> bestOpenedNodes;
    private double bestCost;
    private Random random;
    private double alpha; // Parâmetro de aleatoriedade (0-1)
    private int maxIterations; // Número máximo de iterações
    
    public Grasp(List<Node> nodes, List<Pod> pods, double alpha, int maxIterations, long seed) {
        this.nodes = nodes;
        this.pods = pods;
        this.bestAllocation = new HashMap<>();
        this.bestOpenedNodes = new TreeSet<>();
        this.bestCost = Double.MAX_VALUE;
        this.random = new Random(seed);
        this.alpha = alpha;
        this.maxIterations = maxIterations;
    }
    
    /**
     * Executa o algoritmo GRASP
     * 1. Fase construtiva: gera uma solução inicial usando a heurística gulosa aleatorizada
     * 2. Fase de busca local: melhora a solução usando a busca local
     * 3. Atualiza a melhor solução encontrada
     * 4. Repete por um número predefinido de iterações
     */
    public void execute() {
        for (int iter = 0; iter < maxIterations; iter++) {
            // Fase construtiva
            GreedyRandomized greedyRandomized = new GreedyRandomized(nodes, pods, alpha, random);
            greedyRandomized.execute();
            
            // Fase de busca local
            LocalSearch localSearch = new LocalSearch(
                nodes, 
                pods, 
                greedyRandomized.getAllocation(), 
                greedyRandomized.getOpenedNodes()
            );
            localSearch.execute();
            
            // Calcula o custo da solução atual
            double currentCost = localSearch.calculateTotalCost();
            
            // Atualiza a melhor solução se necessário
            if (currentCost < bestCost) {
                bestCost = currentCost;
                bestAllocation = new HashMap<>(localSearch.getAllocation());
                bestOpenedNodes = new TreeSet<>(localSearch.getOpenedNodes());
            }
        }
    }
    
    /**
     * Retorna o custo da melhor solução encontrada
     */
    public double getBestCost() {
        return bestCost;
    }
    
    /**
     * Retorna o número de nós abertos na melhor solução
     */
    public int getBestOpenedNodesCount() {
        return bestOpenedNodes.size();
    }
    
    /**
     * Retorna todos os nós abertos na melhor solução
     */
    public TreeSet<Node> getBestOpenedNodes() {
        return bestOpenedNodes;
    }
    
    /**
     * Retorna o mapeamento de alocação da melhor solução
     */
    public HashMap<Pod, Node> getBestAllocation() {
        return bestAllocation;
    }
}

public class GraspScheduler {
    public static void main(String[] args) throws IOException {
        int[] tamanhosPods = {5000, 10000};
        int[] tamanhosNodes = {5, 10, 20, 50, 100, 200};
        int numberExecutions = 10;
        
        // Parâmetros do GRASP
        double alpha = 0.3; // Fator de aleatoriedade (0-1)
        int maxIterations = 10; // Número máximo de iterações
        long seed = 100; // Semente para reprodutibilidade

        FileWriter writerGrasp = new FileWriter(new File("grasp.csv"));
        writerGrasp.write("number of pods; number of nodes; solution cost; time (ms) \n");

        // Set the global random seed to 100 for reproducibility
        Random globalRandom = new Random(seed);

        for (int numPods : tamanhosPods) {
            for (int numNodes : tamanhosNodes) {
                // The only valid configuration when we consider 5 nodes is the one with 10 pods.
                if (numNodes == 5 && numPods != 10)
                    continue;

                System.out.println("Total pods: " + numPods + " and Total Nodes: " + numNodes);

                // Create the data structures and generate random data.
                List<Node> nodes = new ArrayList<>(numNodes);
                List<Pod> pods = new ArrayList<>(numPods);

                int capacityMin = numPods / numNodes + 1; // Specify the minimum node capacity
                int capacityMax = numPods * 2; // Specify the maximum node capacity

                int resourceUsageMin = 1; // Specify the minimum pod size
                int resourceUsageMax = 10; // Specify the maximum pod size

                int openingCostInit = 1; // Specify the minimum cost per unit opening node 
                int openingCostEnd = 4 * numNodes; // Specify the maximum cost per unit opening node 

                int allocatingCostInit = 1; // Specify the minimum cost per unit allocation cost 
                int allocatingCostEnd = 4 * numNodes; // Specify the maximum cost per unit allocation cost 

                // Create nodes using random data with fixed seed for reproducibility
                Random random = new Random(seed);
                for (int i = 0; i < numNodes; i++) {
                    int capacity = random.nextInt(capacityMax - capacityMin + 1) + capacityMin;
                    double openingCost = random.nextInt(openingCostEnd - openingCostInit + 1) + openingCostInit;
                    double allocationCost = random.nextInt(allocatingCostEnd - allocatingCostInit + 1) + allocatingCostInit;
                    
                    Node node = new Node(capacity, i, openingCost, allocationCost);
                    nodes.add(node);
                }

                // Create pods using random data with fixed seed for reproducibility
                for (int j = 0; j < numPods; j++) {
                    int resourceUsage = random.nextInt(resourceUsageMax - resourceUsageMin + 1) + resourceUsageMin;
                    Pod pod = new Pod(resourceUsage, j);
                    pods.add(pod);
                }

                // Create the GRASP algorithm
                Grasp grasp = new Grasp(nodes, pods, alpha, maxIterations, seed);

                // Measure execution time
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < numberExecutions; i++) {
                    // Execute GRASP
                    grasp = new Grasp(nodes, pods, alpha, maxIterations, seed);
                    grasp.execute();
                }
                
                long endTime = System.currentTimeMillis();
                long elapsedTime = (endTime - startTime) / numberExecutions;

                // Get solution statistics
                double totalCost = grasp.getBestCost();
                int usedNodes = grasp.getBestOpenedNodesCount();

                // Print results
                System.out.println("=== GRASP Results ===");
                System.out.println("Used nodes: " + usedNodes);
                System.out.println("Solution cost: " + totalCost);
                System.out.println("Time taken: " + elapsedTime + " ms");
                System.out.println("Alpha parameter: " + alpha);
                System.out.println("Max iterations: " + maxIterations);
                System.out.println("======================\n");

                // Write to CSV
                writerGrasp.write(numPods + "; " + numNodes + "; " + totalCost + "; " + elapsedTime + "\n");
                writerGrasp.flush();
            }
        }

        writerGrasp.close();
        System.out.println("CSV file written successfully");
    }
}