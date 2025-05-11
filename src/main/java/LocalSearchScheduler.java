import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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
 * Implementação da heurística gulosa para o escalonamento de pods em Kubernetes
 * Baseada no algoritmo descrito no artigo que modela o problema como CFLP
 */
class GreedyHeuristic {
    private List<Node> nodes;
    private List<Pod> pods;
    private HashMap<Pod, Node> allocation;
    private TreeSet<Node> openedNodes;

    public GreedyHeuristic(List<Node> nodes, List<Pod> pods) {
        this.nodes = nodes;
        this.pods = pods;
        this.allocation = new HashMap<>();
        this.openedNodes = new TreeSet<>();
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
     * Executa a heurística construtiva gulosa
     * Seguindo o algoritmo descrito no artigo:
     * 1. Ordena os nós por custo de abertura
     * 2. Para cada pod, encontra o nó que minimiza o custo de alocação
     */
    public void execute() {
        // Limpa alocações anteriores
        for (Node node : nodes) {
            node.clear();
        }
        allocation.clear();
        openedNodes.clear();
        
        // Ordena os nós por custo de abertura (crescente)
        List<Node> sortedNodes = new ArrayList<>(nodes);
        sortedNodes.sort(Comparator.comparingDouble(Node::getOpeningCost));
        
        // Para cada pod
        for (Pod pod : pods) {
            double bestCost = Double.MAX_VALUE;
            Node bestNode = null;
            
            // Tenta cada nó
            for (Node node : sortedNodes) {
                // Verifica se o nó tem capacidade para este pod
                if (temCapacidadeSuficiente(node, pod)) {
                    // Calcula o custo
                    double cost = calcularCustoAlocacao(node, pod);
                    
                    // Se o custo for melhor que o atual, atualiza
                    if (cost < bestCost) {
                        bestCost = cost;
                        bestNode = node;
                    }
                }
            }
            
            // Se encontrou um nó viável, aloca o pod a ele
            if (bestNode != null) {
                allocation.put(pod, bestNode);
                openedNodes.add(bestNode);
                bestNode.allocatePod(pod);
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
}

/**
 * Implementação da Busca Local com Melhor Melhoria para o escalonamento de pods
 * Baseada no algoritmo descrito no artigo que modela o problema como CFLP
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
     * 1. Para cada pod, tenta movê-lo para todos os outros nós
     * 2. Seleciona o movimento que resulta na maior redução de custo
     * 3. Repete até que não haja mais melhorias possíveis
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

public class LocalSearchScheduler {
    public static void main(String[] args) throws IOException {
        int[] tamanhosPods = {10, 50, 100, 200, 500, 1000, 5000, 10000};
        int[] tamanhosNodes = {5, 10, 20, 50, 100, 200};
        int numberExecutions = 10;

        FileWriter writerHeuristic = new FileWriter(new File("greedy_heuristic.csv"));
        FileWriter writerLocalSearch = new FileWriter(new File("local_search.csv"));
        
        writerHeuristic.write("number of pods; number of nodes; solution cost; time (ms) \n");
        writerLocalSearch.write("number of pods; number of nodes; solution cost; time (ms) \n");

        // Set the global random seed to 100 for reproducibility
        Random globalRandom = new Random(100);
        // Note: ThreadLocalRandom.current().setSeed() is not supported, 
        // so we'll use our own Random instance instead

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
                Random random = new Random(100);
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

                // ====== Greedy Heuristic ======
                GreedyHeuristic heuristic = new GreedyHeuristic(nodes, pods);

                // Measure execution time for Greedy Heuristic
                long startTimeGreedy = System.currentTimeMillis();
                
                for (int i = 0; i < numberExecutions; i++) {
                    // Clear allocations for each execution to get a fair time measurement
                    for (Node node : nodes) {
                        node.clear();
                    }
                    heuristic.execute();
                }
                
                long endTimeGreedy = System.currentTimeMillis();
                long elapsedTimeGreedy = (endTimeGreedy - startTimeGreedy) / numberExecutions;

                // Calculate solution cost for Greedy Heuristic
                double totalCostGreedy = heuristic.calculateTotalCost();
                int usedNodesGreedy = heuristic.getOpenedNodesCount();

                // Print results for Greedy Heuristic
                System.out.println("=== Greedy Heuristic Results ===");
                System.out.println("Used nodes: " + usedNodesGreedy);
                System.out.println("Solution cost: " + totalCostGreedy);
                System.out.println("Time taken: " + elapsedTimeGreedy + " ms");

                // Write to CSV for Greedy Heuristic
                writerHeuristic.write(numPods + "; " + numNodes + "; " + totalCostGreedy + "; " + elapsedTimeGreedy + "\n");
                writerHeuristic.flush();
                
                // ====== Local Search ======
                // Use the greedy solution as a starting point for the local search
                HashMap<Pod, Node> initialAllocation = new HashMap<>(heuristic.getAllocation());
                TreeSet<Node> initialOpenedNodes = new TreeSet<>(heuristic.getOpenedNodes());
                
                LocalSearch localSearch = new LocalSearch(nodes, pods, initialAllocation, initialOpenedNodes);
                
                // Measure execution time for Local Search
                long startTimeLocalSearch = System.currentTimeMillis();
                
                for (int i = 0; i < numberExecutions; i++) {
                    // Reset to initial greedy solution for each execution
                    localSearch = new LocalSearch(nodes, pods, initialAllocation, initialOpenedNodes);
                    localSearch.execute();
                }
                
                long endTimeLocalSearch = System.currentTimeMillis();
                long elapsedTimeLocalSearch = (endTimeLocalSearch - startTimeLocalSearch) / numberExecutions;
                
                // Calculate solution cost for Local Search
                double totalCostLocalSearch = localSearch.calculateTotalCost();
                int usedNodesLocalSearch = localSearch.getOpenedNodesCount();
                
                // Print results for Local Search
                System.out.println("=== Local Search Results ===");
                System.out.println("Used nodes: " + usedNodesLocalSearch);
                System.out.println("Solution cost: " + totalCostLocalSearch);
                System.out.println("Time taken: " + elapsedTimeLocalSearch + " ms");
                System.out.println("Improvement over Greedy: " + (totalCostGreedy - totalCostLocalSearch) + " (" + 
                        String.format("%.2f", ((totalCostGreedy - totalCostLocalSearch) / totalCostGreedy * 100)) + "%)");
                
                // Write to CSV for Local Search
                writerLocalSearch.write(numPods + "; " + numNodes + "; " + totalCostLocalSearch + "; " + elapsedTimeLocalSearch + "\n");
                writerLocalSearch.flush();
                
                System.out.println("=============================\n");
            }
        }

        writerHeuristic.close();
        writerLocalSearch.close();
        System.out.println("CSV files written successfully");
    }
}