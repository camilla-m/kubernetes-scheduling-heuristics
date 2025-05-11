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

public class GreedyHeuristicScheduler {
    public static void main(String[] args) throws IOException {
        int[] tamanhosPods = {10, 50, 100, 200, 500, 1000, 5000, 10000};
        int[] tamanhosNodes = {5, 10, 20, 50, 100, 200};
        int numberExecutions = 10;
        long seed = 100; // Fixed seed for reproducibility

        FileWriter writerHeuristic = new FileWriter(new File("greedy_heuristic.csv"));
        writerHeuristic.write("number of pods; number of nodes; solution cost; time (ms) \n");

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

                // Create the greedy heuristic
                GreedyHeuristic heuristic = new GreedyHeuristic(nodes, pods);

                // Measure execution time
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < numberExecutions; i++) {
                    // Clear allocations for each execution to get a fair time measurement
                    for (Node node : nodes) {
                        node.clear();
                    }
                    heuristic.execute();
                }
                
                long endTime = System.currentTimeMillis();
                long elapsedTime = (endTime - startTime) / numberExecutions;

                // Calculate solution cost
                double totalCost = heuristic.calculateTotalCost();
                int usedNodes = heuristic.getOpenedNodesCount();

                // Print results
                System.out.println("Used nodes: " + usedNodes);
                System.out.println("Solution cost: " + totalCost);
                System.out.println("Time taken: " + elapsedTime + " ms");
                System.out.println("==========================\n");

                // Write to CSV
                writerHeuristic.write(numPods + "; " + numNodes + "; " + totalCost + "; " + elapsedTime + "\n");
                writerHeuristic.flush();
            }
        }

        writerHeuristic.close();
        System.out.println("CSV file written successfully");
    }
}