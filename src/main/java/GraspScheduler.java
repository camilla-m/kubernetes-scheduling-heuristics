import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;

class Pod {
    private int resourceUsage;
    private int index;
    private Node allocatedNode;

    public Pod(int resourceUsage, int index) {
        this.resourceUsage = resourceUsage;
        this.index = index;
        this.allocatedNode = null;
    }

    public int getResourceUsage() {
        return resourceUsage;
    }

    public int getIndex() {
        return index;
    }
    
    public Node getAllocatedNode() {
        return allocatedNode;
    }
    
    public void setAllocatedNode(Node node) {
        this.allocatedNode = node;
    }
    
    public boolean isAllocated() {
        return allocatedNode != null;
    }
}

class Node implements Comparable<Node> {
    private int capacity;
    private List<Pod> pods;
    private int index;
    private double openingCost;
    private double allocationCost;
    private int currentUsage;

    public Node(int capacity, int index, double openingCost, double allocationCost) {
        this.capacity = capacity;
        this.pods = new ArrayList<>();
        this.index = index;
        this.openingCost = openingCost;
        this.allocationCost = allocationCost;
        this.currentUsage = 0;
    }

    public int getCapacity() {
        return capacity;
    }
    
    public int getCurrentUsage() {
        return currentUsage;
    }

    public double getOpeningCost() {
        return openingCost;
    }

    public double getAllocationCost() {
        return allocationCost;
    }

    public boolean canAllocatePod(Pod pod) {
        return currentUsage + pod.getResourceUsage() <= capacity;
    }

    public void allocatePod(Pod pod) {
        pods.add(pod);
        currentUsage += pod.getResourceUsage();
        // Se o pod já estiver alocado em outro nó, remove-o de lá primeiro
        if (pod.getAllocatedNode() != null && pod.getAllocatedNode() != this) {
            pod.getAllocatedNode().removePod(pod);
        }
        pod.setAllocatedNode(this);
    }
    
    public void removePod(Pod pod) {
        if (pods.remove(pod)) {
            currentUsage -= pod.getResourceUsage();
            if (pod.getAllocatedNode() == this) {
                pod.setAllocatedNode(null);
            }
        }
    }

    public List<Pod> getPods() {
        return pods;
    }

    public int getIndex() {
        return index;
    }
  
    public void clear() {
        for (Pod pod : new ArrayList<>(pods)) { // Cria uma cópia para evitar ConcurrentModificationException
            pod.setAllocatedNode(null);
        }
        pods.clear();
        currentUsage = 0;
    }

    public int compareTo(Node nodeTemp) {
        return Integer.compare(this.index, nodeTemp.index);
    }
}

class GreedyRandomized {
    private List<Node> nodes;
    private List<Pod> pods;
    private boolean[] nodeOpened;
    private Random random;
    private double alpha;

    public GreedyRandomized(List<Node> nodes, List<Pod> pods, double alpha, Random random) {
        this.nodes = nodes;
        this.pods = pods;
        this.nodeOpened = new boolean[nodes.size()];
        this.alpha = alpha;
        this.random = random;
    }

    private double calcularCustoAlocacao(Node node, Pod pod) {
        double cost = 0.0;
        
        if (!nodeOpened[node.getIndex()]) {
            cost += node.getOpeningCost();
        }
        
        cost += node.getAllocationCost();
        
        return cost;
    }
    
    public void execute() {
        // Reset state
        for (Node node : nodes) {
            node.clear();
        }
        for (int i = 0; i < nodeOpened.length; i++) {
            nodeOpened[i] = false;
        }
        
        for (Pod pod : pods) {
            List<NodeCost> candidatos = new ArrayList<>();
            
            for (Node node : nodes) {
                if (node.canAllocatePod(pod)) {
                    double cost = calcularCustoAlocacao(node, pod);
                    candidatos.add(new NodeCost(node, cost));
                }
            }
            
            if (candidatos.isEmpty()) {
                continue;
            }
            
            candidatos.sort(Comparator.comparingDouble(NodeCost::getCost));
            
            double minCost = candidatos.get(0).getCost();
            double maxCost = candidatos.get(candidatos.size() - 1).getCost();
            double threshold = minCost + alpha * (maxCost - minCost);
            
            List<Node> rcl = new ArrayList<>();
            for (NodeCost nc : candidatos) {
                if (nc.getCost() <= threshold) {
                    rcl.add(nc.getNode());
                }
            }
            
            if (!rcl.isEmpty()) {
                int randomIndex = random.nextInt(rcl.size());
                Node selectedNode = rcl.get(randomIndex);
                
                selectedNode.allocatePod(pod);
                nodeOpened[selectedNode.getIndex()] = true;
            }
        }
    }
    
    public double calculateTotalCost() {
        double totalCost = 0.0;
        
        for (int i = 0; i < nodeOpened.length; i++) {
            if (nodeOpened[i]) {
                totalCost += nodes.get(i).getOpeningCost();
                
                for (Pod pod : nodes.get(i).getPods()) {
                    totalCost += nodes.get(i).getAllocationCost();
                }
            }
        }
        
        return totalCost;
    }
    
    public int getOpenedNodesCount() {
        int count = 0;
        for (boolean isOpen : nodeOpened) {
            if (isOpen) count++;
        }
        return count;
    }
    
    public boolean[] getNodeOpenedArray() {
        return nodeOpened;
    }
    
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

class LocalSearch {
    private List<Node> nodes;
    private List<Pod> pods;
    private boolean[] nodeOpened;
    
    public LocalSearch(List<Node> nodes, List<Pod> pods, boolean[] initialNodeOpened) {
        this.nodes = nodes;
        this.pods = pods;
        this.nodeOpened = new boolean[nodes.size()];
        
        System.arraycopy(initialNodeOpened, 0, this.nodeOpened, 0, initialNodeOpened.length);
    }
    
    private boolean temCapacidadeSuficiente(Node node, Pod pod, Node currentNode) {
        if (node.equals(currentNode)) {
            return true;
        }
        
        int usedCapacity = node.getCurrentUsage();
        
        return (usedCapacity + pod.getResourceUsage() <= node.getCapacity());
    }
    
    private double calcularVariacaoCusto(Pod pod, Node currentNode, Node newNode) {
        double deltaCost = 0.0;
        
        // Remover custo da alocação atual
        deltaCost -= currentNode.getAllocationCost();
        
        // Adicionar custo da nova alocação
        deltaCost += newNode.getAllocationCost();
        
        // Se o nó atual ficará vazio, remover seu custo de abertura
        boolean currentNodeWillBeEmpty = currentNode.getPods().size() == 1;
        if (currentNodeWillBeEmpty) {
            deltaCost -= currentNode.getOpeningCost();
        }
        
        // Se o novo nó não está aberto, adicionar seu custo de abertura
        if (!nodeOpened[newNode.getIndex()]) {
            deltaCost += newNode.getOpeningCost();
        }
        
        return deltaCost;
    }
    
    public void execute() {
        boolean melhorou = true;
        
        while (melhorou) {
            melhorou = false;
            double melhorDeltaCusto = 0;
            Pod melhorPod = null;
            Node melhorNoDestino = null;
            
            for (Pod pod : pods) {
                Node currentNode = pod.getAllocatedNode();
                
                if (currentNode == null) {
                    continue;
                }
                
                for (Node newNode : nodes) {
                    if (newNode.equals(currentNode)) {
                        continue;
                    }
                    
                    if (temCapacidadeSuficiente(newNode, pod, currentNode)) {
                        double deltaCost = calcularVariacaoCusto(pod, currentNode, newNode);
                        
                        if (deltaCost < melhorDeltaCusto) {
                            melhorDeltaCusto = deltaCost;
                            melhorPod = pod;
                            melhorNoDestino = newNode;
                        }
                    }
                }
            }
            
            if (melhorDeltaCusto < 0 && melhorPod != null && melhorNoDestino != null) {
                melhorou = true;
                
                Node noAtual = melhorPod.getAllocatedNode();
                
                // Remove o pod do nó atual
                noAtual.removePod(melhorPod);
                
                // Verifica se o nó atual ficou vazio
                if (noAtual.getPods().isEmpty()) {
                    nodeOpened[noAtual.getIndex()] = false;
                }
                
                // Adiciona o pod ao novo nó
                melhorNoDestino.allocatePod(melhorPod);
                nodeOpened[melhorNoDestino.getIndex()] = true;
            }
        }
    }
    
    public double calculateTotalCost() {
        double totalCost = 0.0;
        
        for (int i = 0; i < nodeOpened.length; i++) {
            if (nodeOpened[i]) {
                totalCost += nodes.get(i).getOpeningCost();
                
                for (Pod pod : nodes.get(i).getPods()) {
                    totalCost += nodes.get(i).getAllocationCost();
                }
            }
        }
        
        return totalCost;
    }
    
    public int getOpenedNodesCount() {
        int count = 0;
        for (boolean isOpen : nodeOpened) {
            if (isOpen) count++;
        }
        return count;
    }
    
    public boolean[] getNodeOpenedArray() {
        return nodeOpened;
    }
}

class Grasp {
    private List<Node> nodes;
    private List<Pod> pods;
    private boolean[] bestNodeOpened;
    private double bestCost;
    private Random random;
    private double alpha;
    private int maxIterations;
    
    public Grasp(List<Node> nodes, List<Pod> pods, double alpha, int maxIterations, long seed) {
        this.nodes = nodes;
        this.pods = pods;
        this.bestNodeOpened = new boolean[nodes.size()];
        this.bestCost = Double.MAX_VALUE;
        this.random = new Random(seed);
        this.alpha = alpha;
        this.maxIterations = maxIterations;
    }
    
    public void execute() {
        for (int iter = 0; iter < maxIterations; iter++) {
            // Fase construtiva
            GreedyRandomized greedyRandomized = new GreedyRandomized(nodes, pods, alpha, random);
            greedyRandomized.execute();
            
            // Fase de busca local
            LocalSearch localSearch = new LocalSearch(
                nodes, 
                pods, 
                greedyRandomized.getNodeOpenedArray()
            );
            localSearch.execute();
            
            // Calcula o custo da solução atual
            double currentCost = localSearch.calculateTotalCost();
            
            // Atualiza a melhor solução se necessário
            if (currentCost < bestCost) {
                bestCost = currentCost;
                System.arraycopy(localSearch.getNodeOpenedArray(), 0, bestNodeOpened, 0, bestNodeOpened.length);
            }
        }
        
        // Após encontrar a melhor solução, aplicamos esta solução aos nós e pods
        applyBestSolution();
    }
    
    private void applyBestSolution() {
        // Primeiro, limpa todas as alocações atuais
        for (Node node : nodes) {
            node.clear();
        }
        
        // Agora, para cada pod, encontramos o nó com menor custo dentre os abertos
        for (Pod pod : pods) {
            Node bestNode = null;
            double bestAllocationCost = Double.MAX_VALUE;
            
            for (int i = 0; i < bestNodeOpened.length; i++) {
                if (bestNodeOpened[i]) {
                    Node node = nodes.get(i);
                    if (node.canAllocatePod(pod) && node.getAllocationCost() < bestAllocationCost) {
                        bestNode = node;
                        bestAllocationCost = node.getAllocationCost();
                    }
                }
            }
            
            if (bestNode != null) {
                bestNode.allocatePod(pod);
            }
        }
    }
    
    public double getBestCost() {
        return bestCost;
    }
    
    public int getBestOpenedNodesCount() {
        int count = 0;
        for (boolean isOpen : bestNodeOpened) {
            if (isOpen) count++;
        }
        return count;
    }
}

public class GraspScheduler {
    public static void main(String[] args) throws IOException {

        int[] tamanhosPods = {10, 50, 100, 200, 500, 1000, 5000, 10000};
        int[] tamanhosNodes = {5, 10, 20, 50, 100, 200};
        int numberExecutions = 10;
        
        // Parâmetros do GRASP
        double alpha = 0.3; // Fator de aleatoriedade (0-1)
        int maxIterations = 10; // Número máximo de iterações
        long seed = 100; // Semente para reprodutibilidade

        FileWriter writerGrasp = new FileWriter(new File("grasp_optimized.csv"));
        writerGrasp.write("number of pods; number of nodes; solution cost; time (ms) \n");

        // Set the global random seed for reproducibility
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

                int capacityMin = numPods / numNodes + 1; 
                int capacityMax = numPods * 2;

                int resourceUsageMin = 1; 
                int resourceUsageMax = 10;

                int openingCostInit = 1; 
                int openingCostEnd = 4 * numNodes;

                int allocatingCostInit = 1;
                int allocatingCostEnd = 4 * numNodes;

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

                // Measure execution time
                long startTime = System.currentTimeMillis();
                
                double totalCostSum = 0;
                int usedNodesSum = 0;
                
                for (int i = 0; i < numberExecutions; i++) {
                    // Create a new GRASP instance for each execution
                    Grasp grasp = new Grasp(nodes, pods, alpha, maxIterations, seed + i);
                    grasp.execute();
                    
                    totalCostSum += grasp.getBestCost();
                    usedNodesSum += grasp.getBestOpenedNodesCount();
                }
                
                long endTime = System.currentTimeMillis();
                long elapsedTime = (endTime - startTime) / numberExecutions;

                // Calculate averages
                double totalCost = totalCostSum / numberExecutions;
                int usedNodes = usedNodesSum / numberExecutions;

                // Print results
                System.out.println("=== GRASP Results (Optimized) ===");
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