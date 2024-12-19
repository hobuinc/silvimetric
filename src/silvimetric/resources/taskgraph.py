from .metric import Metric
from functools import reduce

class Graph():
    def __init__(self, metrics: list[Metric] | Metric):
        if isinstance(metrics, Metric):
            metrics = [metrics]

        self.metrics = metrics
        self.nodes: dict[str, Node] = { }
        self.results = None
        self.initialized = False

    def init(self):
        """
        Create task dependency list
        """
        for m in self.metrics:
            self.nodes[m.name] = Node(m, self)
        node_vals = list(self.nodes.values())
        for n in node_vals:
            n.init()
        self.initialized = True
        return self

    def run(self, data_in):
        """
        Iterate and run Graph Nodes.
        """
        if not self.initialized:
            self.init()

        m_names = [ m.name for m in self.metrics ]
        res = [n.run(data_in) for k, n in self.nodes.items() if k in m_names]

        def join(x, y):
            return x.join(y, on=['xi','yi'])
        self.results = reduce(join, res)
        return self.results


class Node():
    def __init__(self, metric, graph):
        self.metric = metric
        self.graph = graph
        self.dependencies: set[Node] = ()
        self.results = None
        self.initialized = False

    def init(self):
        """
        Iterate through dependencies. If node is already in graph, reference that.
        If not, create an entry in the graph and then reference it. Nodes
        represent arguments to pass to the Metric method, in the order that
        they're presented in the set.
        """
        nodes = []

        for dep in self.metric.dependencies:
            if dep.name in self.graph.nodes.keys():
                nodes.append(self.graph.nodes[dep.name])
            else:
                depnode = Node(dep, self.graph)
                self.graph.nodes[dep.name] = depnode
                depnode.init()
                nodes.append(depnode)

        self.dependencies = set(nodes)
        self.initialized = True

        return self

    def run(self, data_in):
        """
        Iterate dependency Nodes and run them. If this Node has already been run
        then return the results instead of running again.
        """
        if not self.initialized:
            self.init()

        if self.results is not None:
            return self.results

        args = list(node.run(data_in) for node in self.dependencies)
        self.results = self.metric.do(data_in, *args)
        return self.results
