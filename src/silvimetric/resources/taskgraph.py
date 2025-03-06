from .metric import Metric
from .filter import Filter
from functools import reduce

NodeType =  Metric | Filter
NodeTypeList = list[NodeType]

class Graph():
    def __init__(self, tasks: NodeTypeList | NodeType):
        """
        Task graph for Metrics.
        """

        # TODO: add mutex to Graph so it can be run in parallel
        if isinstance(tasks, NodeType):
            tasks = [tasks]

        self.tasks = tasks
        self.nodes: dict[str, Node] = { }
        self.results = None
        self.initialized = False

    def init(self):
        """
        Create task dependency list
        """
        for m in self.tasks:
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

        t_names = [ t.name for t in self.tasks ]
        res = [n.run(data_in) for k, n in self.nodes.items() if k in t_names]

        self.results = res[0].join(res[1:])
        return self.results


class Node():
    # TODO add mutex to Node so it can be run in parallel?
    def __init__(self, task: Filter | Metric, graph: Graph):
        self.task = task
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
        pre_runs = self.task.dependencies
        if isinstance(self.task, Metric):
            pre_runs = pre_runs + self.task.filters

        for dep in pre_runs:
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

        deps = { node.task.name: node.run(data_in) for node in self.dependencies }

        self.results = self.task.do(data_in, **deps)
        return self.results
