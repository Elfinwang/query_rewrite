
import math

class MCTS_node():
    def __init__(self, ra, sql, cost, parent, rule, value):
        self.ra = ra
        self.sql = sql
        self.cost = cost
        self.value = value
        self.visited = 0
        self.dead = False
        self.parents = [parent]
        self.rules = [rule]
        self.children = []

    # UCB = lg(value+1) + gamma*...
    def ucb(self, gamma):
        parents_visited = 0
        for parent in self.parents:
            parents_visited += parent.visited
        return math.log(max(self.value, 0)+1, 10) + gamma * math.sqrt(math.log(1 + parents_visited) / (1 + self.visited))

    # find a node to expand
    def find_leaf(self, gamma, trace):
        if self.visited == 0:
            return self
        children_use = []
        children_alive = []
        for child in self.children:
            if not child.dead:
                children_alive.append(child)
                if not child in trace:
                    children_use.append(child)
        if len(children_alive) == 0:
            self.dead = True
        self.children = children_alive
        if len(children_use) == 0:
            return None
        
        children_sorted = sorted(children_use, key=lambda k : k.ucb(gamma), reverse=True)
        for child in children_sorted:
            trace.append(child)
            ans = child.find_leaf(gamma, trace)
            if not ans is None:
                return ans
            else:
                trace.pop()
        self.dead = True
        return None
        
    # back propagate to all the ancestors (maybe several parents)
    def back_prop(self, needq=False):
        v = set()
        q = [(self, -1, -1)]
        fp = 0
        rp = 1
        while fp < rp:
            curr = q[fp][0]
            for i, parent in enumerate(curr.parents):
                if parent is None:
                    continue
                if not parent in v:
                    v.add(parent)
                    if needq or self.value > parent.value:
                        parent.value = self.value
                        q.append((parent, fp, curr.rules[i]))
                        rp += 1
            fp += 1
        return q


