#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    A genetic algorithm implementation
    
 -------
    3966813
    3967180   
    
http://doughellmann.com/2008/05/pymotw-heapq.html    
    
WEIGHT_RATIO = 0.8
N_ENTRIES = 1000    

best: 3951089 [3951089, 3476930, 3476521, 3476625, 3476020]
best: 3951558 [3951558, 3476930, 3476521, 3476625, 3476020]
best: 3958496 [3958496, 3476930, 3476521, 3476625, 3476020]
best: 3958856 [3958856, 3476930, 3476521, 3476625, 3476020]
best: 3959132 [3959132, 3476930, 3476521, 3476625, 3476020]
best: 3959181 [3959181, 3476930, 3476521, 3476625, 3476020]
best: 3959271 [3959271, 3476930, 3476521, 3476625, 3476020]
best: 3959414 [3959414, 3476930, 3476521, 3476625, 3476020]
best: 3960050 [3960050, 3476930, 3476521, 3476625, 3476020]
best: 3960593 [3960593, 3476930, 3476521, 3476625, 3476020]
best: 3960615 [3960615, 3476930, 3476521, 3476625, 3476020]
best: 3962062 [3962062, 3476930, 3476521, 3476625, 3476020]
best: 3963690 [3963690, 3476930, 3476521, 3476625, 3476020]
     
      
--------------------------------------------------------------------------------

WEIGHT_RATIO = 0.9
N_ENTRIES = 2000

best: 3951474 [3951474, 3480800, 3480664, 3480762, 3480607]
best: 3952149 [3952149, 3480800, 3480664, 3480762, 3480607]
best: 3954524 [3954524, 3480800, 3480664, 3480762, 3480607]
best: 3956238 [3956238, 3480800, 3480664, 3480762, 3480607]
best: 3957084 [3957084, 3480800, 3480664, 3480762, 3480607]
best: 3957926 [3957926, 3480800, 3480664, 3480762, 3480607]
best: 3958718 [3958718, 3480800, 3480664, 3480762, 3480607]
best: 3958729 [3958729, 3480800, 3480664, 3480762, 3480607]
best: 3958899 [3958899, 3480800, 3480664, 3480762, 3480607]
best: 3960268 [3960268, 3480800, 3480664, 3480762, 3480607]
best: 3960381 [3960381, 3480800, 3480664, 3480762, 3480607]
best: 3961357 [3961357, 3480800, 3480664, 3480762, 3480607]
best: 3961818 [3961818, 3480800, 3480664, 3480762, 3480607]
best: 3961927 [3961927, 3480800, 3480664, 3480762, 3480607]
best: 3962126 [3962126, 3480800, 3480664, 3480762, 3480607]
best: 3962281 [3962281, 3480800, 3480664, 3480762, 3480607]
best: 3962871 [3962871, 3480800, 3480664, 3480762, 3480607]
 

"""
from __future__ import division
import sys, os, random
import numpy as np

n_elems = 0

WEIGHT_RATIO = 0.9
N_ENTRIES = 2000

WEIGHTS = WEIGHT_RATIO ** (1 + np.arange(N_ENTRIES))
WEIGHTS = np.cumsum(WEIGHTS)
WEIGHTS /= WEIGHTS[-1]      # Cumulative sums up to 1.0

    
def spin_roulette_wheel():
    """ Find the roulette wheel winner
        returns: an index with probability proportional to index's weight
    """
    return np.searchsorted(WEIGHTS, random.random())

    
def spin_roulette_wheel_twice():
    """" Spin the roulette wheel twice and return 2 different values """
    i1 = spin_roulette_wheel()
    while True:    
        i2 = spin_roulette_wheel()
        if i2 != i1:
            return i1, i2


N_REPLACEMENTS = 1
def mutate(elements, complement):
    """Replace N_REPLACEMENTS of the in elements itmes from complement and return it"""
       
    #print 'mutate:', len(elements), len(complement) 
    assert len(elements) + len(complement) == n_elems
    added = random.sample(complement, N_REPLACEMENTS)
    removed = random.sample(elements, N_REPLACEMENTS)
    
    return set(added), set(removed)
    

def crossOver(c1, c2):
    """Swap half the elements in c1 and c2
        c1: a set of ints
        c2: a set of ints
    """

    # Find elements that are not in both lists
    shared = c1 &  c2
    d1 = c1 - shared
    d2 = c2 - shared
   
    move_1_to_2 = random.sample(d1, (len(d1) + 1)// 2)
    move_2_to_1 = random.sample(d2, (len(d2) + 1)// 2)
        
    # Return shared + 1/2 of original + 1/2 swapped
    return set(move_2_to_1), set(move_1_to_2)  

  
def get_state(values_weights, capacity, elements):  
    value = sum(value_weight[i][0] for i in elements)
    weight = sum(value_weight[i][1] for i in elements)
    return value, capacity - weight

    
def update_state(values_weights, value, capacity, added, removed):
    value += sum(values_weights[i][0] for i in added) - sum(values_weights[i][0] for i in removed)
    capacity -= sum(values_weights[i][1] for i in added) - sum(values_weights[i][1] for i in removed)   
    return value, capacity    
    
    
def get_score(value, capacity):
    if capacity >= 0:
        return True, value
    else: 
        return False, value / (1.0 - capacity) / 2.0    
    
class Solution:
    """A solution to the knapsack problem
        capacity: remaining capacity in the knapscack
        value: value of items in the knapscak
        elements: elements included in the knapsack
        complement: elements not included in the knapsacl
    """

    def __init__(self, value, capacity, elements, complement):
        self.capacity = capacity
        self.value = value
        self.elements = elements
        self.complement = complement
        assert len(elements) + len(complement) == n_elems
        assert not self.elements & self.complement
        
    def score(self):
        valid, scor = get_score(self.value, self.capacity)
        if valid:
            assert scor == self.value, 'valid=%s,score=%s,value=%s' % (valid, scor, self.value)
        return scor
        
    def update(self, values_weights, added, removed):
        value, capacity = update_state(values_weights, self.value, self.capacity, added, removed)
        elements = (self.elements | added) - removed
        complement = (self.complement | removed) - added
        assert not self.elements & self.complement
        assert not added & removed
        assert len(elements) + len(complement) == n_elems, '\n%d %s\n%d %s\n%s\n%s\n%s' % (
                len(elements), sorted(elements), 
                len(complement), sorted(complement),
                elements & complement,
                added,
                removed,
               )
        return Solution(value, capacity, elements, complement) 
        
    def top_up(self, values_weights):
        if self.capacity <= 0:
            return
        for i, (v, w) in enumerate(values_weights):
            if i in self.complement and w <= self.capacity:
                self.value += v
                self.capacity -= w
                self.elements.add(i)
                self.complement.remove(i)
                
    def update_top_up(self, values_weights, added, removed):            
        solution = self.update(values_weights, added, removed)
        solution.top_up(values_weights)
        return solution
        
        
def solve_greedy(capacity, value, values_weights, elements, complement):
    """A trivial greedy algorithm for filling the knapsack
        it takes items in-order until the knapsack is full
        capacity: remaining capacity
        value: current value
        values_weights: table of value, weight pairs
        elements: indexes into values_weights of items taken so far
        complement: indexes into values_weights of items not taken so far
        returns: 
            value of knapsack after this solution
            indexes into values_weights of items taken after running this algo
    """    
    taken = elements[:]
    for i in complement:
        v, w = values_weights[i]
        if w > capacity:
            break
        value += v
        capacity -= w
        taken.append(i)

    return value, capacity, taken        
    
    
from itertools import count
from heapq import heappush, heappop, heappushpop    

INVERSE_MUTATION_RATIO = 10    
def solve_ga(capacity, values, weights):
    
    random.seed(111) # The Nelson!
    
    values_weights = zip(values, weights)
    n = len(values_weights)
    
    global n_elems
    n_elems = n
    
    print 'WEIGHT_RATIO',  WEIGHT_RATIO
    print 'N_ENTRIES', N_ENTRIES 
    
    best_value = 0
    
    # Prime the heap with 1000 elements
    elements, complement = [], range(n)
    complem_set = set(complement)
    Q = []
    for i in range(N_ENTRIES * 10):
        value, cap, elts = solve_greedy(capacity, 0, values_weights, elements, complement)
        elts = set(elts)
        #if i % N_ENTRIES == N_ENTRIES - 1:
        #    print (-Q[0][0], value, -value/Q[0][0],  i), 
        assert len(elts) and len(complem_set - elts), '%d %d' % (len(elts), len(complem_set - elts))     
        solution = Solution(value, cap, elts, complem_set - elts)
        assert len(solution.elements) and len(solution.complement), '%d %d' % (len(solution.elements), len(solution.complement)) 
        if len(Q) < N_ENTRIES:
            heappush(Q, (solution.score(), solution))
        else: 
            heappushpop(Q, (solution.score(), solution))
        random.shuffle(complement) 

    best_value = Q[-1][0]
    print
    print 'best:', best_value, [int(Q[-i][1].score()) for i in range(1,6)], Q[0][1].score()        
    print '-' * 80
    
    counter = count()
 
    while True:
        if next(counter) % INVERSE_MUTATION_RATIO == 0:
            i = spin_roulette_wheel()
            added, removed = mutate(Q[i][1].elements, Q[i][1].complement)
            solution = Q[i][1].update_top_up(values_weights, added, removed)
            heappushpop(Q, (solution.score(), solution))    
        else:    
            i1, i2 = spin_roulette_wheel_twice()
            move_2_to_1, move_1_to_2 = crossOver(Q[i1][1].elements, Q[i2][1].elements)
            for i, added, removed in (i1, move_2_to_1, move_1_to_2), (i2, move_1_to_2, move_2_to_1):
                solution = Q[i][1].update_top_up(values_weights, added, removed)
                heappushpop(Q, (solution.score(), solution))    

        if Q[-1][1].score() > best_value:
            best_value = Q[-1][1].score()
            print 'best:', best_value, [int(Q[-i][1].score()) for i in range(1,6)], Q[0][1].score()  
            path = 'best%d.results' % best_value
            with open(path, 'rt') as f:
                f.write('WEIGHT_RATIO=%d\n' %  WEIGHT_RATIO)
                f.write('N_ENTRIES=%d\n' %  N_ENTRIES)
                for i in range(1,6):
                    q = Q[-i][1]
                    els = ', '.join('%d' % j for j in sorted(q.elements))
                    f.write('%d: %s\n' % (q.score(), els)) 
            

