#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    A genetic algorithm implementation
    
    -------
    3966813
    3967180 
   *3966200 <= 104  
    3965897    105
    3967036    108

"""
from __future__ import division
import sys, os, random
import numpy as np
import math

VERSION_NUMBER = 109
WEIGHT_RATIO = 0.96
N_ENTRIES = 1000
N_PRIME_ENTRIES = 10000
INVERSE_MUTATION_RATIO = 10
N_REPLACEMENTS = 4  
SORTED = True

n_elems = 0

# 10^-9 == weight_ratio ^ n_entries
# n_entries = log(10^-9, weight_ratio)

if False:
    for wr in (0.9, 0.95, 0.99):
        n = -int(math.log(10**9, wr))
        print wr, n
    exit()  

WEIGHTS = WEIGHT_RATIO ** (1 + np.arange(N_ENTRIES))
WEIGHTS = np.cumsum(WEIGHTS)
WEIGHTS /= WEIGHTS[-1]      # Cumulative sums [0.0 1.0]

roulette_counts = np.zeros(N_ENTRIES, dtype=np.int)

def spin_roulette_wheel():
    """ Find the roulette wheel winner
        returns: an index i = [0..N_ENTRIES) with probability proportional to WEIGHT_RATIO^(1+1)
    """
    i = np.searchsorted(WEIGHTS, random.random())
    global roulette_counts
    roulette_counts[i] += 1   
    return i    
    
def spin_roulette_wheel_twice():
    """" Spin the roulette wheel twice and return 2 different values """
    i1 = spin_roulette_wheel()
    while True:    
        i2 = spin_roulette_wheel()
        if i2 != i1:
            return i1, i2



def mutate(elements, complement):
    """Replace N_REPLACEMENTS of the in elements itmes from complement and return it"""
       
    #print 'mutate:', len(elements), len(complement) 
    assert len(elements) + len(complement) == n_elems
    try:
        # !@#$
        #added = random.sample(complement, N_REPLACEMENTS)
        added = set([])
        removed = random.sample(elements, N_REPLACEMENTS)
    except Exception as e:
        print e
        print '%d elemensts' % len(elements)
        print '%d complement' % len(complement)
        return set([]), set([]) 
    
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
    
    #assert not (d1 & d2), '\n%s\n%s' % (d1, d2)
   
    move_1_to_2 = set(random.sample(d1, (len(d1) + 1)// 2))
    move_2_to_1 = set(random.sample(d2, (len(d2) + 1)// 2))
    
    #assert not (move_1_to_2 & c2), '\n%s\n%s' % (move_1_to_2, c2)
    #assert not (move_2_to_1 & c1), '\n%s\n%s' % (move_2_to_1, c1)
        
    # Return shared + 1/2 of original + 1/2 swapped
    return move_2_to_1, move_1_to_2  

  
def get_state(values_weights, capacity, elements):  
    value = sum(value_weight[i][0] for i in elements)
    weight = sum(value_weight[i][1] for i in elements)
    return value, capacity - weight

    
def update_state(values_weights, value, capacity, added, removed):
    #assert not (added & removed)
    value += sum(values_weights[i][0] for i in added) - sum(values_weights[i][0] for i in removed)
    capacity -= sum(values_weights[i][1] for i in added) - sum(values_weights[i][1] for i in removed)   
    #assert value > 0, '\nadded=%s\removed=%s' % (added, removed)
    return value, capacity    
    
    
def get_score(value, capacity):
    if capacity >= 0:
        return True, value
    else: 
        return False, value / (1.0 - capacity) / 2.0    


def test(capacity, values_weights, solution):
    return 
    
def test2(capacity, values_weights, solution):    
    val = sum(values_weights[i][0] for i in solution.elements) 
    
    assert val == solution.value, '\n%d %d %d\n%s' % (solution.value, val, solution.value - val, solution)
    if capacity:
        wgt = sum(values_weights[i][1] for i in solution.elements) 
        assert capacity - wgt == solution.capacity, solution

class Tester:

    def setup(self, capacity, values_weights):
        self.capacity = capacity
        self.values_weights = values_weights
    
    def test(self, solution):
        test2(self.capacity, self.values_weights, solution)
        
tester = Tester()
        
class Solution:
    """A solution to the knapsack problem
        capacity: remaining capacity in the knapscack
        value: value of items in the knapscak
        elements: elements included in the knapsack
        complement: elements not included in the knapsacl
    """
    
    def _check(self):
        return
        assert self.value > 0, self
        assert self.elements and self.complement, self
        assert len(self.elements) + len(self.complement) == n_elems, self
        assert not (self.elements & self.complement), self
                
        
    def __init__(self, value, capacity, elements, complement):
        self.capacity = capacity
        self.value = value
        self.elements = elements
        self.complement = complement
        self._check()
        
    def __repr__(self):
        return repr(self.__dict__)
        
    def score(self):
        valid, scor = get_score(self.value, self.capacity)
        if valid:
            assert scor == self.value, 'valid=%s,score=%s,value=%s' % (valid, scor, self.value)
        return scor
        
    def valid(self):    
        return get_score(self.value, self.capacity)[1]
        
    def update(self, values_weights, added, removed):
        self._check()
        value, capacity = update_state(values_weights, self.value, self.capacity, added, removed)
        elements = (self.elements | added) - removed
        complement = (self.complement | removed) - added
        #assert not self.elements & added
        #assert not self.complement & removed
        #assert not added & removed
        #assert len(elements) + len(complement) == n_elems, '\n%d %s\n%d %s\n%s\n%s\n%s' % (
        #        len(elements), sorted(elements), 
        #        len(complement), sorted(complement),
        #        elements & complement,
        #        added,
        #        removed,
        #       )
        self._check() 
        val = sum(values_weights[i][0] for i in elements)
        #assert val == value, '\n%s\n%s\n\n%s\n%s' % (
        #    sorted(added), sorted(elements-self.elements), 
        #    sorted(removed),  sorted(self.elements-elements))
        solution = Solution(value, capacity, elements, complement)
        test(None, values_weights, solution)
        return solution
        
    def top_up(self, values_weights):
        if self.capacity <= 0:
            return
        for i, (v, w) in enumerate(values_weights):
            if i in self.complement and w <= self.capacity:
                self.value += v
                self.capacity -= w
                self.elements.add(i)
                self.complement.remove(i)
        self._check()    
        val = sum(values_weights[i][0] for i in self.elements)
        assert val == self.value, self        
                
    def update_top_up(self, values_weights, added, removed):            
        solution = self.update(values_weights, added, removed)
        solution.top_up(values_weights)
        self._check()
        val = sum(values_weights[i][0] for i in self.elements)
        assert val == self.value, self 
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
from collections import deque
import bisect

class SortedDeque(deque):

    def __init__(self, iterable, maxlen):
        super(SortedDeque, self).__init__(sorted(iterable), maxlen)

    def _insert(self, index, value):
        
        #print ' 1>', self, index, value 
        self.rotate(-index)
        #print ' 2>', self, index, value
        self.appendleft(value)
        #print ' 3>', self, index, value
        self.rotate(index)
        #print ' 4>', self, index, value
        #assert all(self[i-1] <= self[i] for i in range(1, len(self)))

    def insert(self, value):
        if len(self) >= self.maxlen:
            if value > self[-1]:
                return
            self.pop()
        self._insert(bisect.bisect_left(self, value), value)

if False:        
    d = SortedDeque([1,5,3], 3)
    print d
    for i in range(7):
        d.insert(i)
        print i, d, d[-1]
    exit()

def report(Q):
    for i in range(6):
        tester.test(Q[i][1])
    def rpt(q):
        return '%.0f%s' % (q.score(), '' if q.valid() else '*')
    top = str([rpt(Q[i][1]) for i in range(0,6)]).replace("'", '')  
    best = Q[0][1]     
    return '%s value=%6d capacity=%4d' % (top, best.value, best.capacity) 
  
  

    
def solve_ga(capacity, values, weights):

    print 'VERSION_NUMBER=%d' % VERSION_NUMBER
    print 'WEIGHT_RATIO=%.2f' % WEIGHT_RATIO
    print 'N_ENTRIES=%d' %  N_ENTRIES
    print 'N_PRIME_ENTRIES=%d' %  N_PRIME_ENTRIES
    print 'INVERSE_MUTATION_RATIO=%d' %  INVERSE_MUTATION_RATIO
    print 'N_REPLACEMENTS=%d' %  N_REPLACEMENTS
    print 'SORTED=%s' %  SORTED
   
    values_weights = zip(values, weights)
    n = len(values_weights)
    if SORTED:
        i_values_weights = list(enumerate(values_weights))
        i_values_weights.sort(key=lambda vw: -vw[1][0]/vw[1][1])
        indexes, values_weights2 = zip(*i_values_weights)
        values_weights.sort(key=lambda vw: -vw[0]/vw[1])

        for i in range(len(values_weights)):
            assert values_weights2[i] == values_weights[i]
    else:
        indexes = range(n)
        
    tester.setup(capacity, values_weights)    
    
    path = 'best%d_%d.results.%03d' % (n, capacity, VERSION_NUMBER)
    f = open(path, 'wt')
    f.write('VERSION_NUMBER=%d\n' %  VERSION_NUMBER)
    f.write('WEIGHT_RATIO=%.2f\n' %  WEIGHT_RATIO)
    f.write('N_ENTRIES=%d\n' %  N_ENTRIES)
    f.write('N_PRIME_ENTRIES=%d\n' %  N_PRIME_ENTRIES)
    f.write('INVERSE_MUTATION_RATIO=%d\n' %  INVERSE_MUTATION_RATIO)
    f.write('N_REPLACEMENTS=%d\n' %  N_REPLACEMENTS)
    f.write('SORTED=%s\n' %  SORTED)
    f.write('values_weights=%s\n' % [(i,v,w,v/w) for i,(v,w) in enumerate(values_weights)])

    
    random.seed(111) # The Nelson!
       
    
    global n_elems
    n_elems = n
 
    
    best_value = 0
    
    # Prime the heap with 1000 elements
    elements, complement = [], range(n)
    complem_set = set(complement)
    Q = SortedDeque([], N_ENTRIES)
    Qset = set(frozenset(q[1].elements) for q in Q)
    

    for i in range(N_PRIME_ENTRIES):
        value, cap, elts = solve_greedy(capacity, 0, values_weights, elements, complement)
        elts = set(elts)
        #if i % N_ENTRIES == N_ENTRIES - 1:
        #    print (-Q[0][0], value, -value/Q[0][0],  i), 
        assert len(elts) and len(complem_set - elts), '%d %d' % (len(elts), len(complem_set - elts))     
        solution = Solution(value, cap, elts, complem_set - elts)
        test(capacity, values_weights, solution)
        assert len(solution.elements) and len(solution.complement), '%d %d' % (
                len(solution.elements), len(solution.complement)) 
        if frozenset(solution.elements) not in Qset:
            Q.insert((-solution.score(), solution))
            Qset = set(frozenset(q[1].elements) for q in Q)         
       
        random.shuffle(complement) 

    best_value = Q[0][1].score() 
    print
    print 'best:', best_value, report(Q), Q[0][1].score()        
    print '-' * 80
        
    
    counter = count()
    counter2 = count()
 
    while True:
        assert len(Q) <= N_ENTRIES, 'len=%d' % len(Q)
        
        cnt = next(counter)
        if cnt % INVERSE_MUTATION_RATIO == 0:
            i = spin_roulette_wheel()
            assert Q[i][1].elements and Q[i][1].complement, 'i=%d,Q[i]=%s' % (i, Q[i])
            added, removed = mutate(Q[i][1].elements, Q[i][1].complement)
            solution = Q[i][1].update_top_up(values_weights, added, removed)
            test(capacity, values_weights, solution)            
            #Q.insert((-solution.score(), solution))
            if frozenset(solution.elements) not in Qset:
                Q.insert((-solution.score(), solution))
                Qset = set(frozenset(q[1].elements) for q in Q)    
        else:    
            i1, i2 = spin_roulette_wheel_twice()
            move_2_to_1, move_1_to_2 = crossOver(Q[i1][1].elements, Q[i2][1].elements)
            # assert not (move_2_to_1 & Q[i1][1].elements), 'i=%d' % (i1)
            #assert not (move_1_to_2 & Q[i2][1].elements), 'i=%d' % (i2)
            # Don't update Q unti all crossovers are extracted, otherwise we will update the wrong elements
            solution1 = Q[i1][1].update_top_up(values_weights, move_2_to_1, move_1_to_2)
            solution2 = Q[i2][1].update_top_up(values_weights, move_1_to_2, move_2_to_1)
            for solution in (solution1, solution2):
                if frozenset(solution.elements) not in Qset:
                    Q.insert((-solution.score(), solution))
                    Qset = set(frozenset(q[1].elements) for q in Q) 
            if False:
                for i, added, removed in (i1, move_2_to_1, move_1_to_2), (i2, move_1_to_2, move_2_to_1):
                    assert not (added & Q[i][1].elements), 'i=%d,i1=%d,i2=%d\n added      =%s\n move_2_to_1=%s\n move_1_to_2=%s' % (
                        i, i1, i2, added, move_2_to_1, move_1_to_2)  
                    solution = Q[i][1].update_top_up(values_weights, added, removed)
                    test(capacity, values_weights, solution)   
                    #Q.insert((-solution.score(), solution))
                    if frozenset(solution.elements) not in Qset:
                        Q.insert((-solution.score(), solution))
                        Qset = set(frozenset(q[1].elements) for q in Q)  

        if Q[0][1].score() > best_value:
            improvement = Q[0][1].score() - best_value
            best_value = Q[0][1].score()
            print 'best: %s %s %4d %8d' % (best_value, report(Q), improvement, cnt)  
            f.write('%s\n' % ('-' * 80))
            for i in range(1,6):
                q = Q[i][1]
                els = ', '.join('%d' % j for j in sorted(q.elements))
                f.write('%d: %s, value=%d, capacity=%d\n' % (q.score(), els, q.value, q.capacity)) 
            f.write('%s\n' % ('$' * 40))
            for i in range(1,6):
                q = Q[i][1]
                elements = [indexes[j] for j in q.elements]
                els = ', '.join('%d' % j for j in sorted(elements))
                f.write('%d: %s, value=%d, capacity=%d\n' % (q.score(), els, q.value, q.capacity))     
            f.flush()
        
        #if next(counter2) % 100000 == 0:    
        #    print 'roulette_counts=%s' % roulette_counts
