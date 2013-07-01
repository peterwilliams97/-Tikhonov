#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    A genetic algorithm implementation

"""
from __future__ import division
import sys, os, random
from math import *

# Set random seed so that each run gives same results
random.seed(555)

WEIGHT_RATIO = 0.8

#
# Based on roulette dicts
#

N_ENTRIES = 1000
TOTAL_WEIGHT = sum([WEIGHT_RATIO**(i+1) for i in range(N_ENTRIES)]) 
WEIGHT_SERIES = tuple([WEIGHT_RATIO**(i+1)/TOTAL_WEIGHT for i in range(N_ENTRIES)])

def applyWeights(Q):
    """roulette_in is a list of roulette dicts
        Add and 'idx' and 'weight' keys to roulette dicts 
        'weight' value is based on 'score' value of dict.
        Return list of roulette dicts sorted by weight
        For use in rouletteWheel 
    """
    for i, x in enumerate(Q):
        x.weight = WEIGHT_SERIES[i]

    
def _spin_wheel(Q):
    """ Find the roulette wheel winner
        returns: an index with probability proportional to dict's 'weight'
    """
    v = random.random()             # The value we threw
    bottom = 0.0                    # Bottom value of the ith entry
    for i, x in enumerate(Q):
        top = bottom + x.weight     # Top value of the ith entry
        if v <= top:
            return i
        base = top
    raise RuntimeError('Cannot be here')

def spinRouletteWheel(roulette_in):
    """ Find the roulette wheel winner
        roulette_in: a list of dicts with keys 'idx' and 'weight'
        returns: an index with probability proportional to dict's 'weight'
    """
    roulette = applyWeights(roulette_in)
    return _spin_wheel(roulette)
    
    
def spinRouletteWheelTwice(roulette):
    """" Spin the roulette wheel twice and return 2 different values """
    roulette = applyWeights(roulette)
    i1 = _spin_wheel(roulette)
    while True:    
        i2 = _spin_wheel(roulette)
        if i2 != i1:
            return i1, i2


N_REPLACEMENTS = 1
def mutate(elements, complement):
    """Replace N_REPLACEMENTS of the in elements itmes from complement and return it"""
       
    taken = elements[:]    
    for _ in range(N_REPLACEMENTS):
        taken[randint(0, len(taken)] = complement[randint(0, len(taken)]
    return taken
    

def crossOver(c1, c2):
    """Swap half the elements in c1 and c2
        c1: a list of ints
        c2: a list of ints
    """

    # Find elements that are not in both lists
    d1 = sorted(c1, key = lambda x: x in c2)
    d2 = sorted(c2, key = lambda x: x in c1)
    for i1, x in enumerate(d1):
        if x in d2:
            break
    for i2, x in enumerate(d2):
        if x in d1:
            break
    m = min(i1, i2)     # number of non-shared elements

    shared = d1[m:]     # elements in both lists
    d1 = d1[:m]         # elements only in c1
    d2 = d2[:m]         # elements only in c2    
    shuffle(d1)
    shuffle(d2)
    m1 = len(d1) // 2
    m2 = len(d2) // 2
    
    # Return shared + 1/2 of original + 1/2 swapped
    return shared + d1[:m1] + d2[m2:], shared + d2[:m2] + d1[m1:] 
    
 
    
def mutate(n, capacity, values_weights, elements):
    complement = list(set(xrange(n)) - set(elements))
    elements = _mutate(elements, complement)
        
    weight = sum(values_weights[i][1] for i in elements)
    while weight > capacity:
        i = elements.pop()
        weight -= values_weights[i][1]
    
    return elements    
        
        
def get_score(capacity, values_weights, elements):
    value = sum(value_weight[i][0] for i in elements)
    weight = sum(value_weight[i][1] for i in elements)
    if weight <= capacity:
        return weight - capacity, value
    else: 
        return None, value / (weight / capacity) / 2.0    
    
class Solution:
    """A solution to the knapsack problem"""

    def __init__(self, capacity, values_weights, elements):
        self.elements = elements
        self.score = get_score(capacity, values_weights, elements)
        
        
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
        elements.append(i)

    return value, elements        
    
INVERSE_MUTATION_RATIO = 10    
def solve_ga(capacity, values, weights):
    
    random.seed(111) # The Nelson!
    
    values_weights = zip(values, weights)
    n = len(values_weights)
    best_value = 0
    
    # Prime the heap with 1000 elements
    elements, complement = [], range(N)
    Q = []
    for _ in range(1000):
        value, taken = solve_greedy(capacity, 0, values_weights, elements, complement)
        entry = Solution(value, taken)
        heappush(Q, (entry.score, entry))
        shuffle(complement)   

    while True:
        if next(counter) % INVERSE_MUTATION_RATIO == 0:
            i = spinRouletteWheel(Q)
            elements, complement = mutate(Q[i].elements, Q[i].complement)
            cap, value = get_score(capacity, values_weights, elements):
            if cap:
                value, taken = solve_greedy(cap, 0, values_weights, elements, complement)
            e = Solution(value, elements)    
            heapreplace(Q, (e.score, e)    
        else:    
            i1, i2 = spinRouletteWheelTwice(Q)
            c1, c2 = crossOver(Q[i1].elements, Q[i2].elements)
            for c in c1, c2:
                cap, value = get_score(capacity, values_weights, elements):
                if cap:
                    value, taken = solve_greedy(cap, 0, values_weights, elements, complement)
                e = Solution(value, elements)    
                heapreplace(Q, (e.score, e)    

        if Q[0].score > best_value:
            print best_value

