#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
from fractions import gcd
import time
import logging
from solver_ga import solve_ga

def solve_greedy(capacity, values, weights):

    items = len(values)
    # a trivial greedy algorithm for filling the knapsack
    # it takes items in-order until the knapsack is full
    value = 0
    weight = 0
    taken = []

    for i in range(0, items):
        if weight + weights[i] <= capacity:
            taken.append(1)
            value += values[i]
            weight += weights[i]
        else:
            taken.append(0)

    return value, taken


def solve_dp(capacity, values, weights):
    """Build a dynamic programming table with each column is a capacity
        and each row the number of elements
    """
    
    logging.info('solve_dp')
        
    vw = zip(values, weights)
    n_items = len(vw)
    table = [[0] * (capacity+1) for _ in range(n_items+1)]
  
    for i in range(1, n_items + 1):
        v, w = vw[i-1]
        for cap in range(1, capacity + 1):
            if w > cap:
                table[i][cap] = table[i-1][cap]
            else:
                candidate1 = table[i-1][cap]
                candidate2 = table[i-1][cap-w] + v 
                table[i][cap] = max(candidate1, candidate2)
    
    value = table[-1][-1]            
    weight = capacity
    taken = []
    for i in range(n_items, 0, -1):
        if table[i][weight] != table[i-1][weight]:
            taken.append(i-1)
            weight -= vw[i-1][1]
            if weight == 0:
                break
                
    return value, taken, True

    
def ancestors_ptr_list(ptr):
    items = []
    while ptr:
        ptr, i = ptr
        items.append(i)
    return items
    
    
def ancestors_list_ptr(items):
    ptr = None
    for i in items:
        ptr = [ptr, i]
    return ptr 
    
    
from itertools import count
from heapq import heappush, heappop

DEPTH_FIRST, BEST_FIRST, HYBRID = range(3)

def solve_bb(capacity, values, weights, method, max_time, best_in=None):
    """Branch and bound solution"""
    
    print 'solve_bb" method=%d,max_time=%d,best_in=%s' % (method, max_time, best_in)
    logging.info('solve_bb" method=%d,max_time=%d,best_in=%s' % (method, max_time, best_in))
    
    n = len(values)
    indexes = range(n)
    indexes.sort(key=lambda i: -values[i]/weights[i])
    values_weights = [(values[i], weights[i]) for i in indexes]
 
    best = [0, []]
    if best_in:
        inverse_indexes = list(enumerate(indexes))
        inverse_indexes.sort(key=lambda x: x[1])
        inverse_indexes = [i for _, i in inverse_indexes]
        best[0] = best_in[0]
        best[1] = ancestors_list_ptr([inverse_indexes[i] for i in best_in[1]])
    
    counter = count()
    end_time = time.time() + max_time
    timedout = [False]
    
    def times_up(current_count):
        if not current_count % 1000:
            return False
        return time.time() > end_time

    def bound(sv, sw, m):
        """Return an upper bound on the value of a knapsack
            whose first m items (in v/w descending order) have
            value sv and weight sw
        """
        if m == n:
            return sv
        for av, aw in values_weights[m:]:
            if sw + aw > capacity:
                break
            sv += av
            sw += aw
        return sv + (capacity - sw) * av/aw
    
    def branch(sv, sw, m, parent):
        """Given a node n elements from the root with value sv and weight sw in the m elements,
            generate all nodes below this node that are better than best[0]
        """
        if sw > capacity:
            return
        if sv > best[0]:
            best[0], best[1] = sv, parent
            if True:
                print 'best=%d' % best[0],
                if best[0] > 8000:
                    print sorted(ancestors_ptr_list(parent))
                else:
                    print
        if m == n:
            return
        i = indexes[m]  # !!! Insert original index, not sorted index
        v, w = values_weights[m]
        choices = ((sv, sw, parent), (sv + v, sw + w, [parent, i]))
        for sv, sw, ptr in choices:
            b = bound(sv, sw, m+1)
            if b > best[0]:
                yield b, branch(sv, sw, m+1, ptr)
       
    def explore_depth_first(node):
        print 'number items:', len(values_weights), len(values), len(weights)
        stack = [node]
        while stack and not timedout[0]:
            for _, node in stack.pop():
                stack.append(node)
  
    def explore_best_first(node):            
        heap = [(0, next(counter), node)]
        while heap and not timedout[0]:
            for b, node in heappop(heap)[2]:
                heappush(heap, (b, next(counter), node))
                
    MAX_ENTRIES = 1000 * 1000 * 10           

    def explore_hybrid(node):            
        heap = [(0, next(counter), node)]
        while heap and not timedout[0]:
            _, _, results = heappop(heap)
            for b, node in results:
                #print (len(heap),MAX_ENTRIES),
                if len(heap) > MAX_ENTRIES:
                    print 'depth-first'
                    explore_depth_first(node)
                else:
                    heappush(heap, (b, next(counter), node))            

    if method == DEPTH_FIRST:
        explore_best_first(branch(0, 0, 0, None))
    elif method == BEST_FIRST:
        explore_depth_first(branch(0, 0, 0, None))
    elif method == HYBRID:
        explore_hybrid(branch(0, 0, 0, None))
        
    return best[0], ancestors_ptr_list(best[1]), not timedout[0]
    

def gcds(lst):
    a = lst[0]
    for b in lst[1:]:
        a = gcd(a, b)
    return a

    
def solve(capacity, values, weights):

    MAX_TIME = 4 * 60 * 60

    d = gcds(weights)
    if d > 1:
        weights = [w//d for w in weights]
        capacity = capacity//d

    n = len(values)
    
    print 'n=%d' % n
    print 'capacity=%d' % capacity
    print 'n * capacity=%d' % (n * capacity)
   
    if n < 20:
        value, taken, optimal = solve_bb(capacity, values, weights, HYBRID, MAX_TIME)
    elif n * capacity <= 10 ** 8: 
        value, taken, optimal = solve_dp(capacity, values, weights)
    else:
        value, taken, optimal = solve_ga(capacity, values, weights, 120)
        value, taken, optimal = solve_bb(capacity, values, weights, HYBRID, MAX_TIME, [value, taken])

    taken = set(taken)
    return value, [1 if i in taken else 0 for i in range(n)], optimal

    
def solveIt(inputData):
    # Modify this code to run your optimization algorithm

    # parse the input
    lines = inputData.split('\n')

    firstLine = lines[0].split()
    items = int(firstLine[0])
    capacity = int(firstLine[1])

    values = []
    weights = []

    for i in range(1, items + 1):
        line = lines[i]
        parts = line.split()
        values.append(int(parts[0]))
        weights.append(int(parts[1]))

    value, taken, optimal = solve(capacity, values, weights)

    # prepare the solution in the specified output format
    return '%d %d\n%s' % (value, int(optimal), ' '.join(map(str, taken)))

import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        fileLocation = sys.argv[1].strip()
        inputDataFile = open(fileLocation, 'r')
        inputData = ''.join(inputDataFile.readlines())
        inputDataFile.close()
        print solveIt(inputData)
    else:
        print 'This test requires an input file.  Please select one from the data directory. (i.e. python solver.py ./data/ks_4_0)'

