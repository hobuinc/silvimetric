import numpy as np

# adapted from: https://xiaoganghe.github.io/python-climate-visuals/chapters/data-analytics/scipy-basic.html
# added b3 and l4 from:
#   Vogel, R. M. and Fennessey, N. M.: L moment diagrams should replace product moment diagrams,
#   Water Resour. Res., 29, 1745–1752, https://doi.org/10.1029/93WR00341, 1993.
def lmom4(data):
    # lmom4 returns the first four L-moments of data
    # data is the 1-d array    
    # n is the total number of points in data, j is the j_th point
    #
    # j range in for loops starts with 1 so we need to subtract 1 for all b# equations

    n = len(data)
    # sort in descending order
    data = np.sort(data.reshape(n))[::-1]
    b0 = np.mean(data)
    b1 = np.array([(n - j - 1) * data[j] / n / (n - 1)
                   for j in range(n)]).sum()
    b2 = np.array([(n - j - 1) * (n - j - 2) * data[j] / n / (n - 1) / (n - 2)
                   for j in range(n - 1)]).sum()
    b3 = np.array([(n - j - 1) * (n - j - 2) * (n - j - 3) * data[j] / n / (n - 1) / (n - 2) / (n - 3)
                   for j in range(n - 2)]).sum()
    l1 = b0
    l2 = 2 * b1 - b0
    l3 = 6 * (b2 - b1) + b0
    l4 = 20 * b3 - 30 * b2 + 12 * b1 - b0

    return l1, l2, l3, l4
