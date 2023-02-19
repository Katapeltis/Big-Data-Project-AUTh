import numpy as np
from numpy import random as rd
import pandas as pd


def uniform(d,N):
  data=rd.random((d,N))
  return(data)


def correlated(d,N,diaspora=0.05): 
  cov_matrix=np.zeros((d,d))+1-diaspora
  np.fill_diagonal(cov_matrix,1)
  data = rd.multivariate_normal(np.zeros(d), cov_matrix, size=N,\
                                       check_valid='raise')
  return(data.T)


def anticorrelated(d,N,diaspora=0.05): 
  covs=np.zeros((d,d))-1+diaspora
  
  for i in range(d):
    j_init=i%2
    for j in range(j_init,d,2):
      covs[i][j]=-covs[i][j]

  np.fill_diagonal(covs,1)
  data = rd.multivariate_normal(np.zeros(d), covs, size=N,\
                                       check_valid='raise')
  return(data.T)


def normal(d,N):
  data=rd.normal(size=(d,N))
  return(data)


#optional: kanonikopoiisi twn data sto range [0,1]
#dixws vlavi tis katanomis
def normalize(data):
  d=data.shape[0]
  for i in range(d):
    data[i]=data[i]-min(data[i])
    data[i]=data[i]/max(data[i])
  return(data)

#add headers D1, D2 ... Dn as first row
def headers(data):
  headers=["D"+str(i+1) for i in range(data.shape[0])]
  data = pd.DataFrame.from_dict(dict(zip(headers, data)))
  return(data)

def scala_prep(data):
    #works if data is numpy.ndarray
    cols = [str(i) for i in range(data.shape[1])]
    res = []
    res.append(cols)
    for i in range(data.shape[0]):
        res.append(data[i].tolist())
    df=pd.DataFrame(res[1:], columns = res[0])
    return df
