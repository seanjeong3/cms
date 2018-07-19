import numpy as np
from ctypes import *
from numpy.ctypeslib import ndpointer

class SPOT:
	def __init__(self, init_size = 100, m=2, q=1e-3):
		self.spot = CDLL('./spot.so')
		self.spot.preprocess_data.restype = c_double
		self.spot.run_spot.restype = ndpointer(dtype=c_double, shape=(3,))
		self.init_data = np.zeros(init_size)
		self.init_cnt = 0
		self.init_size = init_size
		self.m = m
		self.q = q
		
	def initialize(self, m, q):
		init_scores = (c_double*len(self.init_data))(*self.init_data)
		self.spot.spot_init(init_scores, c_int(self.init_size), c_int(m), c_double(q))
		
		
	def preprocess(self, raw):
		rawdat = (c_double*len(raw))(*raw)
		return self.spot.preprocess_data(rawdat,c_int(len(raw)))
		
	def run(self, raw):
		score = self.preprocess(raw)
		result = {}
		if (self.init_cnt < self.init_size):
			self.init_data[self.init_cnt] = score
			self.init_cnt += 1
			result['thres'] = None
			result['alarm'] = None
			result['score'] = score
			return result
		elif (self.init_cnt == self.init_size):
			self.initialize(self.m, self.q)
			self.init_cnt += 1
			result['thres'] = None
			result['alarm'] = None
			result['score'] = score
			return result
		else:
			spot_result = self.spot.run_spot(c_double(score))
			result['thres'] = spot_result[0]
			if spot_result[1] == 1:
				result['alarm'] = True
			elif spot_result[1] == 0:
				result['alarm'] = False
			else:
				result['alarm'] = None
			result['score'] = score
			return result
