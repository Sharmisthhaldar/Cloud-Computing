import sys
import numpy as np
from pyspark import SparkContext
from numpy.linalg import inv

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])

  yxlines = yxinputFile.map(lambda line: line.split(','))
  
  # Calculating X. X transpose
  def first_value(data):
   data[0] = 1.0
   X = np.asmatrix(np.array(data).astype('float')).T
   #Product of X and X transpose
   return np.multiply(X, X.T)

	# calculation of X.Y matrices
  def second_value(data):
      Y = float(data[0])
      data[0] = 1.0
      X = np.asmatrix(np.array(data).astype('float')).T
      return np.multiply(X, Y)


   # Calculating x.x Transpose
  first_part = yxlines.map(lambda data:("first_part",first_value(data)))

	# Reducer to sum the values of each key and calculating the value
  A = first_part.reduceByKey(lambda x1,x2: np.add(x1,x2)).map(lambda v:v[1]).collect()[0]

	# Calculating inverse
  A_inverse = inv(A)    

	# calculating x.y Transpose
  second_part = yxlines.map(lambda data:("second_part",second_value(data)))

	# Reducer to sum the values of each key and calculating the value
  B = second_part.reduceByKey(lambda x1,x2: np.add(x1,x2)).map(lambda v:v[1]).collect()[0]
   
   # calculation of the beta coefficient for the multiple linear regression
  beta = np.dot(A_inverse, B)
   
   # Save the RDD
  sc.parallelize(beta).saveAsTextFile("Liner_reg_output")

  # print the linear regression coefficients in desired output format
  print("beta: ")
  
  for coeff in beta:
    print(coeff)

  sc.stop()
