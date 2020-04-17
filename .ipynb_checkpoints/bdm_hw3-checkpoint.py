
import csv
import sys
from pyspark import SparkContext


def extractComplains(partId, records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        
        if len(row) == 18:
        
            (date, product, company) = (int(row[0][:4]), row[1].lower(), row[7].lower())
        
            yield ((date, product, company), 1)


            
def conver_csv(_, records):
    
    for (year, product), (complains, companies, topComp) in records:
        
#         if ',' in product:
        product = '"{}"'.format(product)
            
        yield ','.join((product, str(year), str(complains), str(companies), str(topComp)))
        
        
def main(sc):
    
    complains_df = sc.textFile(sys.argv[1]).mapPartitionsWithIndex(extractComplains) \
                  .reduceByKey(lambda x, y: x+y) \
                  .map(lambda x: (x[0][:2], (x[1], 1, x[1]))) \
                  .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], max(x[2], y[2]))) \
                  .mapValues(lambda x: (x[0], x[1], round(100*x[2]/x[0]))) \
                  .mapPartitionsWithIndex(conver_csv).saveAsTextFile(sys.argv[2])
    
    
if __name__ == '__main__':
    
    sc = SparkContext()
    
    main(sc)