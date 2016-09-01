# DocClassification

Document classification (Project 1) using Naive Bayes

Team # n00bs

Steps to get it up and running:

1. Check out the code
2. Fix the s3 credentials (if you want to access s3) and path names in main.scala file 
3. Fingers crossed, Run it!

# How this thing works?

Following are the main steps in the run-up to classification: 

1. Build the two inputs (X & Y) based on ZipIndex  

2. Build the vocabulary based on X

3. Read the stop word list that is provided 

4. Since, Naive Bayes needs different statistics to compute the probability, we run a sequence of transformations/actions in the training step through the whole corpus that eventually returns the following structure:
   Map[String, (Long, Map[String, (Double, Double)])], where the constituents are:
   Map[TargetType (T), (# of docs with label T, Map[Word (W), (# of total occurrences against T in the whole corpus, # of T docs with word W in it)])]

5. Once we have the above convoluted structure along with some other auxiliary structures that hold class probabilities, we can answer every stat that NB may need. 

6. In the classification, we log-sum all the probabilities after removing the words that are in StopList

7. Based on the highest value, emit the result. 
               
