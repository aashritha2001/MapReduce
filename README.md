# MapReduce
Using MapReduce to solve problems 

Q1. Write a MapReduce program in Hadoop that finds Mutual/Common friend list of two friends specifically
for the pairs (0,1), (20, 28193), (1, 29826), (6222, 19272), (28091, 28056). The key idea is that if two
people are friend then they have a lot of mutual/common friends. This program will find the
common/mutual friend list for them.

Q2. : Please use in-memory join at the Mapper to answer the following question.
For each user print User ID and average age till 01/01/2022 of direct friends of this user.
Note that the userdata.txt will be used to get the extra user information and cached/replicated at each
mapper.

Q3. Please use join at the Reducer to answer the following question.
Given any two Users (they are friend) as input, output a list containing the dates of birth of all their
mutual friends and the age of the oldest friend who were born after 1981.

Q4. Write a program that will construct inverted index in the following ways.
The map function parses each line in an input file, userdata.txt, and emits a sequence of < token, line
number> pairs. The reduce function accepts all pairs for a given word, sorts the corresponding line 
numbers, and emits a < token, list(line numbers)> pair. The set of all the output pairs forms a simple
inverted index. Tokens can contain numbers and dates too

Q5. Please use a Combiner to answer the following question.
Filter the inverted index to keep only those tokens that are words (include names, leave out any
numbers, dates or userids). Then, find all those words(s) whose number of occurrences is the
maximum among all the words in the inverted index. Note that you need to use the same output from
