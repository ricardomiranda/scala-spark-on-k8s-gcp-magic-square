# README #

(C) Ricardo Cristovao Miranda, 2020.

This program solves a magic square puzzle using genetic Programming. 
From the wikipedia, lets see what is what is a magic square:

In recreational mathematics, a magic square is an arrangement of 
distinct numbers (i.e., each number is used once), usually integers, 
in a square grid, where the numbers in each row, and in each column, 
and the numbers in the main and secondary diagonals, all add up 
to the same number, called the “magic constant.” A magic square has 
the same number of rows as it has columns, and in conventional 
math notation, "n" stands for the number of rows (and columns) it has. 
Thus, a magic square always contains n2 numbers, and its size 
(the number of rows [and columns] it has) is described as being "of 
order n." A magic square that contains the integers from 1 to n2 
is called a normal magic square. (The term "magic square" is also 
sometimes used to refer to any of various types of word squares.)


### What is this repository for? ###

* This is a simple GP example complete with tests and the most 
ideomatic Scala
* This is a Scala Spark program that runs in Kubernetes, using 
GCS (Google Cloud Storage) for peersistence.

### How do I get set up? ###

* This a very simple CLI program that uses sbt. No configuartion 
necessary. Dependencies are in build.sbt
* To run test do: sbt test
* To produce a jar file do: sbt package

### Contribution guidelines ###

* For code to be accepted it must have tests

### Who do I talk to? ###

* mail@ricardoMiranda.com