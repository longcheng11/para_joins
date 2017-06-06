para_joins
==========

test code for parallel joins over distributed systems

1, To improve the system I/O over disk, a small library based on zlib has been built and integrated in the x10 code 
   via the foreign function interface, so please install zlib (http://www.zlib.net/) at first.

2, How to run the test codes:
    (a) partition tuples into chunks and compress them in the form of .gz;
    (b) assign the input parameters follwoing the definition at the begining of each main function; and
    (c) compile each x10 code using the commond like: x10c++ -O -NO_CHECKS -o join PRPQ.x10 -post '# -lz'

3, How to set the environment of x10, please refer to http://x10-lang.org/.

4, If any questions, please email to l.cheng(AT)tue.nl