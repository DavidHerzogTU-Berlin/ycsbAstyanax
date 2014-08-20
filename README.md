Astyanax Client in: Yahoo! Cloud System Benchmark (YCSB)
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)

Links
-----
http://wiki.github.com/brianfrankcooper/YCSB/  
http://research.yahoo.com/Web_Information_Management/YCSB/  
ycsb-users@yahoogroups.com  

Getting Started
---------------

1. Clone the latest release of YCSB:

    ```sh
    git clone https://github.com/DavidHerzogTU-Berlin/ycsbAstyanax.git
    cd ycsbAstyanax
    mvn clean package
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 
    
    ```sh
    bin/ycsb load astyanax-1 -P workloads/workloada
    bin/ycsb run astyanax-1 -P workloads/workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.
