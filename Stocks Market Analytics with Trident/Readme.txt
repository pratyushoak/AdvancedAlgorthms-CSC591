Readme:
Software that needs to be installed (if any) with download and installation instructions
No extra software is needed.

Environment variable settings (if any)
No environmental variable settings need to be changed for execution

Instructions on how to run the program
	1.	Unzip the "tutorial" folder under "apache-storm-0.9.3/examples/storm-starter/src/jvm/storm/starter/trident/" directory.
	2.  cd to the "storm-starter" directory at "/apache-storm-0.9.3/examples/storm-starter"
	3.  Run the command "mvn package" to build the topology
	4.  Run the command "storm jar target/storm-starter-0.9.3-jar-with-dependencies.jar storm.starter.trident.tutorial.DRPCStockQueryTopology > output.txt &"
	5.  In the above step appropriately provide the required jar file  
	6.	After a duration of time ( say 5 mins), type the "ps" command to view the running processes and kill the java process of our program
	
Instructions on how to interpret the results
	1. The output will be written to the "output.txt" file
	2. The output written will include all the connection and other process information that we may not require in this case 
	3. Type the following command to view the DRPC results: "cat output.txt | grep "DRPC RESULT"
	4. The result that we see on the console is the total volume of stocks for the companies Apple, Intel and GE ( input from the stocks.csv file and not a live stream).
	5. New volume of stocks queried and displayed every 5 seconds.
	
Sample input and output files (if applicable)
Submitted screenshots for the output.
l2_output.png

References to any software you may have used (if applicable)
Not applicable.
