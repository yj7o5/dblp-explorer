### Load
My main goal was to make it faster thus I decided to pay the cost up front by loading all the individual objects into the memory first. From the actual user standpoint, one may not want to wait for hours to search for papers but instead would want to have faster results thus I utilized `ConcurrentHashMap` to store all the papers and their references in memory which took around ~4 mins to load the whole file in memory. The only downside here is that if a machine is limited to 8 gigs or lower memory it may run out of memory, it hasn't been optimized for that but will be considered later. 

### Processing 
Once the file is loaded in memory, it goes each of the tier from the references it made when loading the file and searching for the documents with title on them. When searching papers in tier it takes an order of O (1) to search in the set because `ConcurrentHashMap` implementation. 
