the uncleaned data:                
  where is it:           yz5077/hw/input/nft_unclean.csv             
it looks like this
<img width="1141" alt="Screen Shot 2022-04-26 at 8 48 17 PM" src="https://user-images.githubusercontent.com/58120560/165416230-93157f1e-90c9-4f7a-b86b-56d07c6937c1.png">
                                                 
after using mapreduce code                          (Clean.java CleanMapper.java)                                   
                         where is it:                              yz5077/hw/output/part-r-00000                               
it looks like this:
<img width="582" alt="Screen Shot 2022-04-26 at 8 51 15 PM" src="https://user-images.githubusercontent.com/58120560/165416497-9ad96415-db40-472c-a0a4-8b31779b5b66.png">

then i clean it on scala to do some final cleaning, which is to drop the last row of the dataset and give it column name
