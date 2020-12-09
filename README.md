## Course Project 2 -- (CS643)		hc398		HonSan, Choy

1. #### Create the EMR cluster to train an ML model in parallel on multiple EC2 instances (4 instances in total)

![image-20201209153439073](./user_images-20201209153439073.png)

​		I selected 1 master node, 2 core nodes and 1 task node for training purpose.

- ![image-20201209153704789](./user_images-20201209153704789.png)

  Upload the dataset and source code to S3 bucket 

- ![image-20201209155508078](./user_images-20201209155508078.png)

2. #### Create the EMR-notebook [notebook link](./EMR-Notebook.ipynb)

   ![image-20201209155742283](./user_images-20201209155742283.png)

3. #### Running pyspark on EMR cli

   ![image-20201209160413868](./user_images-20201209160413868.png)

   download the source code from S3 to EMR

   ![image-20201209160859816](./user_images-20201209160859816.png)

   Use spark submit command to run the training code, then save the model to S3

   ```
   spark-submit course_proj2_train.py
   ```

   The proof of using 4 instances of EMR cluster to train the model simultaneously 

   ![image-20201209161432761](./user_images-20201209161432761.png)

   the input testing csv path can change to another S3 path, and the [result.txt ](./result.txt) for storing the predictions

   ```
   spark-submit course_proj2_docker.py s3://dataset-cs643-proj/ValidationDataset.csv > result.txt
   ```

   