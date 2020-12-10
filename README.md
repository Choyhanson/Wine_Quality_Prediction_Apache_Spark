## Course Project 2 -- (CS643)		hc398		HonSan, Choy

1. #### Create the EMR cluster to train an ML model in parallel on multiple EC2 instances (4 instances in total)

![image-20201209153439073](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209153439073.png)

​		I selected 1 master node, 2 core nodes and 1 task node for training purpose.

- ![image-20201209153704789](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209153704789.png)

  Upload the dataset and source code to S3 bucket 

- ![image-20201209155508078](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209155508078.png)

2. #### Create the EMR-notebook [notebook link](./main/EMR-Notebook.ipynb)

   ![image-20201209155742283](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209155742283.png)

3. #### Running pyspark on EMR cli

   ![image-20201209160413868](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209160413868.png)

   download the source code from S3 to EMR

   ![image-20201209160859816](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209160859816.png)

   Use spark submit command to run the training code, then save the model to S3

   ```
   spark-submit course_proj2_train.py
   ```

   The proof of using 4 instances of EMR cluster to train the model simultaneously 

   ![image-20201209161432761](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201209161432761.png)

   the input testing csv path can change to another S3 path, and the [result.txt ](./result.txt) for storing the predictions

   ```
   spark-submit course_proj2_docker.py s3://dataset-cs643-proj/ValidationDataset.csv > result.txt
   ```

4. #### Docker part

   ```
   git clone https://github.com/Choyhanson/course_proj2_cloud643.git
   cd course_proj2_cloud643/main
   
   ### run the spark frame jupyter by docker
   sudo docker run -it --rm -p 8888:8888 -v/ /home/ubuntu/course_proj2_cloud643/main:/home/jovyan/work jupyter/pyspark-notebook
   ```

   copy the the url from last line, replace http://127.0.0.1 with your ec2 public DNS

   ![image-20201210143208206](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201210143208206.png)

   ![image-20201210143501080](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201210143501080.png)

   Then can start the EMR-notebook with spark frame jupyter-notebook

   ![image-20201210143858970](https://github.com/Choyhanson/course_proj2_cloud643/blob/main/user_images/image-20201210143858970.png)
