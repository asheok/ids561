#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window
import pandas as pd # our main data management package
import string # used for preprocessing
import re # used for preprocessing
import numpy as np # used for managing NaNs


# # Part 1

# In[2]:


df=spark.read.csv('gs://buck_561/ass2/Amazon_Responded_Oct05.csv', header = True, multiLine=True, sep=',',quote="\"",escape="\"",inferSchema=True)## reading csv file
df.count(),df.show(5,False)


# In[3]:


df1 = df.select('tweet_created_at','user_screen_name','user_id_str').withColumn("user_id_str", F.col('user_id_str').cast("Decimal(10,0)")) # correcting the column values

## selecting required columns
df1.show(10,False),df1.count()


# In[4]:


# filtering out the verified users
df2=df1.withColumn("date",F.substring(F.col("tweet_created_at"),5,6)) ## extracting date from tweet_created_at column
df2.count(),df2.show(10)


# In[5]:


daily_active_users=df2.groupBy('user_screen_name','user_id_str').agg(F.countDistinct(F.col('date')).alias('count')).filter('count>=5').drop('count')  ## filtering users active on 5 or more days
daily_active_users.show(10)


# In[6]:


print('number of active users: ',daily_active_users.count())


# In[7]:


daily_active_users.coalesce(1).write.mode('overwrite').csv('gs://buck_561/ass3/daily_active_users.csv',header=True)


# # Part 2

# In[8]:


dfe = spark.read.csv('gs://buck_561/ass3/experiment.txt', header = False, inferSchema=True).withColumnRenamed("_c0", "user_id_str") # reading csv and renaming the column
dfe.show(10),dfe.count()


# In[9]:


dfs=dfe.join(daily_active_users,'user_id_str','left') # join the file with 'daily_active_users'
dfs1 = dfs.withColumn('whether_active',F.when(F.col('user_screen_name').isNull(),F.lit('no')).otherwise(F.lit('yes'))).drop('user_screen_name') ##  creating column with yes or no values
dfs1.show(10), dfs1.count()


# In[10]:


dfs2=dfs1.groupby('whether_active').count() ## counting number of active users
dfs2.show(), dfs2.count()


# In[11]:


print('percentage of active users: ',(dfs2.orderBy('whether_active').select('count').collect()[1]['count']/dfs1.count())*100)


# In[12]:


dfs1.coalesce(1).write.mode('overwrite').csv('gs://buck_561/ass3/experiment_final.csv',header=True)


# # Part 3

# In[13]:


dff = spark.read.csv('gs://buck_561/ass3/final_experiment.csv', header = True, inferSchema=True).drop('whether_active','user_Screen_name') ## reading csv file and droping null columns to be created again
dff.show(10),dff.count()


# In[14]:


dff=dff.withColumn("user_id_str", F.col('user_id_str').cast("Decimal(10,0)")) # correcting the column values
dff.show(10),dff.count()


# In[15]:


a=dff.join(dfs1,'user_id_str','left').join(daily_active_users,'user_id_str','left') # left joining three tables, two at a time to keep all the records from final experiment files
a.show(10)


# In[16]:


a.coalesce(1).write.mode('overwrite').csv('gs://buck_561/ass3/final_experiment_final.csv',header=True)


# In[ ]:





# In[ ]:




