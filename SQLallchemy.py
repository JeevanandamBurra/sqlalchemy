
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd


# In[2]:


df=pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data',sep=',',header=None)


# In[3]:


Dff=df.head(100)


# In[49]:


Dff.columns = ['age', 'workclass','fnlwgt','education','educationnum','maritalstatus','occupation','relationship',
               'race','sex','capitalgain','capitalloss','hoursperweek','nativecountry','label']


# In[50]:


Dff.head()


# ## Create an sqlalchemy engine using a sample from the data set

# In[51]:


import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String,VARCHAR
engine = create_engine('sqlite:///:memory:', echo=True)


# In[52]:


Dff.head(1)


# In[92]:


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import update,delete,func,and_,or_


# In[54]:


Base = declarative_base()

class Adult(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'adult'
   
   
    age = Column(Integer, primary_key=True, nullable=False) 
    workclass = Column(VARCHAR(40))
    fnlwgt = Column(Integer)
    education = Column(VARCHAR(40))
    educationnum = Column(Integer)
    maritalstatus = Column(VARCHAR)
    occupation = Column(VARCHAR(40))
    relationship = Column(VARCHAR(40))
    race = Column(VARCHAR(40))
    sex = Column(VARCHAR(40))
    capitalgain = Column(Integer)
    capitalloss = Column(Integer)
    hoursperweek = Column(Integer)
    nativecountry = Column(VARCHAR(40))
    label = Column(VARCHAR(40))

engine = create_engine('sqlite:///test.db')
Base.metadata.create_all(engine)
session = sessionmaker()
session.configure(bind=engine)


# In[55]:


df = Dff.head(10)
df.to_sql(con=engine,name=Adult.__tablename__, if_exists='replace')


# In[68]:


s = session()
s.close()


# ## 2. Write two basic update queries

# In[69]:


q = update(Adult).where(Adult.maritalstatus=='Never-married').values(maritalstatus='NM')

engine.execute(q)


# In[70]:


q = update(Adult).where(and_(Adult.maritalstatus=='NM',Adult.age<40)).values(race='Red')

engine.execute(q)


# ## 3. Write two delete queries

# In[72]:


del_q=delete(Adult).where(Adult.education=='11th')

engine.execute(del_q)


# In[73]:


del_q1=delete(Adult).where(Adult.hoursperweek=='13')

engine.execute(del_q1)


# ## 4. Write two filter queries

# In[83]:


fliter1=s.query(Adult).filter(Adult.sex=='Male')
for row in fliter1:
    print(row.label)


# In[84]:


fliter2=s.query(Adult).filter(and_(Adult.workclass=='Private',Adult.sex=='Male'))
for row in fliter2:
    print(row.nativecountry)


# ## 5. Write two function queries

# In[90]:


F_avg=s.query(func.avg(Adult.age).label('m_avg')).group_by(Adult.workclass).all()

print(F_avg[0].m_avg)


# In[89]:


F_age=s.query(func.max(Adult.age).label('m_age')).group_by(Adult.sex).all()

print(F_age[0].m_age)

