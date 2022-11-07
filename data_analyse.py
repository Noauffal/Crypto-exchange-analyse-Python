from numpy import size
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from pyvis.network import Network






spark=SparkSession.builder.appName('Dataframe').getOrCreate()

parDF2 = spark.read.parquet("../all_data/transactions_all")
parDF2.createOrReplaceTempView("data")

# parDF2 = spark.read.parquet("/mnt/e/all_data20.parquet")
# parDF2.createOrReplaceTempView("data")

# parDF2 = spark.read.parquet("/mnt/e/data_20_actors.parquet")
# parDF2.createOrReplaceTempView("data")

############################## STEP 1 #####################################
## Evolution du capitale des exchanges qui sont les plus interessant avant 2016

acteurs = ['Huobi.com-2','Bitstamp.net']

res = spark.sql("SELECT src_identity, dst_identity, value, year, month, day, time " + 
                "FROM data " +
                "WHERE year<=2015 and dst_identity in ('Huobi.com-2','Bitstamp.net') and dst_identity <> src_identity ORDER BY dst_identity, time")


df_dst = res.toPandas()
valCum = []
for index, row in df_dst.iterrows():
    if index == 0:
        valCum.append(df_dst['value'][index])
    elif df_dst['dst_identity'][index-1] == df_dst['dst_identity'][index]:
        valCum.append(valCum[index-1]+df_dst['value'][index])
    else:
        valCum.append(df_dst['value'][index])

df_dst['valeurCumul'] = valCum


duree =[]
begin = 0
for index, row in df_dst.iterrows():
    if index==0:
        duree.append(int(df_dst['time'][index])/86400 - int(df_dst['time'][begin])/86400)
    elif df_dst['dst_identity'][index-1] == df_dst['dst_identity'][index]:
        duree.append(int(df_dst['time'][index])/86400 - int(df_dst['time'][begin])/86400)
    else:
        begin = index
        duree.append(int(df_dst['time'][index])/86400 - int(df_dst['time'][begin])/86400)

df_dst['duree'] = duree

print(df_dst)




##
df_src = 0

res = spark.sql("SELECT src_identity, dst_identity, value, year, month, day, time " + 
                "FROM data " +
                "WHERE year<=2015 and src_identity in ('Huobi.com-2','Bitstamp.net') and dst_identity <> src_identity ORDER BY src_identity, time")


df_src = res.toPandas()

valCum = []
for index, row in df_src.iterrows():
    if index == 0:
        valCum.append(df_src['value'][index])
    elif df_src['src_identity'][index-1] == df_src['src_identity'][index]:
        valCum.append(valCum[index-1]+df_src['value'][index])
    else:
        valCum.append(df_src['value'][index])

df_src['valeurCumul'] = valCum

duree =[]
begin = 0
for index, row in df_src.iterrows():
    if index==0:
        duree.append(int(df_src['time'][index])/86400 - int(df_src['time'][begin])/86400)
    elif df_src['src_identity'][index-1] == df_src['src_identity'][index]:
        duree.append(int(df_src['time'][index])/86400 - int(df_src['time'][begin])/86400)
    else:
        begin = index
        duree.append(int(df_src['time'][index])/86400 - int(df_src['time'][begin])/86400)

df_src['duree'] = duree

print(df_src)

list1 = {}


for index, row in df_dst.iterrows():
    if index == 0:
        list1[df_dst['dst_identity'][index]]={'duree':[df_dst['duree'][index]], 'valeurCumul':[df_dst['valeurCumul'][index]]}
        
    elif df_dst['dst_identity'][index-1] == df_dst['dst_identity'][index]:
        list1[df_dst['dst_identity'][index]]['duree'].append(df_dst['duree'][index])
        list1[df_dst['dst_identity'][index]]['valeurCumul'].append(df_dst['valeurCumul'][index])
    else:
        list1[df_dst['dst_identity'][index]]={'duree':[df_dst['duree'][index]], 'valeurCumul':[df_dst['valeurCumul'][index]]}

list2 = {}
for index, row in df_src.iterrows():
    if index == 0:
        list2[df_src['src_identity'][index]]={'duree':[df_src['duree'][index]], 'valeurCumul':[df_src['valeurCumul'][index]]}
        
    elif df_src['src_identity'][index-1] == df_src['src_identity'][index]:
        list2[df_src['src_identity'][index]]['duree'].append(df_src['duree'][index])
        list2[df_src['src_identity'][index]]['valeurCumul'].append(df_src['valeurCumul'][index])
    else:
        list2[df_src['src_identity'][index]]={'duree':[df_src['duree'][index]], 'valeurCumul':[df_src['valeurCumul'][index]]}

for i in list1.keys():
    plt.figure(figsize=(5,5))
    plt.plot(list1[i]['duree'], list1[i]["valeurCumul"],"red", label = "gains " + i)
    plt.plot(list2[i]['duree'], list2[i]["valeurCumul"],"green",  label = "depense " + i)
    plt.savefig('EvolutionCapitale'+i+'Sato'+'.png')


############################## STEP 2 #####################################
# ### Recuperation des sommes entrantes par acteurs
# res = spark.sql("SELECT src_identity, dst_identity, valueUSD, year, month, day, time, begin,src_cat,dst_cat " + 
#                 "FROM data NATURAL JOIN (SELECT DISTINCT dst_identity, MIN(time) AS begin " + 
#                                         "FROM data " + 
#                                         "WHERE year<=2015 GROUP BY  dst_identity) " +
#                 "WHERE year<=2015  and dst_identity <> src_identity")
# res.createOrReplaceTempView('data_temp')

# def MontantNetActors(actors):
#     listActors = '('
#     for i in actors:
#         listActors = listActors+"'"+i+"'"+','
#     listActors = listActors[:-1]
#     listActors = listActors+")"
#     df_dst = 0
#     res = spark.sql("SELECT * " + 
#                     "FROM data_temp " +
#                     "WHERE dst_identity in "+listActors+" and time<=(begin+2592000) ORDER BY dst_identity, src_identity, time")

#     df_dst = res.toPandas()

#     # df_dst['begin'] = pd.to_numeric(df_dst['begin'])
#     # df_dst['time'] = pd.to_numeric(df_dst['time'])
#     # df_dst = df_dst[df_dst.time <= (df_dst.begin+2592000)]

#     # df_dst.reset_index(drop = True, inplace=True)

#     begin= {}
#     valCum = []
#     for index, row in df_dst.iterrows():
#         if index == 0:
#             valCum.append(df_dst['valueUSD'][index])
#             begin[df_dst['dst_identity'][index]] = df_dst['begin'][index]
#         elif df_dst['src_identity'][index-1] == df_dst['src_identity'][index] and df_dst['dst_identity'][index] == df_dst['dst_identity'][index-1]:
#             valCum.append(valCum[index-1]+df_dst['valueUSD'][index])
#         else:
#             begin[df_dst['dst_identity'][index]] = df_dst['begin'][index]
#             valCum.append(df_dst['valueUSD'][index])
#     df_dst['valeurCumul'] = valCum

#     list = {}
#     global list_tmp
#     list_tmp = {}
#     for index, row in df_dst.iterrows():
#         if index==0:
#             list[df_dst['dst_identity'][index]] = {df_dst['src_identity'][index]:df_dst['valeurCumul'][index]}
#             list_tmp[df_dst['dst_identity'][index]] = {df_dst['src_identity'][index]:[df_dst['valeurCumul'][index],df_dst["src_cat"][index]]}
#         elif df_dst['dst_identity'][index] == df_dst['dst_identity'][index-1]:
#             list[df_dst['dst_identity'][index]][df_dst['src_identity'][index]] = df_dst['valeurCumul'][index]
#             list_tmp[df_dst['dst_identity'][index]][df_dst['src_identity'][index]] = [df_dst['valeurCumul'][index],df_dst["src_cat"][index]]
#         else:
#             list[df_dst['dst_identity'][index]] = {df_dst['src_identity'][index]:df_dst['valeurCumul'][index]}
#             list_tmp[df_dst['dst_identity'][index]] = {df_dst['src_identity'][index]:[df_dst['valeurCumul'][index],df_dst["src_cat"][index]]}
#     #print(df_dst)
#     with open("finaldst.txt", "w") as f:
#         f.write(str(list))
# # # # ############################### STEP 3 #####################################
# # # #### Recuperation des sommes sortantes par acteurs
#     df_src = 0


#     res = spark.sql("SELECT src_identity, dst_identity, valueUSD, year, month, day, time, src_cat, dst_cat " + 
#                     "FROM data_temp " +
#                     "WHERE src_identity in "+listActors+" ORDER BY src_identity, dst_identity, time")

#     df_src = res.toPandas()

#     begin2 = []
#     for index, row in df_src.iterrows():
#         if df_src['src_identity'][index] in begin:
#             begin2.append(begin[df_src['src_identity'][index]])

#     df_src['begin']=begin2
#     df_src['begin'] = pd.to_numeric(df_src['begin'])
#     df_src['time'] = pd.to_numeric(df_src['time'])
#     df_src = df_src[df_src.time <= (df_src.begin+2592000)]

#     df_src.reset_index(drop = True, inplace=True)

#     valCum = []
#     for index, row in df_src.iterrows():
#         if index == 0:
#             valCum.append(df_src['valueUSD'][index])
#         elif df_src['src_identity'][index-1] == df_src['src_identity'][index] and df_src['dst_identity'][index-1] == df_src['dst_identity'][index]:
#             valCum.append(valCum[index-1]+df_src['valueUSD'][index])
#         else:
#             valCum.append(df_src['valueUSD'][index])
#     df_src['valeurCumul'] = valCum

#     list2 = {}
#     for index, row in df_src.iterrows():
#         if index==0:
#             list2[df_src['src_identity'][index]] = {df_src['dst_identity'][index]:df_src['valeurCumul'][index]}
#         elif df_src['src_identity'][index] == df_src['src_identity'][index-1]:
#             list2[df_src['src_identity'][index]][df_src['dst_identity'][index]] = df_src['valeurCumul'][index]
#         else:
#             list2[df_src['src_identity'][index]] = {df_src['dst_identity'][index]:df_src['valeurCumul'][index]}
#     with open("finalsrc.txt", "w") as f:
#         f.write(str(list2))
#     #print('deuxieme liste : ') 
#     #print(df_src)

# # # # ############################### STEP 4 #####################################
# # # # #### difference sortante - entrante
#     finalList = {}   
        
#     for i in list.keys():
#         finalList[i]={}
#         for j in list[i].keys():
#             if(j in list2[i].keys()):
#                 finalList[i][j]=list[i][j]-list2[i][j]
#             else:
#                 finalList[i][j]=list[i][j]
#     for i in finalList.keys():
#         finalList[i] = dict(sorted(finalList[i].items(),
#                             key=lambda item: item[1],
#                             reverse=True))
#     with open("final.txt", "w") as f:
#         f.write(str(finalList))
#     return finalList

# # # # ############################### STEP 5 #####################################
# # # # #### Visualisation
# # print("TRIE")
# rang = 1
# list= ['Bitstamp.net']

# nx_graph = nx.MultiDiGraph()
# grp = 1


# while(rang<=3):
#     finalList = MontantNetActors(list)
#     node = []
#     list=[]
#     for i in finalList.keys():
#         node.append(i)
#     for i in node:
#         sum = 0
#         counter = 0
#         grp+=1
#         for j in finalList[i].keys():
#             sum = sum+finalList[i][j]
#             nx_graph.add_node(i, title=str(sum), group=rang)
#             if finalList[i][j] >0:
#                 list.append(j)
#                 nx_graph.add_node(j,title=str(finalList[i][j])+" "+str(list_tmp[i][j][1]), group=grp) #size=finalList[i][j]/100000
#                 nx_graph.add_edge(j,i)
#                 counter+=1
#                 if counter == 3:
#                     break
#     rang+=1
#     print(list)

    

# # # nx_graph.add_node(21, size=15, title='couple', group=2)
# # # nx_graph.add_edge(20, 21, weight=10)
# # # nx_graph.add_edge(20, 21, weight=10)
# # # nx_graph.add_edge(20, 21, weight=10)
# # # nx_graph.add_node(25, size=10, label='lonely', title='lonely node', group=3)
# nt = Network('1000px', '1800px',directed = True)
# ## populates the nodes and edges data structures
# nt.from_nx(nx_graph)
# nt.toggle_physics(True)
# nt.show('nx2.html')