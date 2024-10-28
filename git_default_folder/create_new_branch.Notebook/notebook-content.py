# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MAGIC %%configure  
# MAGIC 
# MAGIC { 
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name": {
# MAGIC                   "parameterName": "defaultLakehouseName",
# MAGIC                   "defaultValue": "lh_dev"
# MAGIC         },
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run create_new_branch_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check if default lakehouse is set
if (mssparkutils.runtime.context['defaultLakehouseId']==None):
    displayHTML('<div style="display: flex; align-items: flex-end;"><img style="float: left; margin-right: 10px;" src="https://github.com/hurtn/images/blob/main/stop.png?raw=true" width="50"><span><h4>Please set a default lakehouse before proceeding</span><img style="float: right; margin-left: 10px;" src="https://github.com/hurtn/images/blob/main/stop.png?raw=true" width="50"></div>')
    print('\n')
    raise noDefaultLakehouseException('No default lakehouse found. Please add a lakehouse to this notebook.')
else: 
    print('Default lakehouse is set to '+ mssparkutils.runtime.context['defaultLakehouseName'] + '('+ mssparkutils.runtime.context['defaultLakehouseId'] + ')')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Store metadata of Capacities, Workspaces and Items
print(BranchCreationUtils.updateCp())
print(BranchCreationUtils.updateWs())
print(BranchCreationUtils.updateItems())
print(BranchCreationUtils.updateReports())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

verbose = False #flag to provide verbose error messages
client = fabric.FabricRestClient()

table_name = 'gitconnections'
spark.sql("Drop table if exists "+ table_name)
dfwks = spark.sql("SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights'").collect()
print("Storing git connection details for all workspaces.\nAny errors will appear below, some you can safely ignore such as the git connection status for the Capacity Metrics workspace...")
for idx,i in enumerate(dfwks):
    if i['Type'] == 'Workspace':
        url = "/v1/workspaces/" + i['ID'] + "/git/connection"
        try:
            print("Storing git details for workspace "+i['Name']+" ("+i['ID']+')...')
            response = client.get(url)

            gitProviderDetailsJSON = response.json()['gitProviderDetails']
            gitConnectionStateJSON = response.json()['gitConnectionState']
            gitSyncDetailsJSON = response.json()['gitSyncDetails']
            df = spark.createDataFrame([i['ID']],"string").toDF("Workspace_ID")
            df=df.withColumn("Workspace_Name",lit(i['Name'])).withColumn("gitConnectionState",lit(gitConnectionStateJSON)).withColumn("gitProviderDetails",lit(json.dumps(gitProviderDetailsJSON))).withColumn("gitSyncDetails",lit(json.dumps(gitSyncDetailsJSON)))

            if idx == 0:
                dfall = df
            else:
                dfall= dfall.union(df)
        except Exception as error:
            errmsg =  "Couldn't get git connection status for workspace " + i['Name'] + "("+ i['ID'] + ")."
            if (verbose):
                 errmsg = errmsg + "Error: "+str(error)
            print(str(errmsg))

dfall.withColumn("metaTimestamp",current_timestamp()).write.mode('overwrite').option("mergeSchema", "true").saveAsTable(table_name)
print('Done')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws_id = mssparkutils.runtime.context['currentWorkspaceId']
print(ws_id)
df = spark.sql(f"SELECT * FROM gitconnections where workspace_id ='{ws_id}'")
df.show()
print("oi")
gitsql = "select gt.gitConnectionState,gt.gitProviderDetails, wks.name Workspace_Name, wks.id Workspace_ID from gitconnections gt " \
         "inner join workspaces wks on gt.Workspace_Name = wks.name " \
         f"where gt.gitConnectionState = 'ConnectedAndInitialized' and wks.id = '{ws_id}'" 

dfgitdetails = spark.sql(gitsql).collect()
display(dfgitdetails)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM gitconnections")
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.runtime.context['currentWorkspaceId']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
