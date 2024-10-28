# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

#lim import
from notebookutils import mssparkutils
import pandas as pd
import datetime
import re,json
import sempy
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,current_timestamp,lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client = fabric.FabricRestClient()

# get the current workspace ID based on the context of where this notebook is run from
thisWsId = mssparkutils.runtime.context['currentWorkspaceId'] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class BranchCreationUtils:
    @staticmethod
    def saveTable(pdf,table_name, mode='overwrite'):
        if mode=='append' and not any(table.name == table_name for table in spark.catalog.listTables()):
                mode = 'overwrite'

        if (isinstance(pdf, pd.DataFrame) and pdf.empty) or \
        (isinstance(pdf, DataFrame) and pdf.isEmpty()):
            return('No ' + table_name + ' found, nothing to save (Dataframe is empty)')
        if not isinstance(pdf, DataFrame):
            pdf = spark.createDataFrame(pdf)

        df = pdf.select([col(c).alias(
                c.replace( '(', '')
                .replace( ')', '')
                .replace( ',', '')
                .replace( ';', '')
                .replace( '{', '')
                .replace( '}', '')
                .replace( '\n', '')
                .replace( '\t', '')
                .replace( ' ', '_')
                .replace( '.', '_')
            ) for c in pdf.columns])
        #display(df)
        df.withColumn("metaTimestamp",current_timestamp()).write.mode(mode) \
        .option("mergeSchema", "true").saveAsTable(table_name)
        return(str(df.count()) +' records saved to the '+table_name + ' table.')

    @staticmethod
    def updateCp():
        spark.sql("drop table if exists capacitiess")
        df = fabric.list_capacities()
        return BranchCreationUtils.saveTable(df,"capacities")
        #print('Capacities table reloaded')

    @staticmethod
    def updateWs():
        spark.sql("drop table if exists workspaces")
        df = fabric.list_workspaces()
        #display(df)
        return BranchCreationUtils.saveTable(df,"workspaces")
        #print('Workspaces table reloaded')

    @staticmethod
    def updateItems():
        all_items = []
        spark.sql("drop table if exists items")
        updateResults = BranchCreationUtils.updateWs()
        df = spark.sql("SELECT distinct ID,Type,Name FROM workspaces").collect()

        for i in df:
            print('Getting items for workspace ' + i['Name'] + '...')
            if i['Type'] == 'Workspace':
                url = "/v1/workspaces/" + i['ID'] + "/items"
            try:
                itmresponse = client.get(url)
                print(itmresponse.json()) 
                all_items.extend(itmresponse.json()['value']) 
            except Exception as error:
                errmsg =  "Couldn't get list of items for workspace " + i['Name'] + "("+ i['ID'] + ")."
                if (verbose):
                    errmsg = errmsg + "Error: "+str(error)
                print(str(errmsg))
        itmdf=spark.read.json(sc.parallelize(all_items))
        print(BranchCreationUtils.saveTable(itmdf,'items'))

    @staticmethod
    def updateReports(only_secondary=False):
        all_report_data = []
        table_name = 'reports'
        spark.sql("Drop table if exists "+ table_name)
        reportsql = "SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights'"
        if only_secondary:
            reportsql = reportsql + " and Name like '%"+secondary_ws_suffix+"'" 
        reportdf = spark.sql(reportsql).collect()

        for idx,i in enumerate(reportdf):
            if i['Type'] == 'Workspace':
                try:
                    print('Retreiving reports for workspace '+ i['Name'] + '...')
                    dfwsreports = fabric.list_reports(i['ID'] )   
                    if idx == 0:
                            dfallreports = dfwsreports
                    else:
                            dfallreports = pd.concat([dfallreports, dfwsreports], ignore_index=True, sort=False)
                except WorkspaceNotFoundException as e:
                    print("WorkspaceNotFoundException:", e)
                except FabricHTTPException as e:
                    print("Caught a FabricHTTPException. Check the API endpoint, authentication.")

                except Exception as error:
                    errmsg =  "Couldn't retreive report details for workspace " + i['Name'] + "("+ i['ID'] + "). Error: "+str(error)
                    print(str(errmsg))

        dfallreports = dfallreports.drop('Subscriptions', axis=1)
        dfallreports = dfallreports.drop('Users', axis=1)
        print(BranchCreationUtils.saveTable(dfallreports,'reports'))

    @staticmethod
    def get_lh_object_list(base_path,data_types = ['Tables', 'Files'])->pd.DataFrame:

        '''
        Function to get a list of tables for a lakehouse
        adapted from https://fabric.guru/getting-a-list-of-folders-and-delta-tables-in-the-fabric-lakehouse
        This function will return a pandas dataframe containing names and abfss paths of each folder for Files and Tables
        '''
        #data_types = ['Tables', 'Files'] #for if you want a list of files and tables
        #data_types = ['Tables'] #for if you want a list of tables

        df = pd.concat([
            pd.DataFrame({
                'name': [item.name for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
                'type': data_type[:-1].lower() , 
                'src_path': [item.path for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
            }) for data_type in data_types], ignore_index=True)

        return df

    @staticmethod
    def getItemId(wks_id,itm_name,itm_type):
        df = fabric.list_items(type=None,workspace=wks_id)
        #print(df)
        if df.empty:
            #print('No items found in workspace '+i['Name']+' (Dataframe is empty)')
            return 'NotExists'
        else:
            #display(df)
            #print(df.query('"Display Name"="'+itm_name+'"'))
            if itm_type != '':
                newdf= df.loc[(df['Display Name'] == itm_name) & (df['Type'] == itm_type)]['Id']
            else:
                newdf= df.loc[(df['Display Name'] == itm_name)]['Id']  
            if newdf.empty:
                return 'NotExists'
            else:
                return newdf.iloc[0]
                
    @staticmethod
    def getWorkspaceRolesAssignments(pWorkspaceId):
        url = "/v1/workspaces/" +pWorkspaceId + "/roleAssignments"
        try:
            print('Attempting to connect workspace '+ i['Workspace_Name'])
            response = client.post(url,json= json.loads(payload))
            print(str(response.status_code) + response.text) 
            success = True
        except Exception as error:
            errmsg =  "Couldn't connect git to workspace " + i['Workspace_Name'] + "("+ i['Workspace_ID'] + "). Error: "+str(error)
            print(str(errmsg))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
