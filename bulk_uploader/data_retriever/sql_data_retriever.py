import pyodbc
import pandas as pd
import sys

class SQLDataRetriever:
    """
    Retrieves the bulk Layer data and Loss data from SQL 
    and loads it in a DataFrame.
    
    """

    def establish_connection(self):
        validation = self.sql_config.validation
        driver = self.sql_config.driver
        server = self.sql_config.server
        database = self.sql_config.database

        try:
            self.connection = pyodbc.connect(driver=driver,
                                server=server,
                                database=database,
                                trusted_connection='yes')
        except Exception as e:
            print('Error occured while establishing connection to database: {}'.format(e))

    def get_bulk_layer_df(self):
        layer_query_str = """
                SELECT Analysis_Item_ID AS LayerID,
                CASE WHEN Layer_Name LIKE '%AGG%' THEN 'AggXL' ELSE 'CatXL' END AS LayerType,                   
                CONCAT(CAST(Analysis_Item_ID AS VARCHAR(10)), ' ', ISNULL(Layer_Name,'')) AS Description,
                ISNULL(Class,'USCAT_SupReg') AS Descr,
                Part as Participation,
                CUR AS Currency,
                Premium AS Premium,
                CONVERT(varchar, Inception_Date, 110) AS "In-Force Period Begin",
                CONVERT(varchar, Expiration_Date, 110) AS "In-Force Period End",
                ISNULL(Occurrence_Deductible,0) AS Attachment,
                ISNULL(Aggregate_Deductible,0) AS AggAttachment,
                CASE WHEN Occurrence_Limit = -1 THEN Aggregate_Limit ELSE ISNULL(Occurrence_Limit,0) END AS Limit,
                CASE WHEN Aggregate_Limit IS NULL AND Layer_Name LIKE '%agg%' THEN Occurrence_Limit 
                        WHEN Aggregate_Limit IS NULL AND Layer_Name NOT LIKE '%agg%' THEN CAST(ISNULL(LEN(CONTRACTS.Reinstatement)- LEN(REPLACE(CONTRACTS.Reinstatement, '@', '')) + 1 ,0) AS FLOAT)*Occurrence_Limit
                                ELSE Aggregate_Limit END AS AggregateLimit,

                ISNULL(Franchise_Deductible,0) AS Franchise,
                ISNULL(REINSTATEMENT_FORMAT.ARE_REINSTATEMENTS,'') AS Reinstatements,
                '1/1/2022' AS "LossSet StartDate",
                CUR AS "LossSet Currency"
                FROM CONTRACTS LEFT JOIN REINSTATEMENT_FORMAT ON REINSTATEMENT_FORMAT.REINSTATEMENT=CONTRACTS.Reinstatement AND REINSTATEMENT_FORMAT.REINSTATEMENTBRKGPCT=CONTRACTS.Reinstatement_Brkg
                ORDER BY Analysis_Item_ID
        """

        layers_df = pd.read_sql_query(layer_query_str, con=self.connection)
       
        return layers_df

    def get_bulk_loss_df(self):
        loss_query_stry = """
        SELECT  L.AnalysisItemID AS LayerID, 
                   Year AS Trial, 
                   (DayOfYear - 1) AS Sequence, 
                   EventID AS Event, 
                   SUM(Ceded) AS Loss, 
                   CASE WHEN MAX(Initial_Premium) > 0 THEN SUM(CededReinstatementPremium)/MAX(Initial_Premium) ELSE 0 END AS ReinstatementPremiumPct,
                   CASE WHEN MAX(Initial_Premium) > 0 THEN SUM(RIP_AP_Brokerage + RIP_AP_Tax)/MAX(Initial_Premium)  ELSE 0 END AS ReinstatementBrokeragePct
        FROM LOSSES L INNER JOIN
        CONTRACTS C ON L.AnalysisItemID = C.Analysis_Item_ID 
        WHERE YEAR <> 0 
        GROUP BY L.AnalysisItemID, Year, DayOfYear, EventID
        ORDER BY L.AnalysisItemID, YEAR, DayOfYear
        """

        loss_df = pd.read_sql_query(loss_query_stry, con=self.connection)

        return loss_df

    def get_bulk_data(self):
        try:
            self.establish_connection()
            layer_df = self.get_bulk_layer_df()
            loss_df = self.get_bulk_loss_df()

            return layer_df, loss_df
        except Exception as e:
            sys.exit('Error while retrieving data from database: {}'.format(e))

    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.sql_config = self.config_parser.get_sql_config()
        self.connection = None
