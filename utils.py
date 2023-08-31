import os
import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Log Data").getOrCreate()

class Logger:
    def __init__(self):
        # Initialize instance variables
        self.server = os.getenv("server")  # safer than os.environ[]
        self.database = os.getenv("database")
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.port = os.getenv("port")

    def _get_widgets_parameters(self):
        """
        Set parameters from widgets with default values  
        """

        widget_defaults = {
            'datafactory_name': 'default_datafactory_name',
            'pipeline_name': 'default_pipeline_name',
            'pipelinerunid': 'default_pipelinerunid',
            'trigger_type': 'default_trigger_type',
            'trigger_id': 'default_trigger_id',
            'trigger_name': 'default_trigger_name',
            'trigger_time': '2000-01-01T00:00:00.00Z'
        }

        for widget, default_value in widget_defaults.items():
            try:
                setattr(self, widget, dbutils.widgets.get(widget))
            except:
                setattr(self, widget, default_value)

        self.trigger_time = datetime.datetime.strptime(self.trigger_time.replace('Z', '')[:-1], '%Y-%m-%dT%H:%M:%S.%f')
    
    def connect(self):
        """
        Connect to the SQL Server database
        """
        try:
            # Construct the JDBC URL with connection parameters
            self.jdbc_url = f"jdbc:sqlserver://{self.server}:{self.port};database={self.database};user={self.user};password={self.password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

            # Fetch the driver manager from your spark context
            self.driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager

            # Create a connection object using a jdbc-url
            self.connection = self.driver_manager.getConnection(self.jdbc_url)

            print('Connection Successful')

            # Call _get_parameters_from_widgets() after connection is established
            self._get_widgets_parameters()  
        except Exception as e:
            print(f"An unexpected error occurred: {e.java_exception.getMessage()}")

    def log_pipeline(self, user_friendly_name, correlation_id, status):
        """
        Logs the pipeline execution details in the SQL Server database using log_pipeline stored procedure.
        """
        try:
            # Write your SQL statement as a string
            sql = "EXEC log_pipeline @user_friendly_name=?, @datafactory_name=?, @pipeline_name=?, @pipelinerunid=?, " \
                "@correlation_id=?, @duration=?, @trigger_type=?, @trigger_id=?, @trigger_name=?, @trigger_time=?," \
                "@start_time=?, @end_time=?, @status=?;"
            
            if status == "In Progress":
                self.start_time1 = datetime.datetime.now()
                start_time1 = self.start_time1
                end_time1 = None
                duration1 = None
            elif status == "Success":
                end_time1 = datetime.datetime.now()
                duration_diff1 = end_time1 - self.start_time1
                duration1 = str(duration_diff1)
                start_time1 = self.start_time1

            # Create callable statement and execute it
            self.statement = self.connection.prepareStatement(sql)

            # Set the values of the parameters
            self.statement.setString(1, user_friendly_name)
            self.statement.setString(2, self.datafactory_name)
            self.statement.setString(3, self.pipeline_name)
            self.statement.setString(4, self.pipelinerunid)
            self.statement.setString(5, correlation_id)
            self.statement.setString(6, duration1)
            self.statement.setString(7, self.trigger_type)
            self.statement.setString(8, self.trigger_id)
            self.statement.setString(9, self.trigger_name)
            self.statement.setTimestamp(10, self.trigger_time)
            self.statement.setTimestamp(11, start_time1)
            self.statement.setTimestamp(12, end_time1)
            self.statement.setString(13, status)
            
            # Execute the stored procedure
            self.statement.execute()
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")


    def log_pipeline_activity(self, user_friendly_name, cluster_name, activity_runid, 
                              activity_name, source_type, sink_type, status):
        """
        Logs the pipeline execution details in the SQL Server database using log_pipeline_activity stored procedure.
        """
        try:
            # Write your SQL statement as a string
            sql = "EXEC log_pipeline_activity @user_friendly_name=?, @cluster_name=?, @duration=?, @activity_runid=?," \
                "@pipeline_runid=?, @pipeline_name=?, @activity_name=?, @start_time=?, @end_time=?, @source_type=?," \
                "@sink_type=?, @status=?;"

            # Create callable statement and execute it
            self.statement = self.connection.prepareStatement(sql)

            if status == "In Progress":
                self.start_time2 = datetime.datetime.now()
                start_time2 = self.start_time2
                end_time2 = None
                duration2 = None
            elif status == "Success":
                end_time2 = datetime.datetime.now()
                duration_diff2 = end_time2 - self.start_time2
                duration2 = str(duration_diff2)
                start_time2 = self.start_time2
                
            # Set the values of the parameters
            self.statement.setString(1, user_friendly_name)
            self.statement.setString(2, cluster_name)
            self.statement.setString(3, duration2) 
            self.statement.setString(4, activity_runid)
            self.statement.setString(5, self.pipelinerunid)
            self.statement.setString(6, self.pipeline_name)
            self.statement.setString(7, activity_name)
            self.statement.setTimestamp(8, start_time2)  
            self.statement.setTimestamp(9, end_time2)  
            self.statement.setString(10, source_type)
            self.statement.setString(11, sink_type)
            self.statement.setString(12, status)

            # Execute the stored procedure
            self.statement.execute()
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")


    def close_connection(self):
        """
        Close the connection to the SQL Server database.
        """

        # Check if the connection attribute exists and if it is not None
        if hasattr(self, 'connection') and self.connection is not None:
            self.connection.close()
            self.connection = None
            print('Connection Closed')