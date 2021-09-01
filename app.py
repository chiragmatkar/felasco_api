import sys
import os
import simplejson as json
import pyodbc
import socket
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from threading import Lock
from tenacity import *
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.trace.samplers import ProbabilitySampler
import logging
import datetime
import decimal
from config import *
# Initialize Flask
app = Flask(__name__)

# Setup Azure Monitor
if 'APPINSIGHTS_KEY' in os.environ:
    middleware = FlaskMiddleware(
        app,
        exporter=AzureExporter(connection_string="InstrumentationKey={0}".format(os.environ['APPINSIGHTS_KEY'])),
        sampler=ProbabilitySampler(rate=1.0),
    )

# Setup Flask Restful framework
api = Api(app)
parser = reqparse.RequestParser()
parser.add_argument('customer')

# Implement singleton to avoid global objects
class ConnectionManager(object):    
    __instance = None
    __connection = None
    __lock = Lock()

    def __new__(cls):
        if ConnectionManager.__instance is None:
            ConnectionManager.__instance = object.__new__(cls)        
        return ConnectionManager.__instance       
    
    def __getConnection(self):
        if (self.__connection == None):
            application_name = ";APP={0}".format(socket.gethostname())
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER}};DATABASE={DB};UID={USR};PWD={PWD}"
            self.__connection = pyodbc.connect(connection_string + application_name)                  
        
        return self.__connection

    def __removeConnection(self):
        self.__connection = None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10), retry=retry_if_exception_type(pyodbc.OperationalError), after=after_log(app.logger, logging.DEBUG))
    def executeQueryJSON(self, procedure, payload=None):
        result = {}  
        try:
            conn = self.__getConnection()

            cursor = conn.cursor()
            
            if payload:
                cursor.execute(f"EXEC {procedure} ?", json.dumps(payload))
            else:
                cursor.execute(f"EXEC {procedure}")

            result = cursor.fetchone()

            columns = [column[0] for column in cursor.description]
            results = []
            formatted_list = []
            if result  != None and len(result) > 0:
                results.append(dict(zip(columns, result)))
                for r in results:
                    item = {}
                    for k,v in r.items():
                        if isinstance(v, (datetime.date, datetime.datetime)):
                            item[k] = v.isoformat()
                        elif isinstance(v, (decimal.Decimal)):
                            item[k] = str(v)
                        else:
                            item[k] = v
                    formatted_list.append(item)
            cursor.commit()    
        except pyodbc.OperationalError as e:            
            app.logger.error(f"{e.args[1]}")
            if e.args[0] == "08S01":
                # If there is a "Communication Link Failure" error, 
                # then connection must be removed
                # as it will be in an invalid state
                self.__removeConnection() 
                raise                        
        finally:
            cursor.close()
                         
        return formatted_list

class Queryable(Resource):
    def executeQueryJson(self, verb, payload=None):
        result = {}  
        entity = type(self).__name__.lower()
        procedure = f"{verb}_{entity}"
        result = ConnectionManager().executeQueryJSON(procedure, payload)
        return result

# Customer Class
class Athlete(Queryable):
    def get(self, athlete_id):
        result = self.executeQueryJson("get", int(athlete_id))   
        return result, 200
    
    def put(self):
        args = parser.parse_args()
        athlete = json.loads(args['athlete'])
        result = self.executeQueryJson("put", athlete)
        return result, 201

    def patch(self, customer_id):
        args = parser.parse_args()
        athlete = json.loads(args['athlete'])
        athlete["id"] = customer_id        
        result = self.executeQueryJson("patch", athlete)
        return result, 202

    def delete(self, athlete_id):       
        athlete = {}
        athlete["id"] = athlete_id
        result = self.executeQueryJson("delete", athlete)
        return result, 202

# Customers Class
class Athletes(Queryable):
    def get(self):     
        result = self.executeQueryJson("get")   
        return result, 200
    
# Create API routes
api.add_resource(Athlete, '/athlete', '/athlete/<athlete_id>')
api.add_resource(Athletes, '/athletes')