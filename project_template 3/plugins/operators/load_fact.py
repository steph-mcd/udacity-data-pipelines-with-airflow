from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Reads data from staging tables in redshift and inserts into songplays table
    
    :param redshift_conn_id: Redshift connection id
    :param table: songplays table name
    :param sql: name of insert sql query from SqlQueries
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",        
                 append_only = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Deleting from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
        self.log.info("Inserting into {} table".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)