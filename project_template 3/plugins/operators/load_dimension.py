from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    Reads data from staging tables in redshift and inserts into songplays table
    
    :param redshift_conn_id: Redshift connection id
    :param table: name of dimension table
    :param sql: name of insert sql query from SqlQueries
    :param primary_key: this is for when we are using append only - this is teh column used to check if the row already exists
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 append_only = "",
                 primary_key = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only
        self.primary_key = primary_key

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_only:
            if self.append_insert:
            table_insert_sql = f"""
                create temp table stage_{self.table} (like {self.table}); 
                
                insert into stage_{self.table}
                {self.select_sql};
                
                delete from {self.table}
                using stage_{self.table}
                where {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
                
                insert into {self.table}
                select * from stage_{self.table};
            """
        else:
            self.log.info("Deleting from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))        
        self.log.info("Inserting into {} table".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)
