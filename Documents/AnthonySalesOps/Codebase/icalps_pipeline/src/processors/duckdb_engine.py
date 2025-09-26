"""
DuckDB Processing Engine for IC'ALPS Pipeline
Handles data transformations, joins, and aggregations
"""

import duckdb
import pandas as pd
import logging
from typing import Dict, Optional, Any, List
from pathlib import Path
from config.database_config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBProcessor:
    """DuckDB processing engine for data transformations"""

    def __init__(self):
        self.config = config
        self.connection = None
        self.registered_tables = {}

    def connect(self) -> bool:
        """Establish DuckDB connection"""
        try:
            db_config = self.config.duckdb_config
            self.connection = duckdb.connect(db_config['database_path'])

            # Configure DuckDB settings
            self.connection.execute(f"SET memory_limit='{db_config['memory_limit']}'")
            self.connection.execute(f"SET threads={db_config['threads']}")
            self.connection.execute(f"SET temp_directory='{db_config['temp_directory']}'")

            logger.info("DuckDB connection established successfully")
            return True

        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {str(e)}")
            return False

    def register_dataframe(self, table_name: str, df: pd.DataFrame) -> bool:
        """Register DataFrame as DuckDB table"""
        try:
            if self.connection is None:
                if not self.connect():
                    return False

            self.connection.register(table_name, df)
            self.registered_tables[table_name] = len(df)
            logger.info(f"Registered table '{table_name}' with {len(df)} records")
            return True

        except Exception as e:
            logger.error(f"Error registering DataFrame '{table_name}': {str(e)}")
            return False

    def execute_query(self, query: str, return_df: bool = True) -> Optional[pd.DataFrame]:
        """Execute SQL query and optionally return DataFrame"""
        try:
            if self.connection is None:
                if not self.connect():
                    return None

            if return_df:
                result = self.connection.execute(query).df()
                logger.info(f"Query executed successfully, returned {len(result)} rows")
                return result
            else:
                self.connection.execute(query)
                logger.info("Query executed successfully")
                return None

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            return None

    def register_bronze_tables(self, bronze_data: Dict[str, pd.DataFrame]) -> bool:
        """Register all Bronze layer tables"""
        try:
            for entity_type, df in bronze_data.items():
                table_name = self.config.get_bronze_table_name(entity_type)
                if not self.register_dataframe(table_name, df):
                    return False

            logger.info(f"Successfully registered {len(bronze_data)} Bronze tables")
            return True

        except Exception as e:
            logger.error(f"Error registering Bronze tables: {str(e)}")
            return False

    def create_companies_view(self) -> bool:
        """Create processed companies view with deduplication"""
        query = """
        CREATE OR REPLACE VIEW Processed_Companies AS
        SELECT DISTINCT
            Comp_CompanyId,
            Comp_Name,
            Comp_Website,
            bronze_extracted_at,
            bronze_source_file
        FROM Bronze_companies
        WHERE Comp_CompanyId IS NOT NULL
        ORDER BY Comp_CompanyId
        """
        return self.execute_query(query, return_df=False) is not None

    def create_persons_view(self) -> bool:
        """Create processed persons view with company associations"""
        query = """
        CREATE OR REPLACE VIEW Processed_Persons AS
        SELECT DISTINCT
            p.Pers_PersonId,
            p.Pers_FirstName,
            p.Pers_LastName,
            p.Pers_EmailAddress,
            p.email_valid,
            p.Comp_CompanyId,
            c.Comp_Name,
            p.bronze_extracted_at,
            p.bronze_source_file
        FROM Bronze_persons p
        LEFT JOIN Processed_Companies c ON p.Comp_CompanyId = c.Comp_CompanyId
        WHERE p.Pers_PersonId IS NOT NULL
        ORDER BY p.Pers_PersonId
        """
        return self.execute_query(query, return_df=False) is not None

    def create_opportunities_view(self) -> bool:
        """Create processed opportunities view with all associations"""
        query = """
        CREATE OR REPLACE VIEW Processed_Opportunities AS
        SELECT
            o.Oppo_OpportunityId,
            o.Oppo_Description,
            o.Oppo_PrimaryCompanyId,
            o.Oppo_PrimaryPersonId,
            o.Oppo_AssignedUserId,
            o.Oppo_Type,
            o.Oppo_Product,
            o.Oppo_Source,
            o.Oppo_Note,
            o.Oppo_CustomerRef,
            o.Oppo_Status,
            o.Oppo_Stage,
            o.Oppo_Forecast,
            o.Oppo_Certainty,
            o.Oppo_Priority,
            o.Oppo_TargetClose,
            o.Oppo_CreatedDate,
            o.Oppo_UpdatedDate,
            o.Oppo_Total,
            o.oppo_cout,
            c.Comp_Name,
            p.Pers_FirstName,
            p.Pers_LastName,
            p.Pers_EmailAddress,
            o.bronze_extracted_at,
            o.bronze_source_file
        FROM Bronze_opportunities o
        LEFT JOIN Processed_Companies c ON o.Oppo_PrimaryCompanyId = c.Comp_CompanyId
        LEFT JOIN Processed_Persons p ON o.Oppo_PrimaryPersonId = p.Pers_PersonId
        WHERE o.Oppo_OpportunityId IS NOT NULL
        ORDER BY o.Oppo_OpportunityId
        """
        return self.execute_query(query, return_df=False) is not None

    def create_communications_view(self) -> bool:
        """Create processed communications view with entity associations"""
        query = """
        CREATE OR REPLACE VIEW Processed_Communications AS
        SELECT
            cm.Comm_CommunicationId,
            cm.Comm_Subject,
            cm.Comm_From,
            cm.Comm_TO,
            cm.Comm_DateTime,
            cm.comm_type,
            cm.Oppo_OpportunityId,
            cm.Pers_PersonId,
            cm.Comp_CompanyId,
            o.Oppo_Description,
            p.Pers_FirstName,
            p.Pers_LastName,
            c.Comp_Name,
            cm.bronze_extracted_at,
            cm.bronze_source_file
        FROM Bronze_communications cm
        LEFT JOIN Processed_Opportunities o ON cm.Oppo_OpportunityId = o.Oppo_OpportunityId
        LEFT JOIN Processed_Persons p ON cm.Pers_PersonId = p.Pers_PersonId
        LEFT JOIN Processed_Companies c ON cm.Comp_CompanyId = c.Comp_CompanyId
        WHERE cm.Comm_CommunicationId IS NOT NULL
        ORDER BY cm.Comm_CommunicationId
        """
        return self.execute_query(query, return_df=False) is not None

    def create_social_networks_view(self) -> bool:
        """Create processed social networks view"""
        query = """
        CREATE OR REPLACE VIEW Processed_Social_Networks AS
        SELECT
            sn.sone_networklink,
            sn.Related_TableID,
            sn.Related_RecordID,
            sn.bord_caption,
            sn.network_type,
            CASE
                WHEN sn.Related_TableID = 5 THEN c.Comp_Name
                WHEN sn.Related_TableID = 13 THEN CONCAT(p.Pers_FirstName, ' ', p.Pers_LastName)
                ELSE 'Unknown'
            END as entity_name,
            sn.bronze_extracted_at,
            sn.bronze_source_file
        FROM Bronze_social_networks sn
        LEFT JOIN Processed_Companies c ON sn.Related_TableID = 5 AND sn.Related_RecordID = c.Comp_CompanyId
        LEFT JOIN Processed_Persons p ON sn.Related_TableID = 13 AND sn.Related_RecordID = p.Pers_PersonId
        WHERE sn.sone_networklink IS NOT NULL AND sn.sone_networklink != ''
        ORDER BY sn.Related_TableID, sn.Related_RecordID
        """
        return self.execute_query(query, return_df=False) is not None

    def create_all_views(self) -> bool:
        """Create all processed views"""
        views = [
            ('Companies', self.create_companies_view),
            ('Persons', self.create_persons_view),
            ('Opportunities', self.create_opportunities_view),
            ('Communications', self.create_communications_view),
            ('Social Networks', self.create_social_networks_view)
        ]

        success_count = 0
        for view_name, view_func in views:
            try:
                if view_func():
                    logger.info(f"Successfully created {view_name} view")
                    success_count += 1
                else:
                    logger.error(f"Failed to create {view_name} view")
            except Exception as e:
                logger.error(f"Error creating {view_name} view: {str(e)}")

        logger.info(f"Created {success_count}/{len(views)} views successfully")
        return success_count == len(views)

    def get_table_summary(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get summary statistics for a table"""
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT *) as unique_rows
        FROM {table_name}
        """
        result = self.execute_query(query)
        if result is not None and len(result) > 0:
            return result.iloc[0].to_dict()
        return None

    def get_all_table_summaries(self) -> Dict[str, Dict[str, Any]]:
        """Get summaries for all registered tables"""
        summaries = {}
        for table_name in self.registered_tables.keys():
            summary = self.get_table_summary(table_name)
            if summary:
                summaries[table_name] = summary
        return summaries

    def export_to_csv(self, table_name: str, output_path: str) -> bool:
        """Export table to CSV file"""
        try:
            query = f"SELECT * FROM {table_name}"
            df = self.execute_query(query)
            if df is not None:
                df.to_csv(output_path, index=False)
                logger.info(f"Exported {table_name} to {output_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error exporting {table_name}: {str(e)}")
            return False

    def close_connection(self):
        """Close DuckDB connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.registered_tables = {}
            logger.info("DuckDB connection closed")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_connection()

# Global processor instance
duckdb_processor = DuckDBProcessor()