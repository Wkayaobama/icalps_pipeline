



1. **Extract schema and connect to db**
-- Convert PowerShell CRM field extraction pattern to dynamic SQL
DECLARE @sql NVARCHAR(MAX), 
        @ExcludedTables NVARCHAR(MAX) 
        = N'BaseGESCOM%,EWSMailbox%,EWMA_%,sys%,INFORMATION_SCHEMA%,temp%,backup%,log%',
        
        @TargetTables NVARCHAR(MAX) 
        = N'comm_%,pers_%,oppo_%,comp_%,case_%',
        
        @QueryTemplate NVARCHAR(MAX) 
        = N'SELECT 
    t.TABLE_NAME as TableName,
    c.COLUMN_NAME as FieldName,
    CASE 
        WHEN cc.Capt_US IS NOT NULL THEN cc.Capt_US
        ELSE c.COLUMN_NAME
    END as FieldLabel,
    c.DATA_TYPE as FieldType,
    CASE 
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND c.CHARACTER_MAXIMUM_LENGTH != -1 
        THEN CAST(c.CHARACTER_MAXIMUM_LENGTH as VARCHAR(10))
        WHEN c.DATA_TYPE IN (''int'', ''bigint'', ''smallint'', ''tinyint'') THEN ''Integer''
        WHEN c.DATA_TYPE IN (''decimal'', ''numeric'', ''float'', ''real'', ''money'') THEN ''Decimal''
        WHEN c.DATA_TYPE IN (''datetime'', ''datetime2'', ''date'', ''time'') THEN ''DateTime''
        WHEN c.DATA_TYPE = ''bit'' THEN ''Boolean''
        ELSE ''Text''
    END as FieldSize,
    CASE WHEN c.IS_NULLABLE = ''YES'' THEN ''True'' ELSE ''False'' END as IsNullable,
    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN ''True'' ELSE ''False'' END as IsPrimaryKey,
    CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN ''True'' ELSE ''False'' END as IsForeignKey,
    fk.REFERENCED_TABLE_NAME as ReferencedTable,
    fk.REFERENCED_COLUMN_NAME as ReferencedColumn,
    c.COLUMN_DEFAULT as DefaultValue,
    c.ORDINAL_POSITION as ColumnOrder,
    ''%table_pattern%'' as MatchedPattern
FROM 
    INFORMATION_SCHEMA.TABLES t
INNER JOIN 
    INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN 
    Custom_Captions cc ON LOWER(c.COLUMN_NAME) = LOWER(REPLACE(cc.Capt_US, '' '', ''_''))
LEFT JOIN (
    SELECT 
        ku.TABLE_NAME,
        ku.COLUMN_NAME
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    INNER JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
    WHERE 
        tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
) pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
LEFT JOIN (
    SELECT 
        fkc.PARENT_OBJECT_ID,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS COLUMN_NAME,
        OBJECT_NAME(fkc.parent_object_id) AS TABLE_NAME,
        OBJECT_NAME(fkc.referenced_object_id) AS REFERENCED_TABLE_NAME,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS REFERENCED_COLUMN_NAME
    FROM 
        sys.foreign_key_columns fkc
) fk ON c.TABLE_NAME = fk.TABLE_NAME AND c.COLUMN_NAME = fk.COLUMN_NAME
WHERE 
    t.TABLE_TYPE = ''BASE TABLE''
    AND t.TABLE_NAME LIKE ''%table_pattern%''
    AND NOT EXISTS (
        SELECT 1 FROM STRING_SPLIT(''' + @ExcludedTables + ''', '','') 
        WHERE t.TABLE_NAME LIKE TRIM(value)
    )
ORDER BY 
    t.TABLE_NAME, c.ORDINAL_POSITION';

-- Generate dynamic SQL for each target table pattern
SELECT @sql = STRING_AGG(REPLACE(@QueryTemplate, '%table_pattern%', TRIM(value)), 
       CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10))     
FROM STRING_SPLIT(@TargetTables, ',');

-- Add final ORDER BY for complete result set
SET @sql = @sql + CHAR(13) + CHAR(10) + 'ORDER BY TableName, ColumnOrder;';

-- Display generated SQL
PRINT 'Generated Dynamic SQL:';
PRINT '=====================';
PRINT LEFT(@sql, 4000); -- Print first 4000 chars due to PRINT limitations

-- Execute the dynamic query
PRINT CHAR(13) + CHAR(10) + 'Executing dynamic query...';
EXEC sys.sp_executesql @sql;

-- Optional: Export results using BCP
DECLARE @BCPCommand NVARCHAR(500) = 
    'bcp "' + REPLACE(@sql, '"', '""') + '" queryout "{OUTPUT_PATH}\CRM_Field_Definitions.csv" -c -t"," -T -S"(localdb)\MSSQLLocalDB" -d"CRMICALPS"';

PRINT CHAR(13) + CHAR(10) + 'BCP Export Command:';
PRINT REPLACE(@BCPCommand, '{OUTPUT_PATH}', 'C:\Users\{USERNAME}\Documents\AnthonySalesOps\Codebase\SalesBrain');



###connection

$ServerName = "(localdb)\MSSQLLocalDB"     # SQL Server LocalDB instance
$DatabaseName = "CRMICALPS"                # Target database name
$ConnectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=true;"

1. PRocess legacy
Unified Deals Processor with Pipeline Differentiation

Merges Studies and Deals into single Deal object using Pipeline helper column

"""

  

import pandas as pd

import duckdb

from pathlib import Path

import logging

from datetime import datetime

  

class UnifiedDealsProcessor:

    def __init__(self):

        self.conn = duckdb.connect(':memory:')

        self.setup_logging()

    def setup_logging(self):

        logging.basicConfig(level=logging.INFO)

        self.logger = logging.getLogger(__name__)

  

    def process_legacy_data(self, file_paths):

        """

        Process legacy data into unified Deal objects

        Output: CSV files ready for HubSpot import with Pipeline differentiation

        """

        # Load raw data

        raw_data = self.load_raw_files(file_paths)

        # Transform data with unified approach

        transformed_data = self.transform_unified_data(raw_data)

        # Export for HubSpot import

        self.export_for_hubspot_import(transformed_data)

        return transformed_data

    def load_raw_files(self, file_paths):

        """Load and register raw CSV files"""

        data = {}

        # Load opportunities data

        if 'opportunities' in file_paths:

            oppo_df = pd.read_csv(file_paths['opportunities'])

            self.conn.register('raw_opportunities', oppo_df)

            data['opportunities'] = oppo_df

            self.logger.info(f"Loaded {len(oppo_df)} opportunity records")

        # Load companies data

        if 'companies' in file_paths:

            comp_df = pd.read_csv(file_paths['companies'])

            self.conn.register('raw_companies', comp_df)

            data['companies'] = comp_df

            self.logger.info(f"Loaded {len(comp_df)} company records")

        # Load persons data

        if 'persons' in file_paths:

            pers_df = pd.read_csv(file_paths['persons'])

            self.conn.register('raw_persons', pers_df)

            data['persons'] = pers_df

            self.logger.info(f"Loaded {len(pers_df)} person records")

        return data

    def transform_unified_data(self, raw_data):

        """Transform raw data into unified Deal objects with Pipeline differentiation"""

        self.logger.info("Starting unified data transformation...")

        # Log the unified approach

        self.logger.info("Unified Object Strategy:")

        self.logger.info("   - All records become 'Deals' in HubSpot")

        self.logger.info("   - Pipeline 'Studies Pipeline': Oppo_Type = 'Preetude'")

        self.logger.info("   - Pipeline 'Sales Pipeline': Oppo_Type <> 'Preetude' AND Forecast <> 0")

        self.logger.info("   - Deal metadata structure used for both types")

        # Transform ALL opportunities into unified Deals with Pipeline differentiation

        unified_deals_query = """

        SELECT

            Oppo_OpportunityId as "Record ID",

            Oppo_Description as "Deal Name",

            Oppo_AssignedUserId as "Deal Owner",

            -- Pipeline Helper Column (MAIN DIFFERENTIATOR)

            CASE

                WHEN Oppo_Type = 'Preetude' THEN 'Studies Pipeline'

                ELSE 'Sales Pipeline'

            END as "Pipeline",

            -- Deal Stage (Different mapping based on pipeline)

            CASE

                -- Studies Pipeline stages (Preetude)

                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Lead%' OR Oppo_Stage LIKE '%Identification%') THEN '01 - Identification'

                WHEN Oppo_Type = 'Preetude' AND Oppo_Stage LIKE '%Qualif%' THEN '02 - Qualifiée'

                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Evaluation%' OR Oppo_Stage LIKE '%Assessment%') THEN '03 - Evaluation technique'

                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Proposal%' OR Oppo_Stage LIKE '%Construction%') THEN '04 - Construction propositions'

                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Negotiation%' OR Oppo_Stage LIKE '%Négociation%') THEN '05 - Négociation'

                -- Sales Pipeline stages (Non-Preetude)

                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Identification%' OR Oppo_Stage LIKE '%Lead%') THEN 'Identified'

                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Qualifiée%' OR Oppo_Stage LIKE '%Qualif%') THEN 'Qualified'

                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Evaluation technique%' OR Oppo_Stage LIKE '%Evaluation%' OR Oppo_Stage LIKE '%Assessment%') THEN 'Design in'

                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Construction offre%' OR Oppo_Stage LIKE '%Construction%' OR Oppo_Stage LIKE '%Proposal%') THEN 'Negotiate'

                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Negociation%' OR Oppo_Stage LIKE '%Negotiation%') THEN 'Design Win'

                -- Legacy closed stages (both pipelines)

                WHEN Oppo_Stage = 'Closed Won' THEN 'closedwon'

                WHEN Oppo_Stage = 'Closed Lost' THEN 'closedlost'

                WHEN Oppo_Stage LIKE '%Contract%' THEN 'contractsent'

                -- Default based on pipeline

                WHEN Oppo_Type = 'Preetude' THEN '01 - Identification'

                ELSE 'Identified'

            END as "Deal Stage",

            -- Deal Type (Original Oppo_Type preserved)

            COALESCE(Oppo_Type, 'Unknown') as "Deal Type",

            -- Deal Source

            COALESCE(Oppo_Source, 'Unknown') as "Lead Source",

            -- Financial fields (unified approach)

            Oppo_Forecast as "Forecast Amount",

            Oppo_Total as "Amount",

            -- Status mapping (unified)

            CASE

                WHEN Oppo_Status LIKE '%Won%' OR Oppo_Status LIKE '%Gagnée%' THEN 'Gagnée'

                WHEN Oppo_Status LIKE '%Lost%' OR Oppo_Status LIKE '%Perdue%' THEN 'Perdue'

                WHEN Oppo_Status LIKE '%Abandon%' THEN 'Abandonnée'

                WHEN Oppo_Status LIKE '%Sommeil%' OR Oppo_Status LIKE '%Sleep%' THEN 'En Sommeil'

                WHEN Oppo_Status LIKE '%NoGo%' OR Oppo_Status LIKE '%No Go%' THEN 'NoGo'

                ELSE 'En cours'

            END as "Deal Status",

            -- Additional fields

            'ICALPS' as "Brand",

            COALESCE(Oppo_Note, '') as "Deal Notes",

            Oppo_CreatedDate as "Create Date",

            Oppo_TargetClose as "Close Date",

            -- FK Helper Columns

            Oppo_PrimaryCompanyId as "Company ID",

            Oppo_PrimaryPersonId as "Contact ID",

            -- Helper column for filtering/reporting

            CASE

                WHEN Oppo_Type = 'Preetude' THEN 'Study'

                ELSE 'Opportunity'

            END as "Deal Category"

        FROM raw_opportunities

        WHERE (

            -- Include Studies (Preetude)

            Oppo_Type = 'Preetude'

            OR

            -- Include Deals (Non-Preetude with forecast)

            (Oppo_Type <> 'Preetude' AND Oppo_Forecast IS NOT NULL AND Oppo_Forecast <> 0)

        )

        """

        # Transform Companies (unchanged)

        companies_query = """

        SELECT

            Comp_CompanyId as "Record ID",

            Comp_Name as "Company name",

            CASE

                WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)

                WHEN Comp_WebSite LIKE 'http%' THEN

                    REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\\.)?([^/]+)', 1)

                ELSE COALESCE(Comp_WebSite, '')

            END as "Company Domain Name",

            Comp_PrimaryUserId as "Company owner",

            Comp_CreatedDate as "Create Date",

            Comp_Revenue as "Annual Revenue",

            Comp_Employees as "Number of Employees",

            COALESCE(Comp_Sector, 'Other') as "Industry",

            COALESCE(Comp_Type, 'Customer') as "Company Type",

            COALESCE(Comp_Status, 'Active') as "Lifecycle Stage",

            COALESCE(Comp_Source, 'Unknown') as "Original Source",

            Comp_UpdatedDate as "Last Modified Date",

            -- FK Helper Column

            Comp_PrimaryPersonId as "Primary Contact ID"

        FROM raw_companies

        WHERE Comp_Name IS NOT NULL

          AND TRIM(Comp_Name) != ''

        """

        # Transform Contacts (unchanged)

        contacts_query = """

        SELECT

            Pers_PersonId as "Record ID",

            COALESCE(Pers_FirstName, '') as "First Name",

            COALESCE(Pers_LastName, 'Unknown') as "Last Name",

            CASE

                WHEN Pers_FirstName IS NOT NULL AND Pers_LastName IS NOT NULL THEN

                    LOWER(Pers_FirstName || '.' || Pers_LastName || '@' ||

                          COALESCE(c.comp_domain, 'example.com'))

                ELSE 'unknown@example.com'

            END as "Email",

            COALESCE(Pers_Title, '') as "Job Title",

            COALESCE(Pers_TitleCode, '') as "Job Title Code",

            COALESCE(Pers_Department, '') as "Department",

            COALESCE(Pers_Salutation, '') as "Salutation",

            COALESCE(Pers_Status, 'Active') as "Contact Status",

            COALESCE(Pers_Source, 'Unknown') as "Original Source",

            Pers_CreatedDate as "Create Date",

            Pers_UpdatedDate as "Last Modified Date",

            Pers_CreatedBy as "Created By",

            Pers_UpdatedBy as "Updated By",

            -- FK Helper Column

            Pers_CompanyId as "Company ID"

        FROM raw_persons p

        LEFT JOIN (

            SELECT

                Comp_CompanyId,

                CASE

                    WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)

                    WHEN Comp_WebSite LIKE 'http%' THEN

                        REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\\.)?([^/]+)', 1)

                    ELSE COALESCE(Comp_WebSite, 'example.com')

                END as comp_domain

            FROM raw_companies

        ) c ON p.Pers_CompanyId = c.Comp_CompanyId

        WHERE COALESCE(Pers_FirstName, Pers_LastName) IS NOT NULL

          AND (Pers_Deleted IS NULL OR Pers_Deleted = 0)

        """

        # Execute transformations

        deals_df = self.conn.execute(unified_deals_query).df()

        companies_df = self.conn.execute(companies_query).df()

        contacts_df = self.conn.execute(contacts_query).df()

        self.logger.info(f"Transformed unified data:")

        self.logger.info(f"   - Unified Deals: {len(deals_df)} records")

        # Log pipeline distribution

        if not deals_df.empty:

            pipeline_counts = deals_df['Pipeline'].value_counts()

            for pipeline, count in pipeline_counts.items():

                self.logger.info(f"     * {pipeline}: {count} records")

        self.logger.info(f"   - Companies: {len(companies_df)} records")

        self.logger.info(f"   - Contacts: {len(contacts_df)} records")

        return {

            'deals': deals_df,  # Single unified deals object

            'companies': companies_df,

            'contacts': contacts_df

        }

    def export_for_hubspot_import(self, transformed_data):

        """Export data as CSV files for HubSpot manual import"""

        output_dir = Path('hubspot_import_files')

        output_dir.mkdir(exist_ok=True)

        # Export each object type

        for object_type, df in transformed_data.items():

            file_path = output_dir / f'{object_type}_import.csv'

            df.to_csv(file_path, index=False)

            self.logger.info(f"Exported {len(df)} {object_type} records to {file_path}")

        # Create import instructions

        self.create_import_instructions(output_dir, transformed_data)

    def create_import_instructions(self, output_dir, transformed_data):

        """Create step-by-step import instructions for unified approach"""

        instructions = """

# HubSpot Import Instructions - Unified Deals Approach

  

## Overview

This approach merges Studies and Deals into a single Deal object, differentiated by Pipeline:

- Studies Pipeline: Former Preetude records (Studies)

- Sales Pipeline: Former opportunity records (Deals)

  

## Prerequisites

1. Create TWO pipelines in HubSpot Deals:

   - "Studies Pipeline" with stages: 01-Identification, 02-Qualifiée, 03-Evaluation technique, 04-Construction propositions, 05-Négociation

   - "Sales Pipeline" with stages: Identified, Qualified, Design in, Negotiate, Design Win, closedwon, closedlost

  

## Import Order

  

### Step 1: Import Companies FIRST

File: companies_import.csv

- Go to Contacts > Companies > Import

- Map all company fields as before

  

### Step 2: Import Contacts SECOND  

File: contacts_import.csv

- Go to Contacts > Contacts > Import

- Map all contact fields as before

  

### Step 3: Import Unified Deals THIRD

File: deals_import.csv

- Go to Sales > Deals > Import

- Key mappings:

  - Record ID -> Deal ID (for deduplication)

  - Deal Name -> Deal name

  - Deal Owner -> Deal owner

  - Pipeline -> Deal pipeline (CRITICAL - differentiates Studies vs Sales)

  - Deal Stage -> Deal stage (mapped per pipeline)

  - Deal Type -> Deal type (preserves original Oppo_Type)

  - Deal Category -> Custom property (Study vs Opportunity)

  - Company ID -> Company association

  - Contact ID -> Contact association

  

## Pipeline Configuration Required

  

### Studies Pipeline Stages:

- 01 - Identification

- 02 - Qualifiée  

- 03 - Evaluation technique

- 04 - Construction propositions

- 05 - Négociation

  

### Sales Pipeline Stages:

- Identified

- Qualified

- Design in

- Negotiate

- Design Win

- closedwon

- closedlost

  

## Benefits of Unified Approach

  

1. Single object to manage (simpler)

2. Unified reporting across all opportunities

3. Easy conversion tracking (Studies -> Sales via pipeline change)

4. Consistent deal management process

5. Pipeline-based filtering and views

  

## Reporting & Views

  

### Create Views:

- "Studies View": Filter by Pipeline = "Studies Pipeline"

- "Sales View": Filter by Pipeline = "Sales Pipeline"

- "All Opportunities": No filter (unified view)

  

### Create Reports:

- Studies funnel performance

- Sales funnel performance

- Study-to-Sale conversion rates

- Pipeline comparison analysis

  

## Statistics

"""

        for object_type, df in transformed_data.items():

            instructions += f"- {object_type.title()}: {len(df)} records\n"

        # Add pipeline breakdown if deals exist

        if 'deals' in transformed_data and not transformed_data['deals'].empty:

            pipeline_counts = transformed_data['deals']['Pipeline'].value_counts()

            instructions += "\n### Pipeline Distribution:\n"

            for pipeline, count in pipeline_counts.items():

                instructions += f"- {pipeline}: {count} records\n"

        with open(output_dir / 'UNIFIED_IMPORT_INSTRUCTIONS.md', 'w', encoding='utf-8') as f:

            f.write(instructions)

        self.logger.info(f"Created unified import instructions")

  

    def preview_unified_approach(self, file_paths, limit=5):

        """Preview the unified approach before processing"""

        print("Unified Deals Approach Preview")

        print("=" * 50)

        if Path(file_paths.get('opportunities', '')).exists():

            oppo_df = pd.read_csv(file_paths['opportunities'])

            # Show pipeline distribution

            preetude_count = len(oppo_df[oppo_df['Oppo_Type'] == 'Preetude'])

            non_preetude_count = len(oppo_df[(oppo_df['Oppo_Type'] != 'Preetude') &

                                           (oppo_df['Oppo_Forecast'].notna()) &

                                           (oppo_df['Oppo_Forecast'] != 0)])

            print(f"\nPipeline Distribution:")

            print(f"Studies Pipeline (Preetude): {preetude_count} records")

            print(f"Sales Pipeline (Non-Preetude): {non_preetude_count} records")

            print(f"Total Unified Deals: {preetude_count + non_preetude_count} records")

            print(f"\nSample unified data:")

            sample_data = oppo_df[['Oppo_OpportunityId', 'Oppo_Type', 'Oppo_Stage', 'Oppo_Description']].head(limit)

            print(sample_data)

  

# Usage Example

def main():

    processor = UnifiedDealsProcessor()

    # File paths to your legacy data

    file_paths = {

        'opportunities': 'data_input/legacy_opportunities.csv',

        'companies': 'data_input/legacy_companies.csv',

        'persons': 'data_input/legacy_persons.csv'

    }

    # Preview unified approach

    processor.preview_unified_approach(file_paths)

    # Process data with unified approach

    transformed_data = processor.process_legacy_data(file_paths)

    print("\nUnified data processing complete!")

    print("Files created in 'hubspot_import_files' directory:")

    print("   - companies_import.csv")

    print("   - contacts_import.csv")

    print("   - deals_import.csv (UNIFIED Studies + Deals)")

    print("Follow instructions in:")

    print("   - UNIFIED_IMPORT_INSTRUCTIONS.md")

    print("\nKey advantages:")

    print("   - Single Deal object for both Studies and Opportunities")

    print("   - Pipeline column differentiates record types")

    print("   - Unified reporting and management")

    print("   - Easy Study -> Deal conversions")

  

if __name__ == "__main__":

    main()

Deal transformation patterns
let
    // Start from the merged dataset (Legacy CRM + Current CRM)
    Source = Table.NestedJoin(Table1, {"Oppo_OpportunityId"}, Table3, {"icalps_deal_id"}, "Table3", JoinKind.LeftOuter),
    #"Expanded Table3" = Table.ExpandTableColumn(Source, "Table3", {"Record ID", "Deal Name", "Deal Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id"}, {"Record ID", "Deal Name", "Deal Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id"}),
    
    // Rename legacy forecast to Amount for consistency
    #"Renamed Legacy Amount" = Table.RenameColumns(#"Expanded Table3",{{"Oppo_Forecast", "Legacy_Amount"}}),
    
    // Stage mapping function - Legacy to IC'ALPS structure
    MapStage = (stage as nullable text) as text =>
        if stage = null then ""
        else if stage = "Identification" then "01 - Identification"
        else if stage = "Qualified" then "02 - Qualifiée"
        else if stage = "Evaluation technique" then "03 - Evaluation technique"
        else if stage = "Construction offre" then "04 - Construction propositions"
        else if stage = "Negotiating" then "05 - Négociations"
        else stage, // Keep original value if no mapping found
    
    // Status mapping function (keeping original values as they are already correct)
    MapStatus = (status as nullable text) as text =>
        if status = null then ""
        else status, // Status values are already correct: In Progress, Abandonne, Lost, Won, NoGo
    
    // Smart semantic mapping function for CRM Deal Stages
    MapDealStage = (pipeline as nullable text, stage as nullable text, status as nullable text) as text =>
        let
            // Normalize inputs
            clean_pipeline = if pipeline = null then "" else Text.Lower(Text.Trim(pipeline)),
            clean_stage = if stage = null then "" else Text.Trim(stage),
            clean_status = if status = null then "" else Text.Lower(Text.Trim(status)),
            
            // Determine if it's Hardware or Software pipeline
            is_hardware = Text.Contains(clean_pipeline, "hardware") or Text.Contains(clean_pipeline, "icalps_hardware"),
            is_software = Text.Contains(clean_pipeline, "software") or Text.Contains(clean_pipeline, "service") or Text.Contains(clean_pipeline, "icalps_service"),
            
            result = 
                // Handle universal outcomes first (regardless of pipeline)
                if clean_status = "perdue" or clean_status = "lost" then "Closed Lost"
                else if clean_status = "abandonnée" or clean_status = "abandonne" or clean_status = "nogo" then "Closed Lost"
                
                // Hardware Pipeline Logic
                else if is_hardware then
                    if clean_status = "gagnée" or clean_status = "won" then
                        // Hardware wins - map based on stage maturity
                        if Text.Contains(clean_stage, "01 - identification") then "Identified"
                        else if Text.Contains(clean_stage, "02 - qualifiée") then "Qualified"
                        else if Text.Contains(clean_stage, "03 - evaluation") then "Design Win"
                        else if Text.Contains(clean_stage, "04 - construction") or Text.Contains(clean_stage, "05 - négociations") then "Closed Won"
                        else "Closed Won"
                    else if clean_status = "en cours" or clean_status = "in progress" then
                        // Hardware in progress - map based on stage
                        if Text.Contains(clean_stage, "01 - identification") then "Identified"
                        else if Text.Contains(clean_stage, "02 - qualifiée") then "Qualified"
                        else if Text.Contains(clean_stage, "03 - evaluation") then "Design In"
                        else if Text.Contains(clean_stage, "04 - construction") then "Design In"
                        else if Text.Contains(clean_stage, "05 - négociations") then "Design Win"
                        else "Design In"
                    else "On-Hold"
                
                // Software Pipeline Logic  
                else if is_software then
                    if clean_status = "gagnée" or clean_status = "won" then "Closed Won"
                    else if clean_status = "en cours" or clean_status = "in progress" then
                        // Software in progress - simpler mapping
                        if Text.Contains(clean_stage, "01 - identification") then "Identification"
                        else if Text.Contains(clean_stage, "02 - qualifiée") or 
                               Text.Contains(clean_stage, "03 - evaluation") or
                               Text.Contains(clean_stage, "04 - construction") or
                               Text.Contains(clean_stage, "05 - négociations") then "Qualified"
                        else "Identified"
                    else "On-Hold"
                
                // Default fallback
                else if clean_status = "gagnée" or clean_status = "won" then "Closed Won"
                else if clean_status = "en cours" or clean_status = "in progress" then "Identified"
                else "On-Hold"
        in
            result,
    
    // Add mapped legacy stage column
    #"Added Mapped Legacy Stage" = Table.AddColumn(#"Renamed Legacy Amount", "Legacy_Mapped_Stage", each 
        MapStage([Oppo_Stage]), type text),
    
    // Add mapped legacy status column  
    #"Added Mapped Legacy Status" = Table.AddColumn(#"Added Mapped Legacy Stage", "Legacy_Mapped_Status", each 
        MapStatus([Oppo_Status]), type text),
    
    // Determine pipeline type based on opportunity type or product
    #"Added Pipeline Type" = Table.AddColumn(#"Added Mapped Legacy Status", "Pipeline_Type", each 
        let
            oppo_type = if [Oppo_Type] = null then "" else Text.Lower([Oppo_Type]),
            oppo_product = if [Oppo_Product] = null then "" else Text.Lower([Oppo_Product]),
            deal_name = if [Deal Name] = null then "" else Text.Lower([Deal Name])
        in
            // Determine pipeline based on business logic
            if Text.Contains(oppo_type, "service") or Text.Contains(oppo_type, "etude") or Text.Contains(oppo_type, "étude") or
               Text.Contains(oppo_product, "service") or Text.Contains(oppo_product, "software") or
               Text.Contains(deal_name, "etude") or Text.Contains(deal_name, "étude") or 
               Text.Contains(deal_name, "faisabilité") or Text.Contains(deal_name, "pre-etude") then 
                "icalps_service"
            else 
                "icalps_hardware", type text),
    
    // Apply smart semantic mapping to get predicted CRM Deal Stage
    #"Added Predicted CRM Stage" = Table.AddColumn(#"Added Pipeline Type", "Predicted_CRM_Deal_Stage", each 
        MapDealStage([Pipeline_Type], [Legacy_Mapped_Stage], [Legacy_Mapped_Status]), type text),
    
    // Add OppoStage concatenation column (for future uploads)
    #"Added OppoStage" = Table.AddColumn(#"Added Predicted CRM Stage", "OppoStage", each 
        if [Legacy_Mapped_Stage] = "" or [Legacy_Mapped_Status] = "" then 
            ""
        else 
            [Legacy_Mapped_Stage] & " - " & [Legacy_Mapped_Status], type text),
    
    // Add comparison column to see if prediction matches actual
    #"Added Stage Comparison" = Table.AddColumn(#"Added OppoStage", "Stage_Match", each 
        if [Deal Stage] = null then "No Current CRM Data"
        else if [Predicted_CRM_Deal_Stage] = [Deal Stage] then "Match"
        else "Mismatch: " & [Predicted_CRM_Deal_Stage] & " vs " & [Deal Stage], type text),
    
    // Financial calculations
    #"Added Net Amount" = Table.AddColumn(#"Added Stage Comparison", "Net_Legacy_Amount", each 
        [Legacy_Amount] - [oppo_cout], type number),
    
    #"Added Weighted Amount" = Table.AddColumn(#"Added Net Amount", "Weighted_Legacy_Amount", each 
        [Legacy_Amount] * [Oppo_Certainty], type number),
    
    #"Added Net Weighted Amount" = Table.AddColumn(#"Added Weighted Amount", "Net_Weighted_Legacy_Amount", each 
        [Net_Legacy_Amount] * [Oppo_Certainty], type number)

in
    #"Added Net Weighted Amount"



Logic and mapping

"""

Updated DataProcessor with Custom Object Creation

Based on new stage mappings from attachments

"""

## Python base data processor
  
"""
Complete Enhanced CRM Processor - Final Version
Unified Deals with Deal Certainty + Communications Processing
"""

import pandas as pd
import duckdb
from pathlib import Path
import logging
from datetime import datetime

class EnhancedCRMProcessor:
    def __init__(self):
        self.conn = duckdb.connect(':memory:')
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    # =========================================================================
    # UNIFIED DEALS PROCESSING (Enhanced with Deal Certainty)
    # =========================================================================
    
    def process_legacy_data(self, file_paths):
        """
        Process legacy data into unified Deal objects with enhanced fields
        Output: CSV files ready for HubSpot import with Pipeline differentiation
        """
        # Load raw data
        raw_data = self.load_raw_files(file_paths)
        
        # Transform data with unified approach
        transformed_data = self.transform_unified_data(raw_data)
        
        # Export for HubSpot import
        self.export_for_hubspot_import(transformed_data)
        
        return transformed_data
    
    def load_raw_files(self, file_paths):
        """Load and register raw CSV files"""
        data = {}
        
        # Load opportunities data
        if 'opportunities' in file_paths:
            oppo_df = pd.read_csv(file_paths['opportunities'])
            self.conn.register('raw_opportunities', oppo_df)
            data['opportunities'] = oppo_df
            self.logger.info(f"Loaded {len(oppo_df)} opportunity records")
        
        # Load companies data
        if 'companies' in file_paths:
            comp_df = pd.read_csv(file_paths['companies'])
            self.conn.register('raw_companies', comp_df)
            data['companies'] = comp_df
            self.logger.info(f"Loaded {len(comp_df)} company records")
        
        # Load persons data
        if 'persons' in file_paths:
            pers_df = pd.read_csv(file_paths['persons'])
            self.conn.register('raw_persons', pers_df)
            data['persons'] = pers_df
            self.logger.info(f"Loaded {len(pers_df)} person records")
        
        return data

    def transform_unified_data(self, raw_data):
        """Transform raw data into unified Deal objects with enhanced fields"""
        
        self.logger.info("Starting enhanced unified data transformation...")
        
        # Log the unified approach
        self.logger.info("Enhanced Unified Object Strategy:")
        self.logger.info("   - All records become 'Deals' in HubSpot")
        self.logger.info("   - Pipeline 'Studies Pipeline': Oppo_Type = 'Preetude'")
        self.logger.info("   - Pipeline 'Sales Pipeline': Oppo_Type <> 'Preetude' AND Forecast <> 0")
        self.logger.info("   - Enhanced with Deal Certainty for analysis")
        
        # Enhanced unified deals query with Deal Certainty
        unified_deals_query = """
        SELECT 
            Oppo_OpportunityId as "Record ID",
            Oppo_Description as "Deal Name",
            Oppo_AssignedUserId as "Deal Owner",
            
            -- Pipeline Helper Column (MAIN DIFFERENTIATOR)
            CASE 
                WHEN Oppo_Type = 'Preetude' THEN 'Studies Pipeline'
                ELSE 'Sales Pipeline'
            END as "Pipeline",
            
            -- Deal Stage (Different mapping based on pipeline)
            CASE 
                -- Studies Pipeline stages (Preetude)
                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Lead%' OR Oppo_Stage LIKE '%Identification%') THEN '01 - Identification'
                WHEN Oppo_Type = 'Preetude' AND Oppo_Stage LIKE '%Qualif%' THEN '02 - Qualifiée'
                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Evaluation%' OR Oppo_Stage LIKE '%Assessment%') THEN '03 - Evaluation technique'
                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Proposal%' OR Oppo_Stage LIKE '%Construction%') THEN '04 - Construction propositions'
                WHEN Oppo_Type = 'Preetude' AND (Oppo_Stage LIKE '%Negotiation%' OR Oppo_Stage LIKE '%Négociation%') THEN '05 - Négociation'
                
                -- Sales Pipeline stages (Non-Preetude)
                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Identification%' OR Oppo_Stage LIKE '%Lead%') THEN 'Identified'
                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Qualifiée%' OR Oppo_Stage LIKE '%Qualif%') THEN 'Qualified'
                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Evaluation technique%' OR Oppo_Stage LIKE '%Evaluation%' OR Oppo_Stage LIKE '%Assessment%') THEN 'Design in'
                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Construction offre%' OR Oppo_Stage LIKE '%Construction%' OR Oppo_Stage LIKE '%Proposal%') THEN 'Negotiate'
                WHEN Oppo_Type <> 'Preetude' AND (Oppo_Stage LIKE '%Negociation%' OR Oppo_Stage LIKE '%Negotiation%') THEN 'Design Win'
                
                -- Legacy closed stages (both pipelines)
                WHEN Oppo_Stage = 'Closed Won' THEN 'closedwon'
                WHEN Oppo_Stage = 'Closed Lost' THEN 'closedlost'
                WHEN Oppo_Stage LIKE '%Contract%' THEN 'contractsent'
                
                -- Default based on pipeline
                WHEN Oppo_Type = 'Preetude' THEN '01 - Identification'
                ELSE 'Identified'
            END as "Deal Stage",
            
            -- ENHANCED DEAL METADATA (Same field names for both Studies and Deals)
            COALESCE(Oppo_Type, 'Unknown') as "Deal Type",
            COALESCE(Oppo_Source, 'Unknown') as "Deal Source",
            Oppo_Forecast as "Deal Forecast",
            Oppo_Total as "Deal Amount",
            Oppo_Certainty as "Deal Certainty",
            
            -- Deal Status (unified)
            CASE 
                WHEN Oppo_Status LIKE '%Won%' OR Oppo_Status LIKE '%Gagnée%' THEN 'Gagnée'
                WHEN Oppo_Status LIKE '%Lost%' OR Oppo_Status LIKE '%Perdue%' THEN 'Perdue'
                WHEN Oppo_Status LIKE '%Abandon%' THEN 'Abandonnée'
                WHEN Oppo_Status LIKE '%Sommeil%' OR Oppo_Status LIKE '%Sleep%' THEN 'En Sommeil'
                WHEN Oppo_Status LIKE '%NoGo%' OR Oppo_Status LIKE '%No Go%' THEN 'NoGo'
                ELSE 'En cours'
            END as "Deal Status",
            
            -- Standard Deal fields
            'ICALPS' as "Deal Brand",
            COALESCE(Oppo_Note, '') as "Deal Notes",
            Oppo_CreatedDate as "Deal Created Date",
            Oppo_TargetClose as "Deal Close Date",
            
            -- FK Helper Columns (using Deal naming)
            Oppo_PrimaryCompanyId as "Deal Company ID",
            Oppo_PrimaryPersonId as "Deal Contact ID",
            
            -- Helper column for original categorization
            CASE 
                WHEN Oppo_Type = 'Preetude' THEN 'Study'
                ELSE 'Opportunity'
            END as "Deal Category"
            
        FROM raw_opportunities
        WHERE (
            -- Include Studies (Preetude)
            Oppo_Type = 'Preetude'
            OR 
            -- Include Deals (Non-Preetude with forecast)
            (Oppo_Type <> 'Preetude' AND Oppo_Forecast IS NOT NULL AND Oppo_Forecast <> 0)
        )
        """
        
        # Transform Companies (unchanged)
        companies_query = """
        SELECT 
            Comp_CompanyId as "Record ID",
            Comp_Name as "Company name",
            CASE 
                WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)
                WHEN Comp_WebSite LIKE 'http%' THEN 
                    REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\\.)?([^/]+)', 1)
                ELSE COALESCE(Comp_WebSite, '')
            END as "Company Domain Name",
            Comp_PrimaryUserId as "Company owner",
            Comp_CreatedDate as "Create Date",
            Comp_Revenue as "Annual Revenue",
            Comp_Employees as "Number of Employees",
            COALESCE(Comp_Sector, 'Other') as "Industry",
            COALESCE(Comp_Type, 'Customer') as "Company Type",
            COALESCE(Comp_Status, 'Active') as "Lifecycle Stage",
            COALESCE(Comp_Source, 'Unknown') as "Original Source",
            Comp_UpdatedDate as "Last Modified Date",
            -- FK Helper Column
            Comp_PrimaryPersonId as "Primary Contact ID"
        FROM raw_companies
        WHERE Comp_Name IS NOT NULL 
          AND TRIM(Comp_Name) != ''
        """
        
        # Transform Contacts (unchanged)
        contacts_query = """
        SELECT 
            Pers_PersonId as "Record ID",
            COALESCE(Pers_FirstName, '') as "First Name",
            COALESCE(Pers_LastName, 'Unknown') as "Last Name",
            CASE 
                WHEN Pers_FirstName IS NOT NULL AND Pers_LastName IS NOT NULL THEN
                    LOWER(Pers_FirstName || '.' || Pers_LastName || '@' || 
                          COALESCE(c.comp_domain, 'example.com'))
                ELSE 'unknown@example.com'
            END as "Email",
            COALESCE(Pers_Title, '') as "Job Title",
            COALESCE(Pers_TitleCode, '') as "Job Title Code",
            COALESCE(Pers_Department, '') as "Department",
            COALESCE(Pers_Salutation, '') as "Salutation",
            COALESCE(Pers_Status, 'Active') as "Contact Status",
            COALESCE(Pers_Source, 'Unknown') as "Original Source",
            Pers_CreatedDate as "Create Date",
            Pers_UpdatedDate as "Last Modified Date",
            Pers_CreatedBy as "Created By",
            Pers_UpdatedBy as "Updated By",
            -- FK Helper Column
            Pers_CompanyId as "Company ID"
        FROM raw_persons p
        LEFT JOIN (
            SELECT 
                Comp_CompanyId,
                CASE 
                    WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)
                    WHEN Comp_WebSite LIKE 'http%' THEN 
                        REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\\.)?([^/]+)', 1)
                    ELSE COALESCE(Comp_WebSite, 'example.com')
                END as comp_domain
            FROM raw_companies
        ) c ON p.Pers_CompanyId = c.Comp_CompanyId
        WHERE COALESCE(Pers_FirstName, Pers_LastName) IS NOT NULL
          AND (Pers_Deleted IS NULL OR Pers_Deleted = 0)
        """
        
        # Execute transformations
        deals_df = self.conn.execute(unified_deals_query).df()
        companies_df = self.conn.execute(companies_query).df()
        contacts_df = self.conn.execute(contacts_query).df()
        
        self.logger.info(f"Transformed enhanced unified data:")
        self.logger.info(f"   - Enhanced Unified Deals: {len(deals_df)} records")
        
        # Log pipeline distribution
        if not deals_df.empty:
            pipeline_counts = deals_df['Pipeline'].value_counts()
            for pipeline, count in pipeline_counts.items():
                self.logger.info(f"     * {pipeline}: {count} records")
        
        self.logger.info(f"   - Companies: {len(companies_df)} records")
        self.logger.info(f"   - Contacts: {len(contacts_df)} records")
        
        return {
            'deals': deals_df,  # Enhanced unified deals object
            'companies': companies_df,
            'contacts': contacts_df
        }
    
    def export_for_hubspot_import(self, transformed_data):
        """Export data as CSV files for HubSpot manual import"""
        
        output_dir = Path('hubspot_import_files')
        output_dir.mkdir(exist_ok=True)
        
        # Export each object type
        for object_type, df in transformed_data.items():
            file_path = output_dir / f'{object_type}_import.csv'
            df.to_csv(file_path, index=False)
            self.logger.info(f"Exported {len(df)} {object_type} records to {file_path}")
        
        # Create import instructions
        self.create_import_instructions(output_dir, transformed_data)
    
    def create_import_instructions(self, output_dir, transformed_data):
        """Create step-by-step import instructions for enhanced unified approach"""
        
        instructions = """
# HubSpot Import Instructions - Enhanced Unified Deals Approach

## Overview
Enhanced approach with unified Deal object + Deal Certainty field:
- Studies Pipeline: Former Preetude records (Studies)  
- Sales Pipeline: Former opportunity records (Deals)
- Enhanced Analytics: Deal Certainty field for advanced analysis

## Enhanced Deal Fields
ALL records (both Studies and Deals) include:
- Deal Forecast (projected value)
- Deal Amount (actual value) 
- Deal Certainty (confidence level) ← ENHANCED FEATURE
- All standard deal metadata with unified naming

## Prerequisites
1. Create TWO pipelines in HubSpot Deals:
   - "Studies Pipeline" with stages: 01-Identification, 02-Qualifiée, 03-Evaluation technique, 04-Construction propositions, 05-Négociation
   - "Sales Pipeline" with stages: Identified, Qualified, Design in, Negotiate, Design Win, closedwon, closedlost

2. Create custom properties:
   - Deal Certainty (Number/Percentage field)
   - Deal Status (Dropdown)
   - Deal Brand (Text)
   - Deal Category (Dropdown: Study/Opportunity)

## Import Order

### Step 1: Import Companies FIRST
File: companies_import.csv
- Standard company import process

### Step 2: Import Contacts SECOND  
File: contacts_import.csv
- Standard contact import process

### Step 3: Import Enhanced Unified Deals THIRD
File: deals_import.csv
- Key mappings:
  - Record ID -> Deal ID (for deduplication)
  - Deal Name -> Deal name
  - Deal Owner -> Deal owner
  - Pipeline -> Deal pipeline (CRITICAL - differentiates Studies vs Sales)
  - Deal Stage -> Deal stage (mapped per pipeline)
  - Deal Forecast -> Deal forecast amount
  - Deal Amount -> Deal amount
  - Deal Certainty -> Deal certainty (ENHANCED FIELD)
  - Deal Status -> Deal status
  - Deal Company ID -> Company association
  - Deal Contact ID -> Contact association

## Enhanced Analytics Opportunities

### Forecast Analysis:
- Weighted Pipeline Value: Deal Forecast × Deal Certainty
- Forecast Accuracy: (Deal Amount - Deal Forecast) / Deal Forecast
- Certainty Trends: Track confidence changes by stage

### Risk Assessment:
- High Risk: Deal Certainty < 30%
- Medium Risk: Deal Certainty 30-70%  
- Low Risk: Deal Certainty > 70%

### Pipeline Reporting:
- Probability-adjusted forecasts
- Certainty distribution by stage
- Conversion correlation with certainty

## Statistics
"""
        
        for object_type, df in transformed_data.items():
            instructions += f"- {object_type.title()}: {len(df)} records\n"
        
        # Add pipeline breakdown if deals exist
        if 'deals' in transformed_data and not transformed_data['deals'].empty:
            pipeline_counts = transformed_data['deals']['Pipeline'].value_counts()
            instructions += "\n### Pipeline Distribution:\n"
            for pipeline, count in pipeline_counts.items():
                instructions += f"- {pipeline}: {count} records\n"
        
        with open(output_dir / 'ENHANCED_UNIFIED_IMPORT_INSTRUCTIONS.md', 'w', encoding='utf-8') as f:
            f.write(instructions)
        
        self.logger.info(f"Created enhanced unified import instructions")

    def preview_unified_approach(self, file_paths, limit=5):
        """Preview the enhanced unified approach before processing"""
        print("Enhanced Unified Deals Approach Preview")
        print("=" * 55)
        
        if Path(file_paths.get('opportunities', '')).exists():
            oppo_df = pd.read_csv(file_paths['opportunities'])
            
            # Show pipeline distribution
            preetude_count = len(oppo_df[oppo_df['Oppo_Type'] == 'Preetude'])
            non_preetude_count = len(oppo_df[(oppo_df['Oppo_Type'] != 'Preetude') & 
                                           (oppo_df['Oppo_Forecast'].notna()) & 
                                           (oppo_df['Oppo_Forecast'] != 0)])
            
            print(f"\nPipeline Distribution:")
            print(f"Studies Pipeline (Preetude): {preetude_count} records")
            print(f"Sales Pipeline (Non-Preetude): {non_preetude_count} records")
            print(f"Total Enhanced Unified Deals: {preetude_count + non_preetude_count} records")
            
            # Show certainty data if available
            if 'Oppo_Certainty' in oppo_df.columns:
                certainty_stats = oppo_df['Oppo_Certainty'].describe()
                print(f"\nDeal Certainty Analysis:")
                print(f"Average Certainty: {certainty_stats['mean']:.1f}%")
                print(f"Min/Max Certainty: {certainty_stats['min']:.1f}% / {certainty_stats['max']:.1f}%")
            
            print(f"\nSample enhanced data:")
            sample_cols = ['Oppo_OpportunityId', 'Oppo_Type', 'Oppo_Stage', 'Oppo_Description']
            if 'Oppo_Certainty' in oppo_df.columns:
                sample_cols.append('Oppo_Certainty')
            sample_data = oppo_df[sample_cols].head(limit)
            print(sample_data)

    # =========================================================================
    # COMMUNICATIONS PROCESSING
    # =========================================================================
    
    def load_communications_data(self, file_path):
        """Load the communications CSV file with all joined data"""
        
        self.logger.info("Loading communications data with joins...")
        
        # Load the complex joined CSV
        comm_df = pd.read_csv(file_path)
        self.conn.register('raw_communications', comm_df)
        
        self.logger.info(f"Loaded {len(comm_df)} communication records")
        self.logger.info(f"Available columns: {len(comm_df.columns)} total")
        
        return comm_df
    
    def create_communications_dummy_object(self):
        """Create dummy communications custom object for HubSpot setup"""
        
        # Create a single dummy record with all properties
        dummy_communication = {
            "Record ID": ["DUMMY_COMM_001"],
            "Communication Subject": ["Dummy Communication - Delete After Import"],
            "Communication Notes": ["Sample notes for property creation"],
            "Communication Source": ["Email"],
            "Communication Type": ["Outbound"],
            "Communication Status": ["Completed"],
            "Communication Date": ["2024-01-01"],
            "Company Name": ["Sample Company"],
            "Contact First Name": ["John"],
            "Contact Last Name": ["Doe"],
            "Company ID": [""],  # FK helper
            "Contact ID": [""],  # FK helper
            "Create Date": ["2024-01-01"]
        }
        
        dummy_df = pd.DataFrame(dummy_communication)
        
        # Create output directory
        output_dir = Path('hubspot_import_files')
        output_dir.mkdir(exist_ok=True)
        
        # Export dummy custom object
        dummy_file_path = output_dir / 'dummy_communications_custom_object.csv'
        dummy_df.to_csv(dummy_file_path, index=False)
        
        self.logger.info(f"Created dummy communications object: {dummy_file_path}")
        
        # Create setup instructions
        self.create_communications_setup_instructions(output_dir)
        
        return dummy_file_path
    
    def create_communications_setup_instructions(self, output_dir):
        """Create setup instructions for communications custom object"""
        
        instructions = """
# Communications Custom Object Setup Instructions

## Step 1: Create Communications Custom Object in HubSpot
1. Go to Settings > Data Management > Objects
2. Click "Create custom object"
3. Enter object details:
   - Object name: Communications
   - Singular label: Communication
   - Plural label: Communications
   - Primary display property: Communication Subject

## Step 2: Import Dummy Record to Create Properties
1. Go to Custom Objects > Communications (after creation)
2. Click "Import"
3. Upload file: dummy_communications_custom_object.csv
4. Map all columns to create properties automatically

## Step 3: Configure Dropdown Options

### Communication Source Options:
- Email, Phone, Meeting, Task, Note, Other

### Communication Type Options:
- Inbound, Outbound, Internal

### Communication Status Options:
- Completed, Pending, Scheduled, Cancelled

## Step 4: Set Up Associations
1. Communications to Companies (Many-to-One)
2. Communications to Contacts (Many-to-One)
3. Communications to Deals (Many-to-Many) - Optional

## Step 5: Delete Dummy Record
After property creation, delete the dummy communication record.

Communications object ready for real data import!
"""
        
        with open(output_dir / 'COMMUNICATIONS_SETUP_INSTRUCTIONS.md', 'w', encoding='utf-8') as f:
            f.write(instructions)
    
    def extract_communications_data(self):
        """Extract and transform communications data using DuckDB"""
        
        self.logger.info("Extracting communications data with DuckDB joins...")
        
        # DuckDB query to extract relevant communications data with joins
        communications_query = """
        SELECT 
            -- Primary communication fields
            Comm_OpportunityId as "Record ID",
            COALESCE(Comm_Subject, 'No Subject') as "Communication Subject",
            COALESCE(Comm_Note, '') as "Communication Notes",
            
            -- Communication metadata
            CASE 
                WHEN Comm_Type LIKE '%Email%' OR Comm_Type LIKE '%email%' THEN 'Email'
                WHEN Comm_Type LIKE '%Phone%' OR Comm_Type LIKE '%Call%' OR Comm_Type LIKE '%phone%' THEN 'Phone'
                WHEN Comm_Type LIKE '%Meeting%' OR Comm_Type LIKE '%meeting%' THEN 'Meeting'
                WHEN Comm_Type LIKE '%Task%' OR Comm_Type LIKE '%task%' THEN 'Task'
                WHEN Comm_Type LIKE '%Note%' OR Comm_Type LIKE '%note%' THEN 'Note'
                ELSE 'Other'
            END as "Communication Source",
            
            CASE 
                WHEN Comm_Action LIKE '%Outbound%' OR Comm_Action LIKE '%Send%' OR Comm_Action LIKE '%Call Out%' THEN 'Outbound'
                WHEN Comm_Action LIKE '%Inbound%' OR Comm_Action LIKE '%Receive%' OR Comm_Action LIKE '%Call In%' THEN 'Inbound'
                ELSE 'Internal'
            END as "Communication Type",
            
            CASE 
                WHEN Comm_Status LIKE '%Complete%' OR Comm_Status LIKE '%Done%' OR Comm_Status LIKE '%Finished%' THEN 'Completed'
                WHEN Comm_Status LIKE '%Pending%' OR Comm_Status LIKE '%Open%' THEN 'Pending'
                WHEN Comm_Status LIKE '%Schedule%' OR Comm_Status LIKE '%Planned%' THEN 'Scheduled'
                WHEN Comm_Status LIKE '%Cancel%' OR Comm_Status LIKE '%Abort%' THEN 'Cancelled'
                ELSE 'Completed'
            END as "Communication Status",
            
            -- Date fields
            Comm_DateTime as "Communication Date",
            Comm_CreatedDate as "Create Date",
            
            -- Company information (from join)
            COALESCE(Comp_Name, 'Unknown Company') as "Company Name",
            
            -- Person information (from join)
            COALESCE(Pers_FirstName, '') as "Contact First Name",
            COALESCE(Pers_LastName, 'Unknown Contact') as "Contact Last Name",
            
            -- Foreign Key helper columns for associations
            CmLi_Comm_CompanyId as "Company ID",
            CmLi_Comm_PersonId as "Contact ID",
            
            -- Additional metadata for context
            Comm_CreatedBy as "Created By",
            Comm_Priority as "Priority",
            Comm_OutCome as "Outcome"
            
        FROM raw_communications
        WHERE 
            -- Filter for relevant communications
            (Comm_Deleted IS NULL OR Comm_Deleted = 0)
            -- Ensure we have either company or person association
            AND (CmLi_Comm_CompanyId IS NOT NULL OR CmLi_Comm_PersonId IS NOT NULL)
            -- Exclude empty communications
            AND (Comm_Subject IS NOT NULL OR Comm_Note IS NOT NULL)
        ORDER BY Comm_CreatedDate DESC
        """
        
        # Execute the query
        try:
            communications_df = self.conn.execute(communications_query).df()
            
            self.logger.info(f"Extracted {len(communications_df)} communications")
            
            # Log some statistics
            if not communications_df.empty:
                source_counts = communications_df['Communication Source'].value_counts()
                self.logger.info("Communication Source distribution:")
                for source, count in source_counts.items():
                    self.logger.info(f"  - {source}: {count} records")
            
            return communications_df
            
        except Exception as e:
            self.logger.error(f"Error executing communications query: {e}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                "Record ID", "Communication Subject", "Communication Notes",
                "Communication Source", "Communication Type", "Communication Status",
                "Communication Date", "Create Date", "Company Name",
                "Contact First Name", "Contact Last Name", "Company ID", "Contact ID"
            ])
    
    def export_communications_for_import(self, communications_df):
        """Export communications data for HubSpot import"""
        
        output_dir = Path('hubspot_import_files')
        output_dir.mkdir(exist_ok=True)
        
        # Export communications data
        file_path = output_dir / 'communications_import.csv'
        communications_df.to_csv(file_path, index=False)
        
        self.logger.info(f"Exported {len(communications_df)} communications to {file_path}")
        
        # Create import instructions
        self.create_communications_import_instructions(output_dir, communications_df)
    
    def create_communications_import_instructions(self, output_dir, communications_df):
        """Create import instructions for communications"""
        
        instructions = """
# Communications Import Instructions

## Import Order
### Step 1: Import Communications
File: communications_import.csv
- Go to Custom Objects > Communications > Import
- Map Company ID and Contact ID for associations

## Statistics
"""
        
        # Add statistics if data exists
        if not communications_df.empty:
            instructions += f"- Total Communications: {len(communications_df)} records\n"
            
            # Source distribution
            source_counts = communications_df['Communication Source'].value_counts()
            instructions += "\n### Communication Source Distribution:\n"
            for source, count in source_counts.items():
                instructions += f"- {source}: {count} records\n"
        
        with open(output_dir / 'COMMUNICATIONS_IMPORT_INSTRUCTIONS.md', 'w', encoding='utf-8') as f:
            f.write(instructions)
        
        self.logger.info("Created communications import instructions")
    
    def process_communications(self, communications_file_path):
        """Main function to process communications data"""
        
        self.logger.info("Starting communications processing...")
        
        # Create dummy custom object for setup
        self.create_communications_dummy_object()
        
        # Load communications data
        raw_data = self.load_communications_data(communications_file_path)
        
        # Extract and transform data
        communications_df = self.extract_communications_data()
        
        if not communications_df.empty:
            # Export for HubSpot import
            self.export_communications_for_import(communications_df)
            
            self.logger.info("Communications processing completed successfully!")
            
            return {
                'communications': communications_df,
                'summary': {
                    'total_records': len(communications_df),
                    'sources': communications_df['Communication Source'].value_counts().to_dict(),
                    'types': communications_df['Communication Type'].value_counts().to_dict()
                }
            }
        else:
            self.logger.warning("No communications data extracted")
            return {'communications': communications_df, 'summary': {}}

    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================
    
    def run_complete_migration(self, file_paths, communications_file_path=None):
        """
        Run the complete enhanced CRM migration workflow
        """
        print("=" * 70)
        print("COMPLETE ENHANCED CRM MIGRATION WORKFLOW")
        print("=" * 70)
        
        # Step 1: Process enhanced deals
        print("\nStep 1: Processing Enhanced Unified Deals...")
        self.preview_unified_approach(

"""

##Custom object workflow

        with open(output_dir / 'CUSTOM_OBJECT_SETUP_INSTRUCTIONS.md', 'w') as f:

            f.write(instructions)

        self.logger.info(f"Created custom object setup instructions")

  

    def process_legacy_data(self, file_paths):

        """

        Process legacy data without any API calls

        Output: CSV files ready for HubSpot import

        """

        # First create dummy custom object

        self.create_dummy_custom_object_csv()

        # Load raw data

        raw_data = self.load_raw_files(file_paths)

        # Transform data

        transformed_data = self.transform_data(raw_data)

        # Export for HubSpot import

        self.export_for_hubspot_import(transformed_data)

        return transformed_data

    def load_raw_files(self, file_paths):

        """Load and register raw CSV files"""

        data = {}

        # Load opportunities data

        if 'opportunities' in file_paths:

            oppo_df = pd.read_csv(file_paths['opportunities'])

            self.conn.register('raw_opportunities', oppo_df)

            data['opportunities'] = oppo_df

            self.logger.info(f"Loaded {len(oppo_df)} opportunity records")

        # Load companies data  

        if 'companies' in file_paths:

            comp_df = pd.read_csv(file_paths['companies'])

            self.conn.register('raw_companies', comp_df)

            data['companies'] = comp_df

            self.logger.info(f"Loaded {len(comp_df)} company records")

        # Load persons data

        if 'persons' in file_paths:

            pers_df = pd.read_csv(file_paths['persons'])

            self.conn.register('raw_persons', pers_df)

            data['persons'] = pers_df

            self.logger.info(f"Loaded {len(pers_df)} person records")

        return data

    def transform_data(self, raw_data):

        """Transform raw data into HubSpot-ready format with updated stage mappings"""

        # Transform Studies (from early-stage opportunities) - UPDATED MAPPINGS

        studies_query = """

        SELECT

            Oppo_OpportunityId as "Record ID",

            Oppo_Description as "Study Name",

            Oppo_AssignedUserId as "Study Owner",

            CASE

                WHEN Oppo_Stage LIKE '%Lead%' OR Oppo_Stage LIKE '%Identification%' THEN '01 - Identification'

                WHEN Oppo_Stage LIKE '%Qualif%' THEN '02 - Qualifiée'

                WHEN Oppo_Stage LIKE '%Evaluation%' OR Oppo_Stage LIKE '%Assessment%' THEN '03 - Evaluation technique'

                WHEN Oppo_Stage LIKE '%Proposal%' OR Oppo_Stage LIKE '%Construction%' THEN '04 - Construction propositions'

                WHEN Oppo_Stage LIKE '%Negotiation%' OR Oppo_Stage LIKE '%Négociation%' THEN '05 - Négociation'

                ELSE '01 - Identification'

            END as "Study Stage",

            COALESCE(Oppo_Type, 'Consulting') as "Study Type",

            COALESCE(Oppo_Source, 'Inbound') as "Study Source",

            COALESCE(Oppo_Forecast, 0) as "Study Forecast Amount",

            CASE

                WHEN Oppo_Status LIKE '%Won%' OR Oppo_Status LIKE '%Gagnée%' THEN 'Gagnée'

                WHEN Oppo_Status LIKE '%Lost%' OR Oppo_Status LIKE '%Perdue%' THEN 'Perdue'

                WHEN Oppo_Status LIKE '%Abandon%' THEN 'Abandonnée'

                WHEN Oppo_Status LIKE '%Sommeil%' OR Oppo_Status LIKE '%Sleep%' THEN 'En Sommeil'

                WHEN Oppo_Status LIKE '%NoGo%' OR Oppo_Status LIKE '%No Go%' THEN 'NoGo'

                ELSE 'En cours'

            END as "Study Status",

            'ICALPS' as "Brand",

            Oppo_PrimaryCompanyId as "Associated Company",

            Oppo_PrimaryPersonId as "Associated Contact",

            Oppo_CreatedDate as "Create Date"

        FROM raw_opportunities

        WHERE Oppo_Stage NOT IN ('Closed Won', 'Closed Lost', 'Negotiation')

           OR Oppo_Stage IS NULL

        """

        # Transform Companies (normalize for HubSpot)

        companies_query = """

        SELECT

            Comp_CompanyId as "Record ID",

            Comp_Name as "Company name",

            CASE

                WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)

                WHEN Comp_WebSite LIKE 'http%' THEN

                    REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\.)?([^/]+)', 1)

                ELSE COALESCE(Comp_WebSite, '')

            END as "Company Domain Name",

            Comp_PrimaryUserId as "Company owner",

            Comp_CreatedDate as "Create Date",

            COALESCE(Comp_Revenue, 0) as "Annual Revenue",

            COALESCE(Comp_Employees, 0) as "Number of Employees",

            COALESCE(Comp_Sector, 'Other') as "Industry",

            'Customer' as "Lifecycle Stage"

        FROM raw_companies

        WHERE Comp_Name IS NOT NULL

          AND TRIM(Comp_Name) != ''

        """

        # Transform Contacts (normalize for HubSpot)

        contacts_query = """

        SELECT

            Pers_PersonId as "Record ID",

            COALESCE(Pers_FirstName, '') as "First Name",

            COALESCE(Pers_LastName, 'Unknown') as "Last Name",

            CASE

                WHEN Pers_FirstName IS NOT NULL AND Pers_LastName IS NOT NULL THEN

                    LOWER(Pers_FirstName || '.' || Pers_LastName || '@' ||

                          COALESCE(c.comp_domain, 'example.com'))

                ELSE 'unknown@example.com'

            END as "Email",

            COALESCE(Pers_Title, '') as "Job Title",

            Pers_CompanyId as "Associated Company",

            Pers_CreatedDate as "Create Date"

        FROM raw_persons p

        LEFT JOIN (

            SELECT

                Comp_CompanyId,

                CASE

                    WHEN Comp_WebSite LIKE 'www.%' THEN SUBSTR(Comp_WebSite, 5)

                    WHEN Comp_WebSite LIKE 'http%' THEN

                        REGEXP_EXTRACT(Comp_WebSite, 'https?://(?:www\.)?([^/]+)', 1)

                    ELSE COALESCE(Comp_WebSite, 'example.com')

                END as comp_domain

            FROM raw_companies

        ) c ON p.Pers_CompanyId = c.Comp_CompanyId

        WHERE COALESCE(Pers_FirstName, Pers_LastName) IS NOT NULL

        """

        # Transform Deals (from qualified opportunities)

        deals_query = """

        SELECT

            Oppo_OpportunityId as "Record ID",

            Oppo_Description as "Deal Name",

            Oppo_AssignedUserId as "Deal Owner",

            Oppo_PrimaryCompanyId as "Associated Company",

            Oppo_PrimaryPersonId as "Associated Contact",

            CASE

                WHEN Oppo_Stage = 'Closed Won' THEN 'closedwon'

                WHEN Oppo_Stage = 'Closed Lost' THEN 'closedlost'

                WHEN Oppo_Stage LIKE '%Negotiation%' THEN 'contractsent'

                ELSE 'qualifiedtobuy'

            END as "Deal Stage",

            Oppo_CreatedDate as "Create Date",

            'Sales Pipeline' as "Pipeline",

            COALESCE(Oppo_Total, Oppo_Forecast, 0) as "Amount",

            Oppo_TargetClose as "Close Date"

        FROM raw_opportunities  

        WHERE Oppo_Stage IN ('Closed Won', 'Closed Lost', 'Negotiation', 'Contract Sent')

           OR Oppo_Total > 0

        """

        # Execute transformations

        studies_df = self.conn.execute(studies_query).df()

        companies_df = self.conn.execute(companies_query).df()

        contacts_df = self.conn.execute(contacts_query).df()

        deals_df = self.conn.execute(deals_query).df()

        self.logger.info(f"Transformed data - Studies: {len(studies_df)}, Companies: {len(companies_df)}, Contacts: {len(contacts_df)}, Deals: {len(deals_df)}")

        return {

            'studies': studies_df,

            'companies': companies_df,

            'contacts': contacts_df,

            'deals': deals_df

        }

    def export_for_hubspot_import(self, transformed_data):

        """Export data as CSV files for HubSpot manual import"""

        output_dir = Path('hubspot_import_files')

        output_dir.mkdir(exist_ok=True)

        # Export each object type

        for object_type, df in transformed_data.items():

            file_path = output_dir / f'{object_type}_import.csv'

            df.to_csv(file_path, index=False)

            self.logger.info(f"Exported {len(df)} {object_type} records to {file_path}")

        # Create import instructions

        self.create_import_instructions(output_dir, transformed_data)

    def create_import_instructions(self, output_dir, transformed_data):

        """Create step-by-step import instructions"""

        instructions = """


## Step 5 associate legacy objects

# HubSpot Clean Association Builder
# Export HubSpot data → Create joins → Re-upload with HubSpot IDs

# Modern package management
if (!require("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(
  janitor,
  dplyr,
  stringr,
  readr,
  magrittr
)

# FILE PATHS - Exported from HubSpot
HUBSPOT_COMPANIES_EXPORT <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/hubspot_exports/companies_export.csv"
HUBSPOT_CONTACTS_EXPORT <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/hubspot_exports/contacts_export.csv"
HUBSPOT_DEALS_EXPORT <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/hubspot_exports/deals_export.csv"

# OUTPUT PATHS - For re-import with associations
ASSOCIATED_DEALS_OUTPUT <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/hubspot_reimport/deals_with_associations.csv"
ASSOCIATED_CONTACTS_OUTPUT <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/hubspot_reimport/contacts_with_associations.csv"

cat("=== HubSpot Clean Association Builder ===\n\n")

# Step 1: Load HubSpot exported data
cat("Step 1: Loading HubSpot exports...\n")

# Load companies
hubspot_companies <- HUBSPOT_COMPANIES_EXPORT %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

cat("Companies loaded:", nrow(hubspot_companies), "records\n")
cat("Company columns:", paste(names(hubspot_companies)[1:5], collapse = ", "), "...\n")

# Load contacts  
hubspot_contacts <- HUBSPOT_CONTACTS_EXPORT %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

cat("Contacts loaded:", nrow(hubspot_contacts), "records\n")
cat("Contact columns:", paste(names(hubspot_contacts)[1:5], collapse = ", "), "...\n")

# Load deals
hubspot_deals <- HUBSPOT_DEALS_EXPORT %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

cat("Deals loaded:", nrow(hubspot_deals), "records\n")
cat("Deal columns:", paste(names(hubspot_deals)[1:5], collapse = ", "), "...\n\n")

# Step 2: Inspect actual column structure after clean_names()
cat("Step 2: Inspecting actual column structure after clean_names()...\n")

cat("Companies columns after clean_names():\n")
print(names(hubspot_companies))
cat("\n")

cat("Contacts columns after clean_names():\n") 
print(names(hubspot_contacts))
cat("\n")

cat("Deals columns after clean_names():\n")
print(names(hubspot_deals))
cat("\n")

# Check specific fields we need
cat("Checking required fields:\n")
cat("- hubspot_companies has 'record_id':", "record_id" %in% names(hubspot_companies), "\n")
cat("- hubspot_contacts has 'record_id':", "record_id" %in% names(hubspot_contacts), "\n") 
cat("- hubspot_deals has 'deal_company_id':", "deal_company_id" %in% names(hubspot_deals), "\n")
cat("- hubspot_deals has 'deal_contact_id':", "deal_contact_id" %in% names(hubspot_deals), "\n")

# Check data types - CRITICAL for joins
cat("\nChecking data types (potential join issue):\n")
if ("record_id" %in% names(hubspot_companies)) {
  cat("- Companies record_id type:", class(hubspot_companies$record_id)[1], "\n")
  cat("- Companies record_id sample:", paste(head(hubspot_companies$record_id, 3), collapse = ", "), "\n")
}

if ("record_id" %in% names(hubspot_contacts)) {
  cat("- Contacts record_id type:", class(hubspot_contacts$record_id)[1], "\n") 
  cat("- Contacts record_id sample:", paste(head(hubspot_contacts$record_id, 3), collapse = ", "), "\n")
}

if ("deal_company_id" %in% names(hubspot_deals)) {
  cat("- Deals deal_company_id type:", class(hubspot_deals$deal_company_id)[1], "\n")
  cat("- Deals deal_company_id sample:", paste(head(hubspot_deals$deal_company_id, 3), collapse = ", "), "\n")
}

if ("deal_contact_id" %in% names(hubspot_deals)) {
  cat("- Deals deal_contact_id type:", class(hubspot_deals$deal_contact_id)[1], "\n")
  cat("- Deals deal_contact_id sample:", paste(head(hubspot_deals$deal_contact_id, 3), collapse = ", "), "\n")
}

if ("company_id" %in% names(hubspot_contacts)) {
  cat("- Contacts company_id type:", class(hubspot_contacts$company_id)[1], "\n")
  cat("- Contacts company_id sample:", paste(head(hubspot_contacts$company_id, 3), collapse = ", "), "\n")
}

if ("company_record_id" %in% names(hubspot_companies)) {
  cat("- Companies company_record_id type:", class(hubspot_companies$company_record_id)[1], "\n")
  cat("- Companies company_record_id sample:", paste(head(hubspot_companies$company_record_id, 3), collapse = ", "), "\n")
}

cat("\n")

# Step 3: Convert data types to ensure join compatibility
cat("Step 3: Converting data types for join compatibility...\n")

# Convert all ID fields to character to avoid type mismatches
hubspot_companies <- hubspot_companies %>%
  dplyr::mutate(
    record_id = as.character(record_id),
    company_record_id = as.character(company_record_id)
  )

hubspot_contacts <- hubspot_contacts %>%
  dplyr::mutate(
    record_id = as.character(record_id),
    company_id = as.character(company_id)
  )

hubspot_deals <- hubspot_deals %>%
  dplyr::mutate(
    record_id = as.character(record_id),
    deal_company_id = as.character(deal_company_id),
    deal_contact_id = as.character(deal_contact_id)
  )

cat("All ID fields converted to character type for consistent joins.\n\n")

# Step 4: Build Contact-Company associations using legacy IDs
cat("\nStep 3: Building Contact-Company associations...\n")

contacts_with_company_ids <- hubspot_contacts %>%
  dplyr::left_join(
    hubspot_companies %>%
      dplyr::select(hubspot_company_id = record_id, company_record_id, company_name),
    by = c("company_id" = "company_record_id")  # Legacy ID to Legacy ID
  ) %>%
  dplyr::mutate(
    company_association_status = dplyr::case_when(
      !is.na(company_name) ~ "SUCCESS",
      is.na(company_id) ~ "NO_COMPANY_ID",
      TRUE ~ "FAILED_MATCH"
    )
  )

cat("Contact-Company associations:\n")
contacts_with_company_ids %>%
  dplyr::count(company_association_status) %>%
  print()

# Step 4: Build Deal-Company associations  
cat("\nStep 4: Building Deal-Company associations...\n")

deals_with_company_ids <- hubspot_deals %>%
  dplyr::left_join(
    hubspot_companies %>% 
      dplyr::select(hubspot_company_id = record_id, company_name),  # Companies also have record_id after clean_names()
    by = c("deal_company_id" = "record_id")  # Both fields exist after janitor
  ) %>%
  dplyr::mutate(
    company_association_status = dplyr::case_when(
      !is.na(company_name) ~ "SUCCESS",
      is.na(deal_company_id) ~ "NO_COMPANY_ID", 
      TRUE ~ "FAILED_MATCH"
    )
  )

cat("Deal-Company associations:\n")
deals_with_company_ids %>%
  dplyr::count(company_association_status) %>%
  print()

# Step 5: Build Deal-Contact associations
cat("\nStep 5: Building Deal-Contact associations...\n")

deals_with_contact_ids <- deals_with_company_ids %>%
  dplyr::left_join(
    hubspot_contacts %>%
      dplyr::select(hubspot_contact_id = record_id, first_name, last_name),  # Contacts also have record_id after clean_names()
    by = c("deal_contact_id" = "record_id")  # Both fields exist after janitor
  ) %>%
  dplyr::mutate(
    contact_association_status = dplyr::case_when(
      !is.na(first_name) ~ "SUCCESS",
      is.na(deal_contact_id) ~ "NO_CONTACT_ID",
      TRUE ~ "FAILED_MATCH"
    )
  )

cat("Deal-Contact associations:\n")
deals_with_contact_ids %>%
  dplyr::count(contact_association_status) %>%
  print()
cat("\nStep 5: Creating final association datasets...\n")

# Final deals dataset
final_deals <- deals_with_contact_ids %>%
  dplyr::select(
    # Core deal fields
    deal_record_id = record_id,
    deal_name,
    pipeline,
    deal_stage,
    deal_status,
    deal_amount,
    deal_forecast,
    deal_certainty,
    
    # Deal details
    deal_type,
    deal_source,
    deal_brand,
    deal_notes,
    deal_category,
    
    # Dates and owner
    deal_created_date,
    deal_close_date,
    deal_owner,
    
    # ASSOCIATION IDs for HubSpot import
    deal_company_id,        # HubSpot Company Record ID
    deal_contact_id,        # HubSpot Contact Record ID
    
    # Verification fields
    company_name,           # Verify correct company
    contact_first_name = first_name,    # Verify correct contact
    contact_last_name = last_name,
    
    # Status tracking
    company_association_status,
    contact_association_status,
    
    # Metadata
    processing_date,
    transformation_notes
  )

# Final contacts dataset
final_contacts <- contacts_with_company_ids %>%
  dplyr::select(
    # Core contact fields
    contact_record_id = record_id,
    first_name,
    last_name,
    email,
    job_title,
    department,
    contact_status,
    original_source,
    
    # ASSOCIATION ID for HubSpot import
    legacy_company_id = company_id,     # Original legacy ID
    hubspot_company_id = company_record_id,  # HubSpot Company Record ID
    
    # Verification
    company_name,
    company_association_status,
    
    # Metadata
    create_date,
    last_modified_date
  )

# Step 6: Export final datasets
cat("\nStep 6: Exporting association-ready datasets...\n")

final_deals %>%
  readr::write_csv(ASSOCIATED_DEALS_OUTPUT)

cat("Deals exported:", nrow(final_deals), "records to", ASSOCIATED_DEALS_OUTPUT, "\n")

final_contacts %>%
  readr::write_csv(ASSOCIATED_CONTACTS_OUTPUT)

cat("Contacts exported:", nrow(final_contacts), "records to", ASSOCIATED_CONTACTS_OUTPUT, "\n")

# Step 7: Summary report
cat("\nStep 7: Association Summary Report\n")
cat(stringr::str_dup("=", 50), "\n")

cat("DEALS SUMMARY:\n")
cat("- Total deals:", nrow(final_deals), "\n")

company_success <- sum(final_deals$company_association_status == "SUCCESS", na.rm = TRUE)
cat("- Company associations:", company_success, 
    "(", round(company_success/nrow(final_deals)*100, 1), "%)\n")

contact_success <- sum(final_deals$contact_association_status == "SUCCESS", na.rm = TRUE)
cat("- Contact associations:", contact_success,
    "(", round(contact_success/nrow(final_deals)*100, 1), "%)\n")

cat("\nCONTACTS SUMMARY:\n")
cat("- Total contacts:", nrow(final_contacts), "\n")

contact_company_success <- sum(final_contacts$company_association_status == "SUCCESS", na.rm = TRUE)
cat("- Company associations:", contact_company_success,
    "(", round(contact_company_success/nrow(final_contacts)*100, 1), "%)\n")

cat("\nPIPELINE BREAKDOWN:\n")
pipeline_analysis <- final_deals %>%
  dplyr::group_by(pipeline, company_association_status) %>%
  dplyr::summarise(
    count = dplyr::n(),
    total_value = sum(deal_amount, na.rm = TRUE),
    .groups = "drop"
  )

print(pipeline_analysis)

cat("\n✅ Association building complete!\n")
cat("Ready for HubSpot re-import with native association IDs.\n")
## Step 6 associate communications

# CRM Legacy to New System Semantic Mapping
# Transform legacy communication fields to new CRM structure using dplyr

# Modern package management
if (!require("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(
  dplyr,
  stringr,
  readr,
  magrittr
)

# FILE PATHS
LEGACY_COMMUNICATIONS_PATH <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/legacy_data/communications_legacy.csv"
COMPANIES_LOOKUP_PATH <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/data_input/companies_lookup.csv"
CONTACTS_LOOKUP_PATH <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/data_input/contacts_lookup.csv"
DEALS_LOOKUP_PATH <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/data_input/deals_lookup.csv"
NEW_CRM_OUTPUT_PATH <- "C:/Users/ayaobama/Documents/AnthonySalesOps/Codebase/moderncrm/new_crm/communications_transformed.csv"

cat("=== CRM Semantic Mapping Pipeline ===\n\n")

# Step 1: Load legacy communications data
cat("Step 1: Loading legacy communications data...\n")
legacy_communications <- LEGACY_COMMUNICATIONS_PATH %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

cat("Legacy communications loaded:", nrow(legacy_communications), "records\n")

# Step 2: Load lookup tables for associations
cat("Step 2: Loading lookup tables...\n")

# Load company lookup (legacy ID to new CRM ID mapping)
companies_lookup <- COMPANIES_LOOKUP_PATH %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

# Load contacts lookup  
contacts_lookup <- CONTACTS_LOOKUP_PATH %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

# Load deals lookup
deals_lookup <- DEALS_LOOKUP_PATH %>%
  readr::read_csv(show_col_types = FALSE) %>%
  janitor::clean_names()

cat("Lookup tables loaded:\n")
cat("- Companies:", nrow(companies_lookup), "records\n")
cat("- Contacts:", nrow(contacts_lookup), "records\n") 
cat("- Deals:", nrow(deals_lookup), "records\n\n")

# Step 3: Semantic field mapping functions
cat("Step 3: Defining semantic mapping functions...\n")

# Function to map communication source to channel type
map_channel_type <- function(communication_source) {
  dplyr::case_when(
    stringr::str_detect(communication_source, "Email|email|E-mail") ~ "EMAIL",
    stringr::str_detect(communication_source, "Phone|phone|Call|call|Tel") ~ "CALL",
    stringr::str_detect(communication_source, "Meeting|meeting|Face") ~ "MEETING",
    stringr::str_detect(communication_source, "SMS|sms|Text") ~ "SMS",
    stringr::str_detect(communication_source, "Chat|chat|Slack|Teams") ~ "CHAT",
    stringr::str_detect(communication_source, "Social|LinkedIn|Twitter") ~ "SOCIAL_MEDIA",
    stringr::str_detect(communication_source, "Video|Zoom|WebEx") ~ "VIDEO_CALL",
    TRUE ~ "OTHER"
  )
}

# Function to map communication type to standardized values
map_communication_type <- function(communication_type) {
  dplyr::case_when(
    stringr::str_detect(communication_type, "Inbound|Incoming|Received") ~ "INBOUND",
    stringr::str_detect(communication_type, "Outbound|Outgoing|Sent") ~ "OUTBOUND", 
    TRUE ~ "UNKNOWN"
  )
}

# Function to create communication body from subject and notes
create_communication_body <- function(subject, notes) {
  dplyr::case_when(
    !is.na(notes) & notes != "" ~ paste0("Subject: ", subject, "\n\nNotes: ", notes),
    !is.na(subject) & subject != "" ~ paste0("Subject: ", subject),
    TRUE ~ "No content available"
  )
}

# Function to map activity assignment
map_activity_assigned_to <- function(from_field, to_field, communication_type) {
  dplyr::case_when(
    communication_type == "OUTBOUND" ~ from_field,
    communication_type == "INBOUND" ~ to_field,
    TRUE ~ from_field  # Default to from_field
  )
}

# Step 4: Perform semantic mapping with association lookups
cat("Step 4: Performing semantic mapping transformations...\n")

# Main transformation pipeline
transformed_communications <- legacy_communications %>%
  # Convert data types for consistent joins
  dplyr::mutate(
    record_id = as.character(record_id),
    company_id = as.character(company_id),
    contact_id = as.character(contact_id)
  ) %>%
  
  # Join with companies lookup to get new CRM company ID
  dplyr::left_join(
    companies_lookup %>%
      dplyr::select(legacy_company_id = company_record_id, new_company_id = record_id, company_name),
    by = c("company_id" = "legacy_company_id")
  ) %>%
  
  # Join with contacts lookup to get new CRM contact ID
  dplyr::left_join(
    contacts_lookup %>%
      dplyr::select(legacy_contact_id = contact_record_id, new_contact_id = record_id, 
                    contact_first_name = first_name, contact_last_name = last_name),
    by = c("contact_id" = "legacy_contact_id")
  ) %>%
  
  # Join with deals lookup to get new CRM deal IDs (many-to-many through company/contact)
  dplyr::left_join(
    deals_lookup %>%
      dplyr::select(deal_company_id = company_id, deal_contact_id = contact_id, new_deal_id = record_id),
    by = c("company_id" = "deal_company_id", "contact_id" = "deal_contact_id")
  ) %>%
  
  # Apply semantic mapping transformations
  dplyr::mutate(
    # SEMANTIC FIELD MAPPINGS
    
    # Record ID (direct mapping)
    new_record_id = record_id,
    
    # Channel Type (semantic mapping from Communication Source)
    channel_type = map_channel_type(communication_source),
    
    # Type (semantic mapping from Communication Type)  
    type = map_communication_type(communication_type),
    
    # Communication Body (combine Subject + Notes)
    communication_body = create_communication_body(communication_subject, communication_notes),
    
    # Associated Contact (from lookup)
    associated_contact = new_contact_id,
    
    # Activity Assigned To (conditional based on type)
    activity_assigned_to = map_activity_assigned_to(from, to, type),
    
    # Activity Date (direct mapping)
    activity_date = communication_date,
    
    # Associated Company (from lookup)
    associated_company = new_company_id,
    
    # Associated Deal (from lookup - could be multiple)
    associated_deal = new_deal_id,
    
    # Associated Conversation Object (derive from context)
    associated_conversation_object = dplyr::case_when(
      !is.na(new_deal_id) ~ paste0("DEAL:", new_deal_id),
      !is.na(new_contact_id) ~ paste0("CONTACT:", new_contact_id), 
      !is.na(new_company_id) ~ paste0("COMPANY:", new_company_id),
      TRUE ~ "STANDALONE"
    ),
    
    # Object Last Modified Date/Time (current timestamp)
    object_last_modified_date_time = Sys.time(),
    
    # Record Source (standardized)
    record_source = "LEGACY_MIGRATION",
    
    # Associated Contact IDs (handle multiple - for now single)
    associated_contact_i_ds = new_contact_id,
    
    # Associated Company IDs (handle multiple - for now single)
    associated_company_i_ds = new_company_id,
    
    # Associated Deal IDs (handle multiple)
    associated_deal_i_ds = new_deal_id,
    
    # Associated Conversation IDs (placeholder)
    associated_conversation_i_ds = paste0("CONV:", new_record_id)
  ) %>%
  
  # Add mapping status tracking
  dplyr::mutate(
    # Association Status Tracking
    company_mapping_status = dplyr::case_when(
      is.na(company_id) | company_id == "" ~ "NO_LEGACY_COMPANY_ID",
      !is.na(new_company_id) ~ "SUCCESS",
      TRUE ~ "FAILED_MATCH"
    ),
    
    contact_mapping_status = dplyr::case_when(
      is.na(contact_id) | contact_id == "" ~ "NO_LEGACY_CONTACT_ID", 
      !is.na(new_contact_id) ~ "SUCCESS",
      TRUE ~ "FAILED_MATCH"
    ),
    
    deal_mapping_status = dplyr::case_when(
      is.na(new_deal_id) ~ "NO_DEAL_ASSOCIATION",
      !is.na(new_deal_id) ~ "SUCCESS",
      TRUE ~ "FAILED_MATCH"
    ),
    
    # Overall mapping quality score
    mapping_quality_score = dplyr::case_when(
      company_mapping_status == "SUCCESS" & contact_mapping_status == "SUCCESS" & deal_mapping_status == "SUCCESS" ~ "EXCELLENT",
      company_mapping_status == "SUCCESS" & contact_mapping_status == "SUCCESS" ~ "GOOD",
      company_mapping_status == "SUCCESS" | contact_mapping_status == "SUCCESS" ~ "FAIR", 
      TRUE ~ "POOR"
    )
  )

cat("Semantic mapping transformations completed.\n")

# Step 5: Create final new CRM format dataset
cat("Step 5: Creating final new CRM format...\n")

new_crm_communications <- transformed_communications %>%
  dplyr::select(
    # NEW CRM FIELD STRUCTURE
    record_id = new_record_id,
    channel_type,
    type,
    communication_body,
    associated_contact,
    activity_assigned_to,
    activity_date,
    associated_company,
    associated_deal,
    associated_conversation_object,
    object_last_modified_date_time,
    record_source,
    associated_contact_i_ds,
    associated_company_i_ds, 
    associated_deal_i_ds,
    associated_conversation_i_ds,
    
    # QUALITY TRACKING FIELDS
    mapping_quality_score,
    company_mapping_status,
    contact_mapping_status,
    deal_mapping_status,
    
    # AUDIT TRAIL (LEGACY REFERENCES)
    legacy_record_id = record_id,
    legacy_company_id = company_id,
    legacy_contact_id = contact_id,
    legacy_subject = communication_subject,
    legacy_notes = communication_notes
  )

cat("Final new CRM format created:", nrow(new_crm_communications), "records\n")

# Step 6: Generate mapping quality report
cat("Step 6: Generating mapping quality report...\n")

# Overall mapping statistics
mapping_summary <- transformed_communications %>%
  dplyr::summarise(
    total_records = dplyr::n(),
    successful_company_mappings = sum(company_mapping_status == "SUCCESS", na.rm = TRUE),
    successful_contact_mappings = sum(contact_mapping_status == "SUCCESS", na.rm = TRUE),
    successful_deal_mappings = sum(deal_mapping_status == "SUCCESS", na.rm = TRUE),
    excellent_quality = sum(mapping_quality_score == "EXCELLENT", na.rm = TRUE),
    good_quality = sum(mapping_quality_score == "GOOD", na.rm = TRUE),
    fair_quality = sum(mapping_quality_score == "FAIR", na.rm = TRUE),
    poor_quality = sum(mapping_quality_score == "POOR", na.rm = TRUE)
  )

cat("\n=== MAPPING QUALITY REPORT ===\n")
cat("Total Records Processed:", mapping_summary$total_records, "\n")
cat("Company Mapping Success:", mapping_summary$successful_company_mappings, 
    "(", round(mapping_summary$successful_company_mappings/mapping_summary$total_records*100, 1), "%)\n")
cat("Contact Mapping Success:", mapping_summary$successful_contact_mappings,
    "(", round(mapping_summary$successful_contact_mappings/mapping_summary$total_records*100, 1), "%)\n")
cat("Deal Mapping Success:", mapping_summary$successful_deal_mappings,
    "(", round(mapping_summary$successful_deal_mappings/mapping_summary$total_records*100, 1), "%)\n")

cat("\nQuality Distribution:\n")
cat("- Excellent:", mapping_summary$excellent_quality, "\n")
cat("- Good:", mapping_summary$good_quality, "\n") 
cat("- Fair:", mapping_summary$fair_quality, "\n")
cat("- Poor:", mapping_summary$poor_quality, "\n")

# Channel type distribution
cat("\nChannel Type Distribution:\n")
channel_summary <- transformed_communications %>%
  dplyr::count(channel_type, sort = TRUE)
print(channel_summary)

# Communication type distribution  
cat("\nCommunication Type Distribution:\n")
type_summary <- transformed_communications %>%
  dplyr::count(type, sort = TRUE)
print(type_summary)

# Step 7: Export final dataset
cat("Step 7: Exporting new CRM format...\n")

new_crm_communications %>%
  readr::write_csv(NEW_CRM_OUTPUT_PATH)

cat("New CRM communications exported:", nrow(new_crm_communications), "records to", NEW_CRM_OUTPUT_PATH, "\n")

# Step 8: Generate field mapping documentation
cat("Step 8: Field mapping documentation...\n")

field_mapping_doc <- data.frame(
  Legacy_Field = c("Record ID", "Communication Subject", "Communication Notes", "Communication Source", 
                   "Communication Type", "Communication Status", "Communication Date", "Create Date",
                   "Company Name", "Contact First Name", "Contact Last Name", "Company ID", "Contact ID",
                   "Priority", "From", "To"),
  New_CRM_Field = c("record_id", "communication_body (part)", "communication_body (part)", "channel_type",
                    "type", "activity_status (derived)", "activity_date", "object_last_modified_date_time",
                    "associated_company (lookup)", "associated_contact (lookup)", "associated_contact (lookup)",
                    "associated_company_i_ds", "associated_contact_i_ds", "priority (derived)", 
                    "activity_assigned_to (conditional)", "activity_assigned_to (conditional)"),
  Mapping_Type = c("Direct", "Semantic Combination", "Semantic Combination", "Semantic Classification",
                   "Semantic Classification", "Derived", "Direct", "Timestamp",
                   "Lookup Join", "Lookup Join", "Lookup Join", "Foreign Key Lookup", "Foreign Key Lookup",
                   "Derived", "Conditional Logic", "Conditional Logic"),
  Transformation_Logic = c("1:1 mapping", "Subject + Notes concatenation", "Subject + Notes concatenation", 
                          "Email/Phone/Meeting classification", "Inbound/Outbound classification", 
                          "Status derivation", "Date format preservation", "Current timestamp",
                          "Company name lookup via ID", "Contact name lookup via ID", "Contact name lookup via ID",
                          "Legacy to new ID mapping", "Legacy to new ID mapping", "Business rules application",
                          "From/To conditional on direction", "From/To conditional on direction")
)

cat("\nFIELD MAPPING DOCUMENTATION:\n")
print(field_mapping_doc)

cat("\n✅ SEMANTIC MAPPING COMPLETE! ✅\n")
cat("Legacy CRM communications successfully transformed to new CRM structure.\n")
cat("All associations resolved through semantic matching and lookup joins.\n")

# HubSpot Import Instructions

  

## Prerequisites

1. ✅ Custom object created using dummy_studies_custom_object.csv

2. ✅ All custom properties configured

3. ✅ Associations set up between objects

  

## Import Order (IMPORTANT - we will Follow this sequence in the CRM process)

  

### Step 1: Import Companies

File: companies_import.csv

- Go to Contacts > Companies > Import

- Map columns to HubSpot properties

- Use "Record ID" for deduplication

  

### Step 2: Import Contacts  

File: contacts_import.csv

- Go to Contacts > Contacts > Import

- Map "Associated Company" to company associations

- Use "Record ID" for deduplication

  

### Step 3: Import Studies (Custom Object)

File: studies_import.csv  

- Go to Custom Objects > Studies > Import

- Map "Associated Company" and "Associated Contact"

- Use "Record ID" for deduplication

- **Note**: This will use the properties created by the dummy import

  

### Step 4: Import Deals

File: deals_import.csv

- Go to Sales > Deals > Import  

- Map associations to companies and contacts

- Use "Record ID" for deduplication

  
Success metrics
## Stage Mappings Applied

  

### Study Stages:

- Legacy → New HubSpot Stage

- Lead/Identification → 01 - Identification

- Qualification → 02 - Qualifiée

- Evaluation/Assessment → 03 - Evaluation technique

- Proposal/Construction → 04 - Construction propositions

- Negotiation → 05 - Négociation

  

### Study Status:

- Won/Gagnée → Gagnée

- Lost/Perdue → Perdue

- Abandon → Abandonnée

- Sleep/Sommeil → En Sommeil

- NoGo → NoGo

- Default → En cours

  

## Post-Import Tasks

1. Verify association mappings

2. Set up workflows for Study → Deal conversion

3. Configure reporting dashboards

4. Test data integrity

5. Delete dummy study record

  

## Statistics

"""

        for object_type, df in transformed_data.items():

            instructions += f"- {object_type.title()}: {len(df)} records\n"

        with open(output_dir / 'IMPORT_INSTRUCTIONS.md', 'w') as f:

            f.write(instructions)

        self.logger.info(f"Created import instructions at {output_dir}/IMPORT_INSTRUCTIONS.md")

  

# Updated Usage Example



Business rules (2)

let
    Source = Excel.CurrentWorkbook(){[Name="Table1"]}[Content],
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Record ID", Int64.Type}, {"Deal Name", type text}, {"Deal Owner", Int64.Type}, {"Pipeline", type text}, {"Deal Stage", type text}, {"Deal Type", type text}, {"Deal Source", type text}, {"Deal Forecast", Int64.Type}, {"Deal Amount", Int64.Type}, {"Deal Certainty", Int64.Type}, {"Deal Status", type text}, {"Deal Brand", type text}, {"Deal Notes", type text}, {"Deal Created Date", type number}, {"Deal Close Date", Int64.Type}, {"Deal Company ID", Int64.Type}, {"Deal Contact ID", Int64.Type}, {"Deal Category", type text}}),
    
    // Clean Deal Stage for Studies Pipeline
    #"Added Helper Column" = Table.AddColumn(#"Changed Type", "Deal Stage Clean", each 
        if [Pipeline] = "Studies Pipeline" 
        then Text.AfterDelimiter([Deal Stage], "- ") 
        else [Deal Stage], 
    type text),
    
    // Add Harmonized Stage mapping
    #"Added Harmonized Stage" = Table.AddColumn(#"Added Helper Column", "Deal Stage Harmonized", each 
        let
            // Clean and normalize inputs
            CleanStage = Text.Trim(Text.Clean([Deal Stage Clean])),
            CleanStatus = Text.Trim(Text.Clean(Text.Lower([Deal Status]))),
            
            // Extract base stage (remove + or - indicators)
            BaseStage = Text.Trim(
                if Text.EndsWith(CleanStage, " +") then Text.BeforeDelimiter(CleanStage, " +")
                else if Text.EndsWith(CleanStage, " -") then Text.BeforeDelimiter(CleanStage, " -")
                else CleanStage
            ),
            
            // Determine if positive (+) or negative (-) indicator
            IsPositive = Text.EndsWith(CleanStage, " +"),
            IsNegative = Text.EndsWith(CleanStage, " -"),
            
            // Main mapping logic
            MappedStage = 
                // Handle "Perdue" status - always maps to Closed Lost
                if Text.Contains(CleanStatus, "perdue") or Text.Contains(CleanStatus, "lost") then "Closed Lost"
                
                // Handle "Gagnée" status - always maps to Closed Won  
                else if Text.Contains(CleanStatus, "gagnée") or Text.Contains(CleanStatus, "gagnã©e") or Text.Contains(CleanStatus, "won") then "Closed Won"
                
                // Handle "Abandonnée" status - maps to stage with negative indicator
                else if Text.Contains(CleanStatus, "abandonnée") or Text.Contains(CleanStatus, "abandonnã©e") or Text.Contains(CleanStatus, "abandoned") then
                    if Text.Contains(BaseStage, "Identification") then "Identified -"
                    else if Text.Contains(BaseStage, "Negotiate") then "Negotiate -"
                    else BaseStage & " -"
                
                // Handle positive indicators (+)
                else if IsPositive then
                    if Text.Contains(BaseStage, "Identification") then "Identified +"
                    else if Text.Contains(BaseStage, "Negotiate") then "Negotiate +"
                    else BaseStage & " +"
                
                // Handle negative indicators (-)
                else if IsNegative then
                    if Text.Contains(BaseStage, "Identification") then "Identified -"
                    else if Text.Contains(BaseStage, "Negotiate") then "Negotiate -"
                    else BaseStage & " -"
                
                // Map common stage variations to standardized names
                else if Text.Contains(BaseStage, "Identification") then "Identified"
                else if Text.Contains(BaseStage, "Negotiate") then "Negotiate"
                else if Text.Contains(BaseStage, "Qualify") then "Qualified"
                else if Text.Contains(BaseStage, "Proposal") then "Proposal"
                else if Text.Contains(BaseStage, "Contract") then "Contract"
                
                // Default: return cleaned base stage
                else BaseStage
        in
            MappedStage,
    type text),
    
    // Reorder columns to show progression
    #"Reordered Columns" = Table.ReorderColumns(#"Added Harmonized Stage",{"Record ID", "Deal Name", "Deal Owner", "Pipeline", "Deal Stage", "Deal Stage Clean", "Deal Stage Harmonized", "Deal Type", "Deal Source", "Deal Forecast", "Deal Amount", "Deal Certainty", "Deal Status", "Deal Brand", "Deal Notes", "Deal Created Date", "Deal Close Date", "Deal Company ID", "Deal Contact ID", "Deal Category"}),
    
    // Optional: Remove intermediate column and use harmonized as final
    #"Removed Original Stage" = Table.RemoveColumns(#"Reordered Columns",{"Deal Stage", "Deal Stage Clean"}),
    #"Renamed Final Column" = Table.RenameColumns(#"Removed Original Stage",{{"Deal Stage Harmonized", "Deal Stage"}})

in
    #"Renamed Final Column"




