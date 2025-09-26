## **Cardinality Correction (Using (x,n) Notation)**

Your current model has some cardinality issues. Here's the corrected version:

### **Corrected Cardinalities:**

- **Contact ↔ Company**: `(0,1)` to `(0,n)` - A contact belongs to 0 or 1 company, a company can have 0 to many contacts
- **Company → Primary Contact**: `(0,1)` to `(0,1)` - A company has 0 or 1 primary contact, a contact can be primary for 0 or 1 company
- **Deal → Company**: `(1,1)` to `(0,n)` - A deal belongs to exactly 1 company, a company can have 0 to many deals
- **Deal → Contact**: `(0,1)` to `(0,n)` - A deal has 0 or 1 contact, a contact can have 0 to many deals
- Opportunity progress(mapping table) to Opportunity 
1. Communications to Companies (Many-to-One)

2. Communications to Contacts (Many-to-One)

3. Communications to Deals (Many-to-Many) - Optional

CRM Data Model - MERISE Style syntax
// This will serve to scaffold the model schemas
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%} {{ default_schema }}

    {%- else -%} {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
Document 


%Models
Former CRM model schema (Sage DB)
th job_details as (select * from {{ ref('src_job_details') }})

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as job_details_id,
    dim
    dim,
    dim,
    dim(,
    dim,
   dim,
    dim,
    dim,
    dim
from job_details

We already extracted the following csv, no need to connect to the end database

# Define new Hubspot CRM model

destination CRM model

Datamart HS_CRM :
th job_details as (select * from {{ ref('src_job_details') }})

select
{{ dbt_utils.generate_surrogate_key(['id']) }} as job_details_id,
    icalps_deal_id	icalps_company_id	icalps_contact_id	dealname	pipeline_name	icalps_dealstatus	dealstage_name	pipeline_and_dealstage_lookup_key	pipeline	dealstage	amount	icalps_dealforecast	icalps_dealcertainty	icalps_dealtype	brand	icalps_dealnotes	icalps_dealcategory	icalps_deal_created_date


With Fact

Deals
Companies
Contacts
Communications
 
 
 CRM Data Model in Obsidian - MERISE Representation ## 📊 **Entity Relationship Overview** ```mermaid erDiagram COMPANIES ||--o{ CONTACTS : employs COMPANIES ||--o| CONTACTS : "has primary" COMPANIES ||--o{ DEALS : "generates" CONT

I've created a comprehensive Obsidian-based CRM model that addresses your requirements:

## 🎯 **Key Features:**

### **1. Corrected Cardinalities (x,n notation):**

- **Contact → Company**: `(0,1)` to `(0,n)`
- **Company → Primary Contact**: `(0,1)` to `(0,1)`
- **Deal → Company**: `(1,1)` to `(0,n)`
- **Deal → Contact**: `(0,1)` to `(0,n)`

### **2. MERISE-Style Entity Representation:**

- **Base definitions** with field specifications
- **Relationship rules** clearly documented
- **Cardinality constraints** enforced through templates

### **3. DuckDB + DBT Native Features:**

- **`[[]]` linking syntax** for associations
- **Dataview queries** for dynamic relationship display
- **YAML frontmatter** for structured properties
- **Templates** for consistent entity creation

### **4. Visualization Approaches:**

#### **Graph View:**

- Color-coded nodes by entity type
- Automatic relationship visualization
- Filter capabilities by tags/types

5.ETL feature based on CSV


1st pipeline 
Database > DLT> CSV > DuckDB (+  loading)

each dlt ressource has specific abilities
ex disposition ="append"


ex : @dlt.ressource(write_disposition = "append")
using decorator enhaneces the functionnality of an existing function here we turn into a dlt ressource object

Note ressource can include incremental loading
## Functionnal specs
def load_csv (args)
df = pd.read_csv(file_path,args)

Note

1. Loading into duckdb

2. Building the pipeline

pipeline = dlt.pipeline (pipeline = "",destination = dlt.destinations.duckdb("dest(path)")).duckdb
f'working directory / "rel_path/file.duckdb"
using f string will prevent the pathlib error


duckdbui and db ver are useful way to control data



1. Exploring the data through the ui

This is an alternative to using xlwing server as the etl to
in the ui we can perform more datatransformation 
**Note : We should check the incremental loading video to implement what we want**
#### **Dashboard Queries:**

- Real-time association counts
- Business intelligence reports
- Health check validations

#### **Canvas Integration:**

- Process flow diagrams
- Relationship mapping
- Visual schema representation

### **5. Database-Like Experience:**

- **Structured bases** act like database tables
- **Entity templates** ensure data consistency
- **Bidirectional associations** through linking
- **Dynamic queries** for reporting


Schemas

1. Mart (old CRM)

with
    fct_job_ads as (select * from {{ ref('fct_job_ads') }}),
    dim_job_details as (select * from {{ ref('dim_job_details') }}),
    dim_occupation as (select * from {{ ref('dim_occupation') }})

select 
    dim,
    dim,
    dim,
    dim,
    dim,
    dim,
   dim,
    dim,
    dim, 
    dim,
    dim,
    dim
    from fct_deal - companies , contact 
left join dim_job_details jd on f.job_details_id = jd.job_details_id
left join dim_occupation o on f.occupation_id = o.occupation_id



```markdown
---
Type: Company
Legacy-Company-ID: 
HubSpot-ID: 
---

# Company Name Here

## 📊 Company Information
- **Legacy ID**: 
- **HubSpot ID**: 
- **Industry**: 
- **Owner**: 
- **Phone**: 
- **City**: 
- **Country**: 

## 👤 Primary Contact
- **Contact**: [[]]

## 🏢 All Company Contacts
- [[]]
- [[]]
- [[]]

## 💼 Company Deals
- [[]]
- [[]]
- [[]]

## 📝 Notes
```

### **2.2 Contact Template*****

1. Create new note in `Templates` folder
2. Name it: `Contact Template`
3. **Copy and paste**:

markdown

```markdown
---
Type: Contact
Legacy-Contact-ID: 
HubSpot-ID: 
---

# First Name Last Name

## 👤 Contact Information
- **Legacy ID**: 
- **HubSpot ID**: 
- **Email**: 
- **Phone**: 
- **Job Title**: 
- **Company**: [[]]

## 💼 Associated Deals
- [[]]
- [[]]
- [[]]

## 📝 Notes
```

### **2.3 Deal Template*****

1. Create new note in `Templates` folder
2. Name it: `Deal Template`
3. **Copy and paste**:

markdown

```markdown
---
Type: Deal
HubSpot-ID: 
---

# Deal Name Here

## 💰 Deal Information
- **HubSpot ID**: 
- **Stage**: 
- **Amount**: $
- **Owner**: 
- **Created**: 
- **Close Date**: 

## 🔗 Associations
- **Company**: [[]]
- **Contact**: [[]]

## 📝 Notes
```

---

## 🏗️ **Step 3: Creating the new records to populate the crm in csv

### **3.1 Create Company Note**



### **3.2 Fill Company Information**



markdown

```markdown
---
Type: Company
Legacy-Company-ID: 3
HubSpot-ID: 35999879490
---

# ACME Corp

## 📊 Company Information
- **Legacy ID**: 3
- **HubSpot ID**: 35999879490
- **Industry**: Technology
- **Owner**: John Smith
- **Phone**: +1-555-0123
- **City**: New York
- **Country**: USA

## 👤 Primary Contact
- **Contact**: [[Jane Doe]]

## 🏢 All Company Contacts
- [[Jane Doe]]
- [[Bob Wilson]]

## 💼 Company Deals
- [[MARSIC Project]]

## 📝 Notes
Great technology company, very responsive team.
```


Functionnal and programmatic sheet creation
````markdown
<%*
// Templater prompts - collect all data first
const hubspot_id = await tp.system.prompt("HubSpot ID");
const legacy_company_id = await tp.system.prompt("Legacy Company ID"); 
const company_name = await tp.system.prompt("Company Name");
const company_owner = await tp.system.prompt("Company Owner");
const industry = await tp.system.prompt("Industry");
const phone_number = await tp.system.prompt("Phone Number");
const city = await tp.system.prompt("City");
const country_region = await tp.system.prompt("Country/Region");
const primary_contact_id = await tp.system.prompt("Primary Contact Legacy ID (optional)", "");
const created_date = tp.date.now("YYYY-MM-DD");

// Set the file title
await tp.file.rename(company_name);
-%>
---
entity_type: company
hubspot_id: <% hubspot_id %>
legacy_company_id: <% legacy_company_id %>
company_name: <% company_name %>
company_owner: <% company_owner %>
industry: <% industry %>
phone_number: <% phone_number %>
city: <% city %>
country_region: <% country_region %>
primary_contact_id: <% primary_contact_id %>
created_date: <% created_date %>
deal_value_total: 0
tags: [company]
---

# <% company_name %>

## 📊 Company Information
- **Legacy ID**: `<% legacy_company_id %>`
- **HubSpot ID**: `<% hubspot_id %>`
- **Industry**: <% industry %>
- **Owner**: <% company_owner %>
- **Phone**: <% phone_number %>
- **Location**: <% city %>, <% country_region %>

## 👥 Associated Contacts
```dataview
TABLE 
  legacy_contact_id as "Contact ID",
  file.name as "Contact Name",
  email as "Email",
  job_title as "Job Title"
FROM "Contacts"
WHERE company_id = "<% legacy_company_id %>"
SORT file.name ASC
````

## 💼 Associated Deals

dataview

```dataview
TABLE 
  file.name as "Deal Name",
  deal_stage as "Stage", 
  deal_amount as "Amount",
  contact_id as "Contact ID",
  close_date as "Close Date"
FROM "Deals"
WHERE company_id = "<% legacy_company_id %>"
SORT deal_amount DESC
```

## 📞 Recent Communications

dataview

```dataview
TABLE 
  file.name as "Communication",
  communication_date as "Date",
  communication_source as "Source",
  communication_type as "Type",
  priority as "Priority",
  communication_status as "Status"
FROM "Communications"
WHERE company_id = "<% legacy_company_id %>"
SORT communication_date DESC
LIMIT 5
```

## 📈 Company Sites

dataview

````dataview
-- SIMPLIFIED: Company Aggregation Query with Contact JOIN
-- This creates the permanent table with contact information via simple JOIN

-- Step 1: Drop and recreate the permanent table with contact fields
IF EXISTS (SELECT * FROM sysobjects WHERE name='CompanyAggregation' AND xtype='U')
    DROP TABLE [CRMICALPS].[dbo].[CompanyAggregation];

CREATE TABLE [CRMICALPS].[dbo].[CompanyAggregation] (
    Comp_CompanyId INT,
    Parent_Company_ID INT,
    Comp_Name NVARCHAR(255),
    Location_Extracted NVARCHAR(100),
    Base_Company_Name NVARCHAR(255),
    Domain_Clean NVARCHAR(255),
    Comp_WebSite NVARCHAR(500),
    Comp_LibraryDir NVARCHAR(255),
    Comp_Type NVARCHAR(50),
    Comp_Status NVARCHAR(50),
    Record_Type NVARCHAR(50),
    Site_Order INT,
    Has_Multiple_Sites BIT,
    Source_Type NVARCHAR(50),
    -- Contact information from JOIN
    Contact_PersonId INT,
    Contact_FirstName NVARCHAR(100),
    Contact_LastName NVARCHAR(100),
    Contact_FullName NVARCHAR(255),
    Contact_Email NVARCHAR(255),
    Contact_Phone NVARCHAR(50),
    Created_Date DATETIME DEFAULT GETDATE()
);

-- Step 2: Create the CTEs and populate the table
WITH Domain_Groups AS (
    SELECT 
        -- Extract base company name (everything except last part after final space)
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END AS Base_Company_Name,
        
        -- Clean domain extraction (using only Comp_WebSite from dbo.Company)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END AS Domain_Clean,
        
        -- Count sites per domain group
        COUNT(*) AS Site_Count,
        
        -- Assign new parent ID (max existing ID + sequential)
        (SELECT MAX(Comp_CompanyId) FROM dbo.Company) + 
        ROW_NUMBER() OVER (ORDER BY 
            CASE 
                WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
                THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
                ELSE 'no-domain'
            END,
            CASE 
                WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
                THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                     LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
                ELSE TRIM(c.Comp_Name)
            END
        ) AS New_Parent_ID,
        
        -- Flag multi-site companies
        CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS Has_Multiple_Sites,
        
        -- Get first website for parent record
        MIN(c.Comp_WebSite) AS Parent_WebSite
        
    FROM dbo.Company c
    WHERE c.Comp_Deleted IS NULL
    GROUP BY 
        -- Base company name
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END,
        -- Clean domain (only from website)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END
    HAVING COUNT(*) > 1  -- Only multi-site companies
),

Company_Analysis AS (
    SELECT 
        c.Comp_CompanyId,
        c.Comp_Name,
        c.Comp_LibraryDir,
        c.Comp_WebSite,
        ISNULL(c.Comp_Type, 'Unknown') as Comp_Type,
        ISNULL(c.Comp_Status, 'Unknown') as Comp_Status,
        
        -- Extract location (last part after final space)
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(REVERSE(SUBSTRING(REVERSE(TRIM(c.Comp_Name)), 1, 
                 CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) - 1))))
            ELSE 'HQ'
        END AS Location_Extracted,
        
        -- Extract base company name
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END AS Base_Company_Name,
        
        -- Clean domain (only from website)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END AS Domain_Clean,
        
        -- Get parent ID from domain groups (XLOOKUP equivalent)
        COALESCE(dg.New_Parent_ID, c.Comp_CompanyId) AS Parent_Company_ID,
        
        -- Record type
        CASE 
            WHEN dg.New_Parent_ID IS NOT NULL THEN 'Site'
            ELSE 'Standalone'
        END AS Record_Type,
        
        -- Site order within company group
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
                    THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                         LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
                    ELSE TRIM(c.Comp_Name)
                END,
                CASE 
                    WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
                    THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
                    ELSE 'no-domain'
                END
            ORDER BY c.Comp_CompanyId
        ) AS Site_Order,
        
        -- Has multiple sites flag
        CASE WHEN dg.New_Parent_ID IS NOT NULL THEN 1 ELSE 0 END AS Has_Multiple_Sites
        
    FROM dbo.Company c
    LEFT JOIN Domain_Groups dg ON 
        -- Match base company name and domain
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END = dg.Base_Company_Name
        AND
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END = dg.Domain_Clean
    WHERE c.Comp_Deleted IS NULL
)

-- Step 3: Insert all records with contact JOIN
INSERT INTO [CRMICALPS].[dbo].[CompanyAggregation] (
    Comp_CompanyId, Parent_Company_ID, Comp_Name, Location_Extracted, Base_Company_Name,
    Domain_Clean, Comp_WebSite, Comp_LibraryDir, Comp_Type, Comp_Status,
    Record_Type, Site_Order, Has_Multiple_Sites, Source_Type,
    Contact_PersonId, Contact_FirstName, Contact_LastName, Contact_FullName, Contact_Email, Contact_Phone
)
SELECT 
    -- Sites (original companies) WITH CONTACT JOIN
    ca.Comp_CompanyId,
    ca.Parent_Company_ID,
    ca.Comp_Name,
    ca.Location_Extracted,
    ca.Base_Company_Name,
    ca.Domain_Clean,
    ca.Comp_WebSite,
    ca.Comp_LibraryDir,
    ca.Comp_Type,
    ca.Comp_Status,
    ca.Record_Type,
    ca.Site_Order,
    ca.Has_Multiple_Sites,
    'Original' as Source_Type,
    -- Contact information from JOIN
    p.Pers_PersonId as Contact_PersonId,
    p.Pers_FirstName as Contact_FirstName,
    p.Pers_LastName as Contact_LastName,
    p.Pers_FirstName + ' ' + p.Pers_LastName as Contact_FullName,
    p.pers_EmailAddress as Contact_Email,
    p.pers_telephone as Contact_Phone
FROM Company_Analysis ca
LEFT JOIN dbo.Person p ON ca.Comp_CompanyId = p.Pers_CompanyId AND p.Pers_Deleted IS NULL

UNION ALL

-- Parent Aggregators (no direct contacts, they aggregate from children)
SELECT 
    dg.New_Parent_ID as Comp_CompanyId,
    NULL as Parent_Company_ID,
    dg.Base_Company_Name as Comp_Name,
    'HQ' as Location_Extracted,
    dg.Base_Company_Name,
    dg.Domain_Clean,
    dg.Parent_WebSite as Comp_WebSite,
    dg.Base_Company_Name as Comp_LibraryDir,
    'Parent' as Comp_Type,
    'Active' as Comp_Status,
    'Parent_Aggregator' as Record_Type,
    0 as Site_Order,
    dg.Has_Multiple_Sites,
    'Generated' as Source_Type,
    NULL as Contact_PersonId,
    NULL as Contact_FirstName,
    NULL as Contact_LastName,
    NULL as Contact_FullName,
    NULL as Contact_Email,
    NULL as Contact_Phone
FROM Domain_Groups dg;

-- Step 4: Create indexes for performance
CREATE INDEX IX_CompanyAggregation_ParentID ON [CRMICALPS].[dbo].[CompanyAggregation](Parent_Company_ID);
CREATE INDEX IX_CompanyAggregation_RecordType ON [CRMICALPS].[dbo].[CompanyAggregation](Record_Type);
CREATE INDEX IX_CompanyAggregation_BaseCompany ON [CRMICALPS].[dbo].[CompanyAggregation](Base_Company_Name);
CREATE INDEX IX_CompanyAggregation_Domain ON [CRMICALPS].[dbo].[CompanyAggregation](Domain_Clean);
CREATE INDEX IX_CompanyAggregation_ContactID ON [CRMICALPS].[dbo].[CompanyAggregation](Contact_PersonId);

-- Step 5: Verify the table was created and populated with contact data
SELECT 
    'TABLE_WITH_CONTACTS' as Status,
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN Record_Type = 'Site' THEN 1 END) as Site_Records,
    COUNT(CASE WHEN Record_Type = 'Standalone' THEN 1 END) as Standalone_Records,
    COUNT(CASE WHEN Record_Type = 'Parent_Aggregator' THEN 1 END) as Parent_Records,
    COUNT(Contact_PersonId) as Records_With_Contacts,
    COUNT(*) - COUNT(Contact_PersonId) as Records_Without_Contacts
FROM [CRMICALPS].[dbo].[CompanyAggregation];

-- Step 6: Show companies with their contacts (your original query structure enhanced)
SELECT 
    ca.Comp_CompanyId,
    ca.Comp_Name,
    ca.Location_Extracted,
    ca.Base_Company_Name,
    ca.Record_Type,
    ca.Contact_PersonId,
    ca.Contact_FirstName,
    ca.Contact_LastName,
    ca.Contact_Email
FROM [CRMICALPS].[dbo].[CompanyAggregation] ca
WHERE ca.Contact_PersonId IS NOT NULL  -- Only show records with contacts
ORDER BY ca.Base_Company_Name, ca.Site_Order, ca.Contact_LastName;

-- Step 7: Show the enhanced version of your original query
SELECT 
    ca.Comp_CompanyId, 
    ca.Comp_Name, 
    ca.Comp_LibraryDir,
    ca.Contact_PersonId,
    ca.Contact_FirstName,
    ca.Contact_LastName,
    ca.Contact_Email,
    ca.Parent_Company_ID,
    ca.Record_Type
FROM [CRMICALPS].[dbo].[CompanyAggregation] ca
WHERE ca.Contact_PersonId IS NOT NULL
ORDER BY ca.Base_Company_Name, ca.Contact_LastName;

-- Step 2: Create the CTEs and populate the table
WITH Domain_Groups AS (
    SELECT 
        -- Extract base company name (everything except last part after final space)
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END AS Base_Company_Name,
        
        -- Clean domain extraction (using only Comp_WebSite from dbo.Company)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END AS Domain_Clean,
        
        -- Count sites per domain group
        COUNT(*) AS Site_Count,
        
        -- Assign new parent ID (max existing ID + sequential)
        (SELECT MAX(Comp_CompanyId) FROM dbo.Company) + 
        ROW_NUMBER() OVER (ORDER BY 
            CASE 
                WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
                THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
                ELSE 'no-domain'
            END,
            CASE 
                WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
                THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                     LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
                ELSE TRIM(c.Comp_Name)
            END
        ) AS New_Parent_ID,
        
        -- Flag multi-site companies
        CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS Has_Multiple_Sites,
        
        -- Get first website for parent record
        MIN(c.Comp_WebSite) AS Parent_WebSite
        
    FROM dbo.Company c
    WHERE c.Comp_Deleted IS NULL
    GROUP BY 
        -- Base company name
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END,
        -- Clean domain (only from website)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END
    HAVING COUNT(*) > 1  -- Only multi-site companies
),

Company_Analysis AS (
    SELECT 
        c.Comp_CompanyId,
        c.Comp_Name,
        c.Comp_LibraryDir,
        c.Comp_WebSite,
        ISNULL(c.Comp_Type, 'Unknown') as Comp_Type,
        ISNULL(c.Comp_Status, 'Unknown') as Comp_Status,
        
        -- Extract location (last part after final space)
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(REVERSE(SUBSTRING(REVERSE(TRIM(c.Comp_Name)), 1, 
                 CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) - 1))))
            ELSE 'HQ'
        END AS Location_Extracted,
        
        -- Extract base company name
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END AS Base_Company_Name,
        
        -- Clean domain (only from website)
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END AS Domain_Clean,
        
        -- Get parent ID from domain groups (XLOOKUP equivalent)
        COALESCE(dg.New_Parent_ID, c.Comp_CompanyId) AS Parent_Company_ID,
        
        -- Record type
        CASE 
            WHEN dg.New_Parent_ID IS NOT NULL THEN 'Site'
            ELSE 'Standalone'
        END AS Record_Type,
        
        -- Site order within company group
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
                    THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                         LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
                    ELSE TRIM(c.Comp_Name)
                END,
                CASE 
                    WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
                    THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
                    ELSE 'no-domain'
                END
            ORDER BY c.Comp_CompanyId
        ) AS Site_Order,
        
        -- Has multiple sites flag
        CASE WHEN dg.New_Parent_ID IS NOT NULL THEN 1 ELSE 0 END AS Has_Multiple_Sites,
        
        -- NEW: Contact aggregation for sites
        (SELECT COUNT(*) 
         FROM dbo.Person p 
         WHERE p.Pers_CompanyId = c.Comp_CompanyId 
         AND p.Pers_Deleted IS NULL) AS Contact_Count,
        
        -- Primary contact (first contact alphabetically)
        (SELECT TOP 1 p.Pers_FirstName + ' ' + p.Pers_LastName
         FROM dbo.Person p 
         WHERE p.Pers_CompanyId = c.Comp_CompanyId 
         AND p.Pers_Deleted IS NULL
         ORDER BY p.Pers_LastName, p.Pers_FirstName) AS Primary_Contact_Name,
        
        -- Primary contact email
        (SELECT TOP 1 p.pers_EmailAddress
         FROM dbo.Person p 
         WHERE p.Pers_CompanyId = c.Comp_CompanyId 
         AND p.Pers_Deleted IS NULL
         AND p.pers_EmailAddress IS NOT NULL
         ORDER BY p.Pers_LastName, p.Pers_FirstName) AS Primary_Contact_Email,
        
        -- All contact names (comma separated)
        (SELECT STRING_AGG(p.Pers_FirstName + ' ' + p.Pers_LastName, ', ')
         FROM dbo.Person p 
         WHERE p.Pers_CompanyId = c.Comp_CompanyId 
         AND p.Pers_Deleted IS NULL) AS All_Contact_Names,
        
        -- All contact emails (comma separated)
        (SELECT STRING_AGG(p.pers_EmailAddress, ', ')
         FROM dbo.Person p 
         WHERE p.Pers_CompanyId = c.Comp_CompanyId 
         AND p.Pers_Deleted IS NULL
         AND p.pers_EmailAddress IS NOT NULL) AS All_Contact_Emails
        
    FROM dbo.Company c
    LEFT JOIN Domain_Groups dg ON 
        -- Match base company name and domain
        CASE 
            WHEN CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))) > 0 
            THEN LTRIM(RTRIM(LEFT(TRIM(c.Comp_Name), 
                 LEN(TRIM(c.Comp_Name)) - CHARINDEX(' ', REVERSE(TRIM(c.Comp_Name))))))
            ELSE TRIM(c.Comp_Name)
        END = dg.Base_Company_Name
        AND
        CASE 
            WHEN c.Comp_WebSite IS NOT NULL AND c.Comp_WebSite != ''
            THEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(c.Comp_WebSite, 'https://', ''), 'http://', ''), 'www.', ''), '/', ''))
            ELSE 'no-domain'
        END = dg.Domain_Clean
    WHERE c.Comp_Deleted IS NULL
)

-- Step 3: Insert all records into the permanent table
INSERT INTO [CRMICALPS].[dbo].[CompanyAggregation] (
    Comp_CompanyId, Parent_Company_ID, Comp_Name, Location_Extracted, Base_Company_Name,
    Domain_Clean, Comp_WebSite, Comp_LibraryDir, Comp_Type, Comp_Status,
    Record_Type, Site_Order, Has_Multiple_Sites, Source_Type,
    Contact_Count, Primary_Contact_Name, Primary_Contact_Email, All_Contact_Names, All_Contact_Emails
)
SELECT 
    -- Sites (original companies) with contact information
    ca.Comp_CompanyId,
    ca.Parent_Company_ID,
    ca.Comp_Name,
    ca.Location_Extracted,
    ca.Base_Company_Name,
    ca.Domain_Clean,
    ca.Comp_WebSite,
    ca.Comp_LibraryDir,
    ca.Comp_Type,
    ca.Comp_Status,
    ca.Record_Type,
    ca.Site_Order,
    ca.Has_Multiple_Sites,
    'Original' as Source_Type,
    ca.Contact_Count,
    ca.Primary_Contact_Name,
    ca.Primary_Contact_Email,
    ca.All_Contact_Names,
    ca.All_Contact_Emails
FROM Company_Analysis ca

UNION ALL

-- Parent Aggregators (with aggregated contact counts from all child sites)
SELECT 
    dg.New_Parent_ID as Comp_CompanyId,
    NULL as Parent_Company_ID,  -- Parents have no parent
    dg.Base_Company_Name as Comp_Name,
    'HQ' as Location_Extracted,
    dg.Base_Company_Name,
    dg.Domain_Clean,
    dg.Parent_WebSite as Comp_WebSite,
    dg.Base_Company_Name as Comp_LibraryDir,
    'Parent' as Comp_Type,
    'Active' as Comp_Status,
    'Parent_Aggregator' as Record_Type,
    0 as Site_Order,  -- Parents get order 0
    dg.Has_Multiple_Sites,
    'Generated' as Source_Type,
    -- Aggregate contact count from all child sites
    (SELECT SUM(ca.Contact_Count) 
     FROM Company_Analysis ca 
     WHERE ca.Base_Company_Name = dg.Base_Company_Name 
     AND ca.Domain_Clean = dg.Domain_Clean) AS Contact_Count,
    -- Primary contact from largest site
    (SELECT TOP 1 ca.Primary_Contact_Name
     FROM Company_Analysis ca 
     WHERE ca.Base_Company_Name = dg.Base_Company_Name 
     AND ca.Domain_Clean = dg.Domain_Clean
     ORDER BY ca.Contact_Count DESC) AS Primary_Contact_Name,
    -- Primary contact email from largest site
    (SELECT TOP 1 ca.Primary_Contact_Email
     FROM Company_Analysis ca 
     WHERE ca.Base_Company_Name = dg.Base_Company_Name 
     AND ca.Domain_Clean = dg.Domain_Clean
     ORDER BY ca.Contact_Count DESC) AS Primary_Contact_Email,
    -- All contacts from all child sites
    (SELECT STRING_AGG(ca.All_Contact_Names, ', ')
     FROM Company_Analysis ca 
     WHERE ca.Base_Company_Name = dg.Base_Company_Name 
     AND ca.Domain_Clean = dg.Domain_Clean
     AND ca.All_Contact_Names IS NOT NULL) AS All_Contact_Names,
    -- All emails from all child sites
    (SELECT STRING_AGG(ca.All_Contact_Emails, ', ')
     FROM Company_Analysis ca 
     WHERE ca.Base_Company_Name = dg.Base_Company_Name 
     AND ca.Domain_Clean = dg.Domain_Clean
     AND ca.All_Contact_Emails IS NOT NULL) AS All_Contact_Emails
FROM Domain_Groups dg;

-- Step 4: Create indexes for performance
CREATE INDEX IX_CompanyAggregation_ParentID ON [CRMICALPS].[dbo].[CompanyAggregation](Parent_Company_ID);
CREATE INDEX IX_CompanyAggregation_RecordType ON [CRMICALPS].[dbo].[CompanyAggregation](Record_Type);
CREATE INDEX IX_CompanyAggregation_BaseCompany ON [CRMICALPS].[dbo].[CompanyAggregation](Base_Company_Name);
CREATE INDEX IX_CompanyAggregation_Domain ON [CRMICALPS].[dbo].[CompanyAggregation](Domain_Clean);
CREATE INDEX IX_CompanyAggregation_ContactCount ON [CRMICALPS].[dbo].[CompanyAggregation](Contact_Count);

-- Step 5: Verify the table was created and populated with contact data
SELECT 
    'TABLE_CREATED_WITH_CONTACTS' as Status,
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN Record_Type = 'Site' THEN 1 END) as Site_Records,
    COUNT(CASE WHEN Record_Type = 'Standalone' THEN 1 END) as Standalone_Records,
    COUNT(CASE WHEN Record_Type = 'Parent_Aggregator' THEN 1 END) as Parent_Records,
    SUM(Contact_Count) as Total_Contacts,
    AVG(CAST(Contact_Count as FLOAT)) as Avg_Contacts_Per_Site
FROM [CRMICALPS].[dbo].[CompanyAggregation];

-- Step 6: Show companies with most contacts
SELECT TOP 10
    Record_Type,
    Base_Company_Name,
    Comp_Name,
    Location_Extracted,
    Contact_Count,
    Primary_Contact_Name,
    Primary_Contact_Email
FROM [CRMICALPS].[dbo].[CompanyAggregation]
WHERE Contact_Count > 0
ORDER BY Contact_Count DESC, Base_Company_Name;

-- Step 7: Show parent companies with total contact rollup
SELECT 
    Comp_Name as Parent_Company,
    Location_Extracted,
    Contact_Count as Total_Child_Contacts,
    All_Contact_Names
FROM [CRMICALPS].[dbo].[CompanyAggregation]
WHERE Record_Type = 'Parent_Aggregator'
AND Contact_Count > 0
ORDER BY Contact_Count DESC;
````

### 👤 Contact Details

dataview

```dataview
TABLE 
  file.name as "Contact Name",
  email as "Email",
  job_title as "Job Title",
  phone_number as "Phone"
FROM "Contacts"
WHERE legacy_contact_id = "<% contact_id %>"
```

### 💼 Related Deals

dataview

```dataview
TABLE 
  file.name as "Deal Name",
  deal_stage as "Stage",
  deal_amount as "Amount",
  deal_owner as "Owner"
FROM "Deals"
WHERE company_id = "<% company_id %>" OR contact_id = "<% contact_id %>"
SORT deal_amount DESC
```


## 📈 Opportunity status Timeline

dataview

```dataview
TABLE 
  communication_date as "Date",
  file.name as "Subject",
  communication_source as "Channel",
  communication_type as "Type",
  priority as "Priority",
  communication_status as "Status",
  company_id as "Company",
  contact_id as "Contact"
FROM "Communications"
WHERE entity_type = "communication"
SORT communication_date DESC
LIMIT 20
```

## 🏢 Company Communication Activity

dataview

```dataview
TABLE WITHOUT ID
  company_id as "Company ID",
  length(rows) as "Total Communications",
  length(filter(rows, (x) => x.communication_type = "Inbound")) as "Inbound",
  length(filter(rows, (x) => x.communication_type = "Outbound")) as "Outbound",
  max(rows.communication_date) as "Last Contact"
FROM "Communications"
WHERE entity_type = "communication" AND company_id != null AND company_id != ""
GROUP BY company_id
SORT "Total Communications" DESC
LIMIT 15
```

## 👥 Contact Engagement Levels

dataview

```dataview
TABLE WITHOUT ID
  contact_id as "Contact ID",
  length(rows) as "Total Interactions",
  length(filter(rows, (x) => x.priority = "High")) as "High Priority",
  length(filter(rows, (x) => x.communication_source = "Email")) as "Email",
  length(filter(rows, (x) => x.communication_source = "Phone")) as "Phone",
  max(rows.communication_date) as "Last Contact"
FROM "Communications"
WHERE entity_type = "communication" AND contact_id != null AND contact_id != ""
GROUP BY contact_id
SORT "Total Interactions" DESC
LIMIT 15
```

## 📅 Communication Status Tracking

dataview

```dataview
TABLE WITHOUT ID
  communication_status as "Status",
  length(rows) as "Count",
  communication_source as "Primary Channel"
FROM "Communications"
WHERE entity_type = "communication"
GROUP BY communication_status
SORT "Count" DESC
```


```dataview
TABLE 
  file.name as "Subject",
  communication_date as "Original Date",
  communication_source as "Channel",
  company_id as "Company",
  contact_id as "Contact",
  priority as "Priority"
FROM "Communications"
WHERE entity_type = "communication" AND communication_status = "Scheduled"
SORT communication_date ASC
```

````

### **2.2 Contact Template (Corrected)**
Create: `Templates/Contact-Template.md`
```markdown
<%*
// Templater prompts - collect all data first
const hubspot_id = await tp.system.prompt("HubSpot ID");
const legacy_contact_id = await tp.system.prompt("Legacy Contact ID");
const first_name = await tp.system.prompt("First Name");
const last_name = await tp.system.prompt("Last Name");
const email = await tp.system.prompt("Email");
const phone_number = await tp.system.prompt("Phone Number", "");
const job_title = await tp.system.prompt("Job Title", "");
const company_id = await tp.system.prompt("Company Legacy ID");
const created_date = tp.date.now("YYYY-MM-DD");

// Set the file title
await tp.file.rename(`${first_name} ${last_name}`);
-%>
---
entity_type: contact
hubspot_id: <% hubspot_id %>
legacy_contact_id: <% legacy_contact_id %>
first_name: <% first_name %>
last_name: <% last_name %>
email: <% email %>
phone_number: <% phone_number %>
job_title: <% job_title %>
company_id: <% company_id %>
created_date: <% created_date %>
tags: [contact]
---

# <% first_name %> <% last_name %>

## 👤 Contact Information
- **Legacy ID**: `<% legacy_contact_id %>`
- **HubSpot ID**: `<% hubspot_id %>`
- **Email**: <% email %>
- **Phone**: <% phone_number %>
- **Job Title**: <% job_title %>
- **Company ID**: `<% company_id %>`

## 🏢 Company Information
```dataview
TABLE 
  file.name as "Company Name",
  industry as "Industry",
  company_owner as "Owner",
  city as "City"
FROM "Companies"
WHERE legacy_company_id = "<% company_id %>"
````

## 💼 Associated Deals

dataview

```dataview
TABLE 
  file.name as "Deal Name",
  deal_stage as "Stage",
  deal_amount as "Amount", 
  company_id as "Company ID",
  close_date as "Close Date"
FROM "Deals"
WHERE contact_id = "<% legacy_contact_id %>"
SORT deal_amount DESC
```

## 📞 Communications History

dataview

```dataview
TABLE 
  file.name as "Communication",
  communication_date as "Date",
  communication_source as "Source",
  communication_type as "Type",
  communication_subject as "Subject",
  priority as "Priority"
FROM "Communications"
WHERE contact_id = "<% legacy_contact_id %>"
SORT communication_date DESC
LIMIT 10
```

## 📊 Contact Metrics

dataview

```dataview
TABLE WITHOUT ID
  length(rows) as "Total Deals Managed",
  sum(rows.deal_amount) as "Total Deal Value"
- [ ] FROM "Deals"
WHERE contact_id = "<% legacy_contact_id %>"
```

````

### **2.3 Deal Template (Corrected)**
Create: `Templates/Deal-Template.md`
```markdown
<%*
// Templater prompts - collect all data first
const hubspot_id = await tp.system.prompt("HubSpot ID");
const deal_name = await tp.system.prompt("Deal Name");
const deal_stage = await tp.system.prompt("Deal Stage");
const deal_amount = await tp.system.prompt("Deal Amount (numbers only)", "0");
const company_id = await tp.system.prompt("Company Legacy ID");
const contact_id = await tp.system.prompt("Contact Legacy ID", "");
const deal_owner = await tp.system.prompt("Deal Owner");
const close_date = await tp.system.prompt("Close Date (YYYY-MM-DD)", "");
const created_date = tp.date.now("YYYY-MM-DD");

// Set the file title
await tp.file.rename(deal_name);
-%>
---
entity_type: deal
hubspot_id: <% hubspot_id %>
deal_name: <% deal_name %>
deal_stage: <% deal_stage %>
deal_amount: <% deal_amount %>
company_id: <% company_id %>
contact_id: <% contact_id %>
deal_owner: <% deal_owner %>
close_date: <% close_date %>
created_date: <% created_date %>
tags: [deal]
---

# <% deal_name %>

## 💰 Deal Information
- **HubSpot ID**: `<% hubspot_id %>`
- **Stage**: <% deal_stage %>
- **Amount**: $<% deal_amount %>
- **Owner**: <% deal_owner %>
- **Close Date**: <% close_date %>
- **Created**: <% created_date %>

## 🔗 Associations
- **Company ID**: `<% company_id %>`
- **Contact ID**: `<% contact_id %>`

## 🏢 Company Details
```dataview
TABLE 
  file.name as "Company Name",
  industry as "Industry",
  company_owner as "Owner",
  phone_number as "Phone"
FROM "Companies"
WHERE legacy_company_id = "<% company_id %>"
````


Company linkedin


Deal calculated columns