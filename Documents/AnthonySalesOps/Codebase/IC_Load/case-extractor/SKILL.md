---
name: case-extractor
description: Extract Case/Support Ticket data from SQL Server with Company and Person denormalization. Use this skill to extract case data into Bronze layer with type-safe dataclasses.
---

# Case Extractor

## Overview

Extracts Case/Support Ticket data from SQL Server CRM database with automatic denormalization of Company and Person information through JOINs. Outputs Bronze layer CSV and Python dataclass instances for downstream processing.

## When to Use This Skill

- **Extract case/support ticket data** from CRM database
- **Denormalize company and person** information into case records
- **Generate Bronze layer CSV** with "Bronze_Cases" prefix
- **Create type-safe Case dataclass** instances
- **Track case lifecycle** (opened, closed, status, priority)

## Entity Properties

### Core Case Fields
- `Case_CaseId` - Unique case identifier
- `Case_PrimaryCompanyId` - FK to Company
- `Case_PrimaryPersonId` - FK to Person (Contact)
- `Case_AssignedUserId` - Assigned user/owner
- `Case_Status` - Case status (Open, Closed, Pending)
- `Case_Stage` - Current stage
- `Case_Priority` - Priority level (High, Medium, Low)
- `Case_Opened` - Date/time opened
- `Case_Closed` - Date/time closed

### Denormalized Company Fields (from JOIN)
- `Company_Name` - Company name
- `Company_WebSite` - Company website

### Denormalized Person Fields (from JOIN)
- `Person_FirstName` - Contact first name
- `Person_LastName` - Contact last name
- `Person_EmailAddress` - Contact email

## Cardinality Relationships

- **Case → Company**: many:1 (via `Case_PrimaryCompanyId`)
- **Case → Person**: many:1 (via `Case_PrimaryPersonId`)
- **Case → User**: many:1 (via `Case_AssignedUserId`)

## SQL Query

```sql
SELECT
    c.[Case_CaseId],
    c.[Case_PrimaryCompanyId],
    comp.[Comp_Name] AS Company_Name,
    comp.[Comp_WebSite] AS Company_WebSite,
    c.[Case_PrimaryPersonId],
    p.[Pers_FirstName] AS Person_FirstName,
    p.[Pers_LastName] AS Person_LastName,
    v.[Emai_EmailAddress] AS Person_EmailAddress,
    c.[Case_AssignedUserId],
    c.[Case_ChannelId],
    c.[Case_Description],
    c.[Case_Status],
    c.[Case_Stage],
    c.[Case_Priority],
    c.[Case_Opened],
    c.[Case_Closed]
FROM [CRMICALPS].[dbo].[vCases] c
LEFT JOIN [CRMICALPS].[dbo].[Company] comp
    ON c.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]
LEFT JOIN [CRMICALPS].[dbo].[Person] p
    ON c.[Case_PrimaryPersonId] = p.[Pers_PersonId]
LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v
    ON c.[Case_PrimaryPersonId] = v.[Pers_PersonId]
```

## Generated Dataclass

```python
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class Case:
    """Case entity with denormalized Company and Person fields"""
    case_id: int
    primary_company_id: Optional[int]
    primary_person_id: Optional[int]
    assigned_user_id: Optional[int]
    status: Optional[str]
    stage: Optional[str]
    priority: Optional[str]
    description: Optional[str]
    opened: Optional[datetime]
    closed: Optional[datetime]

    # Denormalized Company fields
    company_name: Optional[str]
    company_website: Optional[str]

    # Denormalized Person fields
    person_first_name: Optional[str]
    person_last_name: Optional[str]
    person_email: Optional[str]
```

## Usage

### Basic Extraction

```python
from scripts.case_extractor import CaseExtractor

extractor = CaseExtractor(
    connection_string="your_connection_string"
)

# Extract all cases
cases = extractor.extract()

# Output: List[Case] dataclass instances
print(f"Extracted {len(cases)} cases")

# Save to Bronze CSV
extractor.save_to_bronze(cases, "Bronze_Cases.csv")
```

### Filtered Extraction

```python
# Extract only open cases
open_cases = extractor.extract(
    filter_clause="WHERE c.Case_Status = 'Open'"
)

# Extract recent cases
recent_cases = extractor.extract(
    filter_clause="WHERE c.Case_Opened >= DATEADD(month, -3, GETDATE())"
)
```

### Integration with Dataframe

```python
import pandas as pd

# Extract cases
cases = extractor.extract()

# Convert to DataFrame
from dataframe_dataclass_converter import DataFrameConverter
converter = DataFrameConverter()
df = converter.dataclasses_to_dataframe(cases)

# Now use with DuckDB or other tools
print(df.head())
```

## Resources

See `scripts/case_extractor.py` for implementation.
