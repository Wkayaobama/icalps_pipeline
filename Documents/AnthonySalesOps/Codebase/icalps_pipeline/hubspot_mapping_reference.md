# HubSpot Object Mapping Reference

This file serves as a comprehensive lookup reference for mapping IC'ALPS legacy data to HubSpot objects during data transformation.

## 📊 Input CSV Files Overview

### Available Input Files
- `Legacy_companies.csv` - Company data
- `Legacy_Opportunities.csv` - Deal/Opportunity data
- `Legacy_persons.csv` - Contact data
- `Legacy_comm.csv` - Communication data
- `legacy_socialnetworks.csv` - Social network links
- `combination_set.csv` - Status/Stage combinations
- `combination_set._pipeline.csv` - Pipeline combinations

## 🏢 Companies Mapping

### HubSpot Object: **Companies**
**Source File**: `Legacy_companies.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `Comp_CompanyId` | Custom: `icalps_company_id` | Legacy company ID | Direct mapping for reference |
| `Comp_Name` | `name` | Company name | Direct mapping, clean trailing spaces |
| `Comp_Website` | `website` | Company website | Clean URL format, add protocol if missing |
| `Oppo_OpportunityId` | Association | Link to deals | Use for company-deal associations |
| `Oppo_Description` | Association metadata | Deal context | Store in association for reference |

### Data Quality Notes
- Companies appear with multiple opportunity associations
- Website field may contain NULL values
- Company names should be deduplicated by `Comp_CompanyId`

## 👥 Contacts Mapping

### HubSpot Object: **Contacts**
**Source File**: `Legacy_persons.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `Pers_PersonId` | Custom: `icalps_person_id` | Legacy person ID | Direct mapping for reference |
| `Pers_FirstName` | `firstname` | Contact first name | Direct mapping, capitalize first letter |
| `Pers_LastName` | `lastname` | Contact last name | Direct mapping, capitalize first letter |
| `Pers_EmailAddress` | `email` | Primary email | Direct mapping, validate email format |
| `Comp_CompanyId` | Company Association | Associated company | Link to company via company ID |
| `Comp_Name` | Company Association | Company name | For validation/reference |
| `Oppo_OpportunityId` | Deal Association | Associated deals | Link to deals via opportunity ID |
| `Oppo_Description` | Deal Association | Deal context | Store in association metadata |

### Data Quality Notes
- Contacts have many-to-many relationships with opportunities
- Email validation required
- Company associations should be validated against company data

## 💼 Deals/Opportunities Mapping

### HubSpot Object: **Deals**
**Source File**: `Legacy_Opportunities.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `Oppo_OpportunityId` | Custom: `icalps_opportunity_id` | Legacy opportunity ID | Direct mapping for reference |
| `Oppo_Description` | `dealname` | Deal name/description | Direct mapping, truncate if > 255 chars |
| `Oppo_PrimaryCompanyId` | Company Association | Primary company | Link to company via company ID |
| `Oppo_PrimaryPersonId` | Contact Association | Primary contact | Link to contact via person ID |
| `Oppo_AssignedUserId` | `hubspot_owner_id` | Deal owner | Map to HubSpot user IDs |
| `Oppo_Type` | Custom: `deal_type` | Deal type (FTK, Preetude) | Map to pipeline selection |
| `Oppo_Product` | Custom: `product` | Product information | Direct mapping |
| `Oppo_Source` | `hs_lead_source` | Lead source | Map to HubSpot lead sources |
| `Oppo_Note` | `description` | Deal description | Clean and format |
| `Oppo_CustomerRef` | Custom: `customer_reference` | Customer reference | Direct mapping |
| `Oppo_Status` | `dealstage` | Current deal stage | **See Pipeline Mapping below** |
| `Oppo_Stage` | `dealstage` | Current deal stage | **See Pipeline Mapping below** |
| `Oppo_Forecast` | `amount` | Deal forecast amount | Convert to number, handle currency |
| `Oppo_Certainty` | Custom: `deal_certainty` | Confidence percentage | Direct mapping (0-100) |
| `Oppo_Priority` | Custom: `priority` | Deal priority | Map High/Normal/Low |
| `Oppo_TargetClose` | `closedate` | Target close date | Convert date format |
| `Oppo_CreatedDate` | `createdate` | Created date | Convert date format |
| `Oppo_UpdatedDate` | `hs_lastmodifieddate` | Last modified | Convert date format |
| `Oppo_Total` | Custom: `total_amount` | Total deal value | Convert to number |
| `oppo_cout` | Custom: `deal_cost` | Deal cost | Convert to number |

### Pipeline and Stage Mapping

#### Pipeline Selection Logic
```python
# Based on Oppo_Type field
if Oppo_Type == "Preetude":
    pipeline = "Studies Pipeline"
elif Oppo_Type == "FTK" or Oppo_Type is NULL:
    pipeline = "Sales Pipeline"
```

#### Studies Pipeline (Preetude Records)
**HubSpot Pipeline**: "Studies Pipeline"

| IC'ALPS Stage | HubSpot Stage | Status Combinations |
|---------------|---------------|-------------------|
| `Identification` | `01-Identification` | NoGo, In Progress, Won, Lost, Abandonne |
| `Qualified` | `02-Qualifiée` | In Progress, Won, Lost |
| `Evaluation technique` | `03-Evaluation technique` | In Progress, Won, Lost |
| `Construction propositions` | `04-Construction propositions` | In Progress, Won, Lost |
| `Negotiating` | `05-Négociation` | In Progress, Won, Lost, Sleap, Abandonne |
| `NULL` + Status="Won" | `closedwon` | Final stage for won deals |
| Any + Status="Lost" | `closedlost` | Final stage for lost deals |

#### Sales Pipeline (Opportunity Records)
**HubSpot Pipeline**: "Sales Pipeline"

| IC'ALPS Stage | HubSpot Stage | Status Combinations |
|---------------|---------------|-------------------|
| `Identification` | `Identified` | NoGo, In Progress, Won, Lost |
| `Qualified` | `Qualified` | In Progress, Won, Lost |
| `Evaluation technique` | `Design in` | In Progress, Won, Lost |
| `Construction propositions` | `Negotiate` | In Progress, Won, Lost |
| `Negotiating` | `Design Win` | In Progress, Won, Lost, Sleap |
| `NULL` + Status="Won" | `closedwon` | Final stage for won deals |
| Any + Status="Lost" | `closedlost` | Final stage for lost deals |

### Deal Stage Transformation Rules

```python
# Double Granularity Mapping (Final Stages)
final_stage_mapping = {
    "Won": "closedwon",
    "Lost": "closedlost",
    "NoGo": "closedlost",     # Not profitable
    "Abandonne": "closedlost", # Project doesn't want ASIC
    "Sleap": "Design Win"      # Keep in active pipeline
}

# Active Stage Mapping
active_stage_mapping = {
    "Identification": {"Studies": "01-Identification", "Sales": "Identified"},
    "Qualified": {"Studies": "02-Qualifiée", "Sales": "Qualified"},
    "Evaluation technique": {"Studies": "03-Evaluation technique", "Sales": "Design in"},
    "Construction propositions": {"Studies": "04-Construction propositions", "Sales": "Negotiate"},
    "Negotiating": {"Studies": "05-Négociation", "Sales": "Design Win"}
}
```

## 📞 Communications Mapping

### HubSpot Object: **Communications/Activities**
**Source File**: `Legacy_comm.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `Comm_CommunicationId` | Custom: `icalps_comm_id` | Legacy communication ID | Direct mapping for reference |
| `Comm_Subject` | `hs_activity_subject` | Communication subject | Direct mapping |
| `Comm_From` | `hs_activity_source` | From field | Map if available |
| `Comm_TO` | `hs_activity_outcome` | To field | Map if available |
| `Comm_DateTime` | `hs_timestamp` | Communication date | Convert date format |
| `Oppo_OpportunityId` | Deal Association | Associated deal | Link to deal via opportunity ID |
| `Pers_PersonId` | Contact Association | Associated contact | Link to contact via person ID |
| `Comp_CompanyId` | Company Association | Associated company | Link to company via company ID |

### Communication Type Mapping
```python
# Default communication type based on subject patterns
communication_type_mapping = {
    "Suivi": "NOTE",  # Follow-up notes
    "Call": "CALL",   # Phone calls
    "Email": "EMAIL", # Email communications
    "Meeting": "MEETING" # In-person meetings
}
```

## 🔗 Social Networks Mapping

### HubSpot Object: **Social Media URLs (Company/Contact Properties)**
**Source File**: `legacy_socialnetworks.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `sone_networklink` | Various social properties | Social network URL | **See URL Mapping below** |
| `Related_TableID` | Entity Type Identifier | Table reference | 5=Company, 13=Person |
| `Related_RecordID` | Entity ID | Record reference | Link to company/contact |
| `bord_caption` | Entity Type | Entity description | Company/Person identifier |

### Social URL Mapping Rules

```python
# LinkedIn URL mapping
if "in/" in sone_networklink:
    if Related_TableID == 13:  # Person
        property_name = "linkedin_bio"
    elif Related_TableID == 5:  # Company
        property_name = "linkedin_company_page"

# Company website mapping
elif "company/" in sone_networklink:
    property_name = "website"  # Secondary website

# Auto-generated links
elif sone_networklink == "#AUTO#":
    # Skip auto-generated placeholder links
    skip_record = True
```

## 📈 Computed Columns & Business Logic

### Required Calculated Fields

| Field Name | Calculation | HubSpot Property | Description |
|------------|-------------|------------------|-------------|
| **Weighted Forecast** | `Oppo_Forecast × Oppo_Certainty / 100` | Custom: `weighted_forecast` | Probability-adjusted forecast |
| **Net Amount** | `Oppo_Forecast - oppo_cout` | Custom: `net_amount` | Profit calculation |
| **Net Weighted Amount** | `(Oppo_Forecast - oppo_cout) × Oppo_Certainty / 100` | Custom: `net_weighted_amount` | Probability-adjusted profit |
| **Deal Age** | `days_between(today, Oppo_CreatedDate)` | Custom: `deal_age_days` | Deal lifecycle tracking |
| **Stage Duration** | `days_between(today, Oppo_UpdatedDate)` | Custom: `stage_duration_days` | Time in current stage |

### Risk Assessment Mapping

```python
# Based on Deal Certainty percentage
risk_assessment = {
    "High Risk": "Oppo_Certainty < 30",
    "Medium Risk": "30 <= Oppo_Certainty <= 70",
    "Low Risk": "Oppo_Certainty > 70"
}
```

## 🔄 Data Transformation Workflow

### 1. Data Validation Rules
- **Email Validation**: Validate email format for contacts
- **Date Formatting**: Convert date fields to ISO format
- **Currency Conversion**: Ensure numeric fields are properly formatted
- **Required Fields**: Ensure dealname, company association, and pipeline are set

### 2. Deduplication Strategy
- **Companies**: Deduplicate by `Comp_CompanyId`
- **Contacts**: Deduplicate by `Pers_PersonId`
- **Deals**: Deduplicate by `Oppo_OpportunityId`
- **Communications**: Deduplicate by `Comm_CommunicationId`

### 3. Association Rules
- **Company → Contacts**: One-to-many via `Comp_CompanyId`
- **Company → Deals**: One-to-many via `Oppo_PrimaryCompanyId`
- **Contact → Deals**: Many-to-many via deal associations
- **Communications**: Many-to-many with all entities

### 4. Data Quality Checks
- Verify all company associations exist
- Validate contact email addresses
- Check deal amount and certainty ranges
- Ensure pipeline/stage combinations are valid

## 🚀 Implementation Notes

### ETL Processing Order
1. **Companies** first (required for associations)
2. **Contacts** second (with company associations)
3. **Deals** third (with company and contact associations)
4. **Communications** last (with all entity associations)
5. **Social Networks** (update existing records)

### Error Handling
- Log records that fail validation
- Create error reports for manual review
- Implement retry logic for API failures
- Maintain data lineage for troubleshooting

### Performance Considerations
- Process in batches of 100 records
- Use bulk import APIs where available
- Implement progress tracking and resumption
- Monitor API rate limits and throttling

This mapping reference ensures consistent and accurate transformation of IC'ALPS legacy data to HubSpot objects while preserving data integrity and business logic.