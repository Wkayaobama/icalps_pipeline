# Desired HubSpot Object Mapping Reference

**📅 Created: September 2025 - Based on Actual HubSpot Implementation Analysis**

This document provides the corrected mapping reference for IC'ALPS legacy data to HubSpot objects, based on the actual properties and pipelines currently implemented in the HubSpot instance.

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
| `Comp_CompanyId` | Custom: `icalps_company_id` ✅ | Legacy company ID | Direct mapping for reference |
| `Comp_Name` | `name` ✅ | Company name | Direct mapping, clean trailing spaces |
| `Comp_Website` | `website` ✅ | Company website | Clean URL format, add protocol if missing |
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
| `Pers_PersonId` | Custom: `icalps_contact_id` ✅ | Legacy person ID | Direct mapping for reference |
| `Pers_FirstName` | `firstname` ✅ | Contact first name | Direct mapping, capitalize first letter |
| `Pers_LastName` | `lastname` ✅ | Contact last name | Direct mapping, capitalize first letter |
| `Pers_EmailAddress` | `email` ✅ | Primary email | Direct mapping, validate email format |
| `Comp_CompanyId` | Company Association | Associated company | Link to company via company ID |
| `Comp_Name` | Company Association | Company name | For validation/reference |
| `Oppo_OpportunityId` | Deal Association | Associated deals | Link to deals via opportunity ID |
| `Oppo_Description` | Deal Association | Deal context | Store in association metadata |

### Additional Properties Available
| IC'ALPS Field | HubSpot Property | Description | Status |
|---------------|------------------|-------------|---------|
| Temperature | Custom: `icalps_temperature` ✅ | Contact temperature/priority | Enumeration field |

### Data Quality Notes
- Contacts have many-to-many relationships with opportunities
- Email validation required
- Company associations should be validated against company data

## 💼 Deals/Opportunities Mapping

### HubSpot Object: **Deals**
**Source File**: `Legacy_Opportunities.csv`

| IC'ALPS Field | HubSpot Property | Description | Transformation Rules |
|---------------|------------------|-------------|---------------------|
| `Oppo_OpportunityId` | Custom: `icalps_deal_id` ✅ | Legacy opportunity ID | Direct mapping for reference |
| `Oppo_Description` | `dealname` ✅ | Deal name/description | Direct mapping, truncate if > 255 chars |
| `Oppo_PrimaryCompanyId` | Company Association | Primary company | Link to company via company ID |
| `Oppo_PrimaryPersonId` | Contact Association | Primary contact | Link to contact via person ID |
| `Oppo_AssignedUserId` | `hubspot_owner_id` ✅ | Deal owner | Map to HubSpot user IDs |
| `Oppo_Type` | Custom: `icalps_dealtype` ✅ | Deal type (FTK, Preetude) | Map to pipeline selection |
| `Oppo_Product` | Custom: `prod___product_name_new_` ✅ | Product information | Direct mapping using enumeration |
| `Oppo_Source` | Custom: `icalps_dealsource` ✅ | Lead source | Map to HubSpot lead sources |
| `Oppo_Note` | Custom: `icalps_dealnotes` ✅ | Deal description | Clean and format |
| `Oppo_CustomerRef` | Custom: `customer_reference` ❌ | Customer reference | **Property needs creation** |
| `Oppo_Status` | Custom: `icalps_dealstatus` ✅ | Current deal status | **See Pipeline Mapping below** |
| `Oppo_Stage` | `dealstage` ✅ | Current deal stage | **See Pipeline Mapping below** |
| `Oppo_Forecast` | Custom: `icalps_dealforecast` ✅ | Deal forecast amount | Convert to number, handle currency |
| `Oppo_Certainty` | Custom: `icalps_dealcertainty` ✅ | Confidence percentage | Direct mapping (0-100) |
| `Oppo_Priority` | Custom: `priority` ❌ | Deal priority | **Property needs creation** |
| `Oppo_TargetClose` | `closedate` ✅ | Target close date | Convert date format |
| `Oppo_CreatedDate` | Custom: `icalps_deal_created_date` ✅ | Created date | Convert date format |
| `Oppo_UpdatedDate` | `hs_lastmodifieddate` ✅ | Last modified | Convert date format |
| `Oppo_Total` | Custom: `total_amount` ❌ | Total deal value | **Property needs creation** |
| `oppo_cout` | Custom: `ic_alps_cost` ✅ | Deal cost | Convert to number |

### 📊 Additional Deal Properties Available

| Property Name | HubSpot Property | Type | Description |
|---------------|------------------|------|-------------|
| Net Amount | Custom: `icalps_netamount` ✅ | String | Net profit calculation |
| Net Weighted Amount | Custom: `icalps_net_weighted_amount` ✅ | Number | Probability-adjusted profit |
| Weighted Amount | Custom: `weighted_amount` ✅ | Number | Probability-adjusted forecast |
| Deal Category | Custom: `icalps_dealcategory` ✅ | String | Deal categorization |
| Pipeline Stage Key | Custom: `icalps_pipeline_stage_key` ✅ | String | Stage reference |
| Original Status | Custom: `icalps_original_status` ✅ | Enumeration | Original IC'ALPS status |
| Original Stage | Custom: `icalps_originalstage` ✅ | Enumeration | Original IC'ALPS stage |
| Transformation Notes | Custom: `icalps_transformation_notes` ✅ | String | Migration notes |

## 🔄 Pipeline and Stage Mapping

### Current HubSpot Pipeline Implementation

#### Pipeline Selection Logic
```python
# Based on icalps_dealtype field - actual implemented pipelines
pipelines = {
    "SEALCOIN": "SEALCOIN",
    "SealSQ_Hardware": "SealSQ Hardware", 
    "SealSQ_Services": "SealSQ Services",
    "Wisekey": "Wisekey",
    "Icalps_Hardware": "Icalps_hardware",
    "Icalps_Services": "Icalps_service"
}
```

#### SEALCOIN Pipeline ✅
**HubSpot Pipeline**: "SEALCOIN"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 1031449001 |
| `Qualified` | `Qualified` | 1031449002 |
| `Evaluation technique` | `Design In` | 1031449003 |
| `Negotiating` | `Design Win` | 1031449004 |
| Won | `Closed Won` | 1031449005 |
| Lost | `Closed Lost` | 1031449006 |

#### SealSQ Hardware Pipeline ✅
**HubSpot Pipeline**: "SealSQ Hardware"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 12096409 |
| `Qualified` | `Qualified` | 12096410 |
| `Evaluation technique` | `Design In` | 12096411 |
| `Negotiating` | `Design Win` | 12096412 |
| Won | `Closed Won` | 12096869 |
| Lost | `Closed Lost` | 12096415 |
| Dead | `Closed Dead` | 13772274 |

#### SealSQ Services Pipeline ✅
**HubSpot Pipeline**: "SealSQ Services"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 13772280 |
| `Qualified` | `Qualified` | 13772281 |
| `Evaluation technique` | `Design In` | 13772282 |
| `Negotiating` | `Design Win` | 31868514 |
| Won | `Closed Won` | 13772285 |
| Lost | `Closed Lost` | 13772286 |
| Dead | `Closed Dead` | 13772309 |

#### Wisekey Pipeline ✅
**HubSpot Pipeline**: "Wisekey"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 85103752 |
| `Qualified` | `Qualified` | 85103753 |
| `Evaluation technique` | `Design In` | 85103754 |
| `Negotiating` | `Design Win` | 85103755 |
| Won | `Closed Won` | 85103756 |
| Dead | `Closed Dead` | 85103757 |
| Lost | `Closed Lost` | 85103758 |

#### Icalps Hardware Pipeline ✅
**HubSpot Pipeline**: "Icalps_hardware"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 1116419644 |
| `Qualified` | `Qualified` | 1116419645 |
| `Evaluation technique` | `Design In` | 1116419646 |
| `Negotiating` | `Design Win` | 1116419647 |
| Won | `Closed Won` | 1116419649 |
| Dead | `Closed Dead` | 1116419650 |
| On-Hold | `On-Hold` | 1116652341 |

#### Icalps Service Pipeline ✅
**HubSpot Pipeline**: "Icalps_service"

| IC'ALPS Stage | HubSpot Stage | Stage ID |
|---------------|---------------|----------|
| `Identification` | `Identified` | 1116269224 |
| `Qualified` | `Qualified` | 1162868542 |
| `Construction propositions` | `Negotiate` | 1116704051 |
| Won | `Closed Won` | 1116704052 |
| Lost | `Closed Lost` | 1116704053 |
| On-Hold | `On-Hold` | 1116269223 |

### Deal Stage Transformation Rules

```python
# Status to Stage Mapping
def map_deal_stage(status, stage, deal_type):
    """
    Map IC'ALPS status and stage to HubSpot pipeline stage
    """
    # Final status mappings
    if status == "Won":
        return "Closed Won"
    elif status in ["Lost", "NoGo", "Abandonne"]:
        return "Closed Lost"
    elif status == "Dead":
        return "Closed Dead"
    
    # Active stage mapping by deal type
    stage_mapping = {
        "Identification": "Identified",
        "Qualified": "Qualified", 
        "Evaluation technique": "Design In",
        "Construction propositions": "Negotiate",  # Services only
        "Negotiating": "Design Win"
    }
    
    return stage_mapping.get(stage, "Identified")
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
    skip_record = True
```

## 📈 Computed Columns & Business Logic

### Required Calculated Fields

| Field Name | Calculation | HubSpot Property | Status |
|------------|-------------|------------------|---------|
| **Weighted Forecast** | `icalps_dealforecast × icalps_dealcertainty / 100` | Custom: `weighted_amount` ✅ | Available |
| **Net Amount** | `icalps_dealforecast - ic_alps_cost` | Custom: `icalps_netamount` ✅ | Available |
| **Net Weighted Amount** | `(icalps_dealforecast - ic_alps_cost) × icalps_dealcertainty / 100` | Custom: `icalps_net_weighted_amount` ✅ | Available |
| **Deal Age** | `days_between(today, icalps_deal_created_date)` | Custom: `deal_age_days` ❌ | Needs creation |
| **Stage Duration** | `days_between(today, hs_lastmodifieddate)` | Custom: `stage_duration_days` ❌ | Needs creation |

## ❌ Missing Properties Requiring Creation

### Contacts
- No missing critical properties identified

### Companies  
- No missing critical properties identified

### Deals
The following properties need to be created in HubSpot:

| Property Name | Type | Description | Purpose |
|---------------|------|-------------|---------|
| `customer_reference` | String | Customer reference number | Track customer references |
| `priority` | Enumeration | Deal priority (High/Medium/Low) | Priority management |
| `total_amount` | Number | Total deal value | Financial tracking |
| `deal_age_days` | Number | Days since deal creation | Performance metrics |
| `stage_duration_days` | Number | Days in current stage | Process tracking |

## 🔄 Data Transformation Workflow

### 1. Data Validation Rules
- **Email Validation**: Validate email format for contacts
- **Date Formatting**: Convert date fields to ISO format  
- **Currency Conversion**: Ensure numeric fields are properly formatted
- **Required Fields**: Ensure dealname, company association, and pipeline are set

### 2. Deduplication Strategy
- **Companies**: Deduplicate by `Comp_CompanyId` → `icalps_company_id`
- **Contacts**: Deduplicate by `Pers_PersonId` → `icalps_contact_id` 
- **Deals**: Deduplicate by `Oppo_OpportunityId` → `icalps_deal_id`
- **Communications**: Deduplicate by `Comm_CommunicationId`

### 3. Association Rules
- **Company → Contacts**: One-to-many via `icalps_company_id`
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

### Pipeline Assignment Logic
```python
def assign_pipeline(deal_type, product_category):
    """Assign correct pipeline based on deal characteristics"""
    pipeline_mapping = {
        "SEALCOIN": "SEALCOIN",
        "SealSQ_Hardware": "SealSQ Hardware",
        "SealSQ_Services": "SealSQ Services", 
        "Wisekey": "Wisekey",
        "Icalps_Hardware": "Icalps_hardware",
        "Icalps_Services": "Icalps_service"
    }
    return pipeline_mapping.get(deal_type, "Icalps_hardware")  # Default fallback
```

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

## 📝 Status Legend

- ✅ **Property exists and verified** - Ready for mapping
- ❌ **Property missing** - Requires creation before mapping
- ⚠️ **Property exists but name differs** - Updated mapping provided

---

**Last Updated**: September 2025  
**Verification Method**: HubSpot MCP Server API Analysis  
**Total Properties Verified**: 300+ across Companies, Contacts, and Deals objects  
**Total Pipelines Verified**: 6 active pipelines with complete stage mapping
