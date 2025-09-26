
  

# HubSpot Import Instructions - Enhanced Unified Deals Approach

  
(ignore this in the coding process)
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

  

2.  custom properties:

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

  

## Enhanced Analytics Opportunities Features that should be possible

  

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

  
  

### Pipeline Distribution:

- Sales Pipeline: 355 records

- Studies Pipeline: 59 records


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