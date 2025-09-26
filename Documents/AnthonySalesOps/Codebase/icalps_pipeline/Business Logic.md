
Guide me towards the visual interpretention pertaining to the implementation of the following modifications 

The set of business rules described here must be respected in the end implementation
**New business logic - Double granularity **
Final opportunity stage (New property for double granulariy)
No-go : pas rentable
Abandonnée :  Project don't want to go asic
Perdue :  Ex : Competition / price
Gagnée
The double granularity is useful for quality reason and post mortem analysis
a. **CRM updates and workflow**
Step 1 - Deal value mapping
From
Pipeline IC'ALPS Hardware (to be mapped through workflow for hubspot logic) and to preserve IC'ALPS understanding
**Hubspot**
| On-Hold    |
| ---------- |
| Identified |
| Qualified  |
| Design In  |
|            |
| Design Win |
| Closed Won |
Pipeline IC'ALPS Software (to be mapped through workflow for hubspot logic)
**Hubspot**
|---|
|On-Hold|
|Identified|
|Closed Won|
|Closed Lost|
|Identification|
Merge into
Pipeline IC'ALPS Hardware nomenclature
|Deal Stage|
|01 - Identification|
|02 - Qualifiée|
|03 - Evaluation technique|
|04 - Construction propositions|
|05 -Négociations|
Pipeline IC'ALPS Software nomenclature
|Deal Stage|
|01 - Identification|
|02 - Qualifiée|
|03 - Evaluation technique|
|04 - Construction propositions|
|05 -Négociations|
WE therefore have 20 different stages to map
Pipeline IC'ALPS Software
|01 - Identification|
    No-go : pas rentable
    Abandonnée :  Project don't want to go asic
    Perdue :  Ex : Competition / price
    Gagnée
...
continue to 05 
Pipeline icAlps Hardware
|01 - Identification|
    No-go : pas rentable
    Abandonnée :  Project don't want to go asic
     En cours
    Perdue :  Ex : Competition / price
    Gagnée
... continue to 05 - Négociations
Rules and logic examples
If Pipeline icAlps Hardware && |01 - Identification| && Perdue  >> Deal stage = Closed lost
If Pipeline icAlps Hardware && |01 -  Evaluation technique | &&  En cours  >> Deal stage = Closed lost 
We use position to map based on the pipeline stage through workflow
3 - Computed columns
Map Ic_alps Forecast into Amount
Set Weighted Forecast as Amount * ICAlps Certainty
Set Net amount (forecast - cost)
Net Weighted  Amount
Things remaining to do 
**licence**
UI and changes
interface utilisateur sur les stades
utiliser claude code pouir mapper les stades
    Champs Calculé pour icalps
    oppo cout pondere la valeur du forecast 
calculated column :
**Business continuity**
if date modified is different created monitor deal changes in ic alps
* Created
* Modified
Property To add
linkedin link for opportunities / contact and companies
To do send email for crm dynamic sheet integration